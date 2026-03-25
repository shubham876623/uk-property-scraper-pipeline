import os
import asyncio
from datetime import datetime
from db.db import get_connection, run_query, run_insert
from src.parser import fetch_properties_for_postcode, fetch_properties_for_postcode_async
from src.validation import validate_scraped_outcode
from src.status_tracker import save_completed_postcode
# Add once at the top of main.py
import sys
sys.stdout.reconfigure(encoding='utf-8')


def cleanup_removed_properties(postcode, seen_ref_ids, total_listing_count=0):
    """
    Mark properties as 'Removed' if they exist in the DB for this postcode
    but were NOT found in the current Rightmove listings.
    """
    if not seen_ref_ids:
        print(f"⚠️ No properties found during scrape for {postcode} — skipping cleanup to avoid false removals")
        return 0

    # Safety check: if we found significantly fewer properties than listing pages showed,
    # some property pages likely failed to load — skip cleanup to avoid false removals
    if total_listing_count > 0 and len(seen_ref_ids) < total_listing_count * 0.95:
        print(f"⚠️ Only parsed {len(seen_ref_ids)}/{total_listing_count} properties for {postcode} — skipping cleanup (too many failures)")
        return 0

    conn = get_connection()

    # Get all properties in DB for this outcode
    db_properties = run_query(
        conn,
        "SELECT PropertyReferanceNumber, PropertyType FROM ExtractedProperties WHERE PropertyOutcode = ?",
        [postcode],
    )

    if not db_properties:
        print(f"📭 No existing properties in DB for {postcode} — nothing to clean up")
        return 0

    removed_count = 0
    today = datetime.utcnow().strftime("%Y-%m-%d")

    for row in db_properties:
        ref = row.get("PropertyReferanceNumber") if isinstance(row, dict) else row[0]
        status = row.get("PropertyType") if isinstance(row, dict) else row[1]

        # Skip properties already marked as Removed or Sold
        status_lower = str(status or "").lower().strip()
        if status_lower in ("removed", "sold", "sold stc"):
            continue

        # If this property was NOT seen in current Rightmove listings, mark as Removed
        if ref and str(ref) not in seen_ref_ids:
            print(f"🗑️ Property {ref} not found on Rightmove — marking as Removed (was: {status})")
            run_insert(
                conn,
                "UPDATE ExtractedProperties SET PropertyType = ? WHERE PropertyReferanceNumber = ?",
                ["Removed", ref],
            )
            # Add marketing history entry
            run_insert(
                conn,
                """INSERT INTO ExtractedPropertyMarketingHistory
                   (PropertyReferenceNumber, PropertyStatus, DateStatusChanged)
                   VALUES (?, ?, ?)""",
                [ref, "Removed", today],
            )
            removed_count += 1

    print(f"🧹 Cleanup for {postcode}: {removed_count} properties marked as Removed")
    return removed_count

# Use async scraper by default for speed; set RIGHTMOVE_USE_ASYNC=0 to use sync
USE_ASYNC = os.environ.get("RIGHTMOVE_USE_ASYNC", "1").strip().lower() in ("1", "true", "yes")
RIGHTMOVE_CONCURRENCY = int(os.environ.get("RIGHTMOVE_SCRAPER_CONCURRENCY", "24"))
RIGHTMOVE_PAGE_CONCURRENCY = int(os.environ.get("RIGHTMOVE_PAGE_CONCURRENCY", "8"))


def process_postcode(postcode, outcode):
    """
    Process a postcode scrape and validate the results.
    
    Args:
        postcode: The expected outcode to scrape (e.g., 'DY3')
        outcode: The Rightmove identifier from CSV (e.g., '5E37')
    
    Returns:
        dict: Validation results
    """
    print(f"🔍 Starting scrape for postcode: {postcode} (identifier: {outcode})")
    if USE_ASYNC:
        print(f"⚡ Using async scraper (concurrency={RIGHTMOVE_CONCURRENCY}, page_concurrency={RIGHTMOVE_PAGE_CONCURRENCY})")
    
    records_scraped = 0
    seen_ref_ids = set()
    total_listing_count = 0
    try:
        # Perform the scrape (returns count of inserted records, seen property IDs, and total listing count)
        if USE_ASYNC:
            records_scraped, seen_ref_ids, total_listing_count = asyncio.run(
                fetch_properties_for_postcode_async(
                    postcode, outcode,
                    concurrency=RIGHTMOVE_CONCURRENCY,
                    page_concurrency=RIGHTMOVE_PAGE_CONCURRENCY,
                )
            )
        else:
            records_scraped, seen_ref_ids, total_listing_count = fetch_properties_for_postcode(postcode, outcode)

        print(f"✅ Scrape completed for {postcode} - {records_scraped} records inserted")
        print(f"📊 Found {len(seen_ref_ids)} properties in current listings (out of {total_listing_count} from listing pages)")

        # Clean up properties no longer listed on Rightmove
        removed_count = cleanup_removed_properties(postcode, seen_ref_ids, total_listing_count)
        print(f"🧹 Removed {removed_count} stale properties for {postcode}")

        # Save completed postcode to status table
        save_completed_postcode(postcode, records_scraped)
        
    except Exception as e:
        print(f"❌ Error during scraping for {postcode}: {e}")
        import traceback
        traceback.print_exc()
        # Don't save to status table if scraping failed
        # Return early to avoid validation on failed scrape
        return {
            'success': False,
            'message': f'Scraping failed: {str(e)}',
            'properties_found': 0,
            'matching_properties': 0,
            'mismatched_properties': 0,
            'mismatches': []
        }
    
    # Simple validation: Check if properties with expected outcode exist
    try:
        validation_result = validate_scraped_outcode(
            expected_postcode=postcode,
            expected_identifier=outcode
        )
        
        # Print validation result
        print(f"\n{'='*80}")
        print(f"VALIDATION RESULT for {postcode}")
        print(f"{'='*80}")
        print(validation_result['message'])
        print(f"Expected Outcode: {postcode}")
        print(f"Rightmove Identifier: {outcode}")
        print(f"Properties Found: {validation_result['properties_found']}")
        print(f"Matching Properties: {validation_result['matching_properties']}")
        print(f"Mismatched Properties: {validation_result['mismatched_properties']}")
        
        if validation_result.get('mismatches'):
            print(f"\n⚠️ Sample Mismatches (first 5):")
            for mismatch in validation_result['mismatches'][:5]:
                print(f"  - Property {mismatch['property_ref']}: Expected {mismatch['expected_outcode']}, Got {mismatch['actual_outcode']}")
        
        print(f"{'='*80}\n")
        
        return validation_result
    except Exception as e:
        print(f"⚠️ Error during validation for {postcode}: {e}")
        import traceback
        traceback.print_exc()
        # Return a default validation result so scraper can continue
        return {
            'success': False,
            'message': f'Validation failed: {str(e)}',
            'properties_found': 0,
            'matching_properties': 0,
            'mismatched_properties': 0,
            'mismatches': []
        }
        