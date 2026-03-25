import sys
import os
from datetime import datetime

# Add parent directory to path for imports
# This allows the script to be run directly
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)  # epc_deep_scraper directory
project_root = os.path.dirname(parent_dir)  # Endpoints directory

# Add project root to path
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Configure stdout encoding for Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

# Safe print function to handle Unicode encoding errors
def safe_print(msg: str):
    """Print message safely, handling Unicode encoding errors on Windows."""
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        # Remove or replace problematic Unicode characters
        safe_msg = msg.encode("ascii", errors="ignore").decode("ascii")
        print(safe_msg, flush=True)


# ---------------------------------------------------------------
# Outcode parsing helper (supports single or multiple outcodes)
# ---------------------------------------------------------------
def _parse_outcode_list(outcode):
    """
    Parse an outcode filter string into a list.

    Supports:
    - Single outcode: "SG1"
    - Comma-separated: "LE8,LE9, LE10"
    - With quotes: "'LE8', 'LE9'"

    Returns:
    - None if no filter
    - List[str] of uppercased outcodes otherwise
    """
    if not outcode:
        return None

    # Remove quotes and normalise
    cleaned = outcode.replace("'", "").replace('"', "")
    # Split on commas
    parts = [p.strip().upper() for p in cleaned.split(",") if p.strip()]

    if not parts:
        return None

    return parts

# Now use absolute imports
from epc_deep_scraper.database.db import get_properties_with_epc
from epc_deep_scraper.src.scraper import EPCScraper
from epc_deep_scraper.src.scraper_async import AsyncEPCScraper
import asyncio
import aiohttp
import ssl
import certifi

def run_deep_scraper(outcode=None):
    # Clear log file at start of new session
    # Calculate path: epc_deep_scraper/src/main.py -> Endpoints/logs/deep_scraper.log
    current_file_dir = os.path.dirname(os.path.abspath(__file__))  # epc_deep_scraper/src
    parent_dir = os.path.dirname(current_file_dir)  # epc_deep_scraper
    project_root = os.path.dirname(parent_dir)  # Endpoints
    log_file = os.path.join(project_root, "logs", "deep_scraper.log")
    try:
        if os.path.exists(log_file):
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write('')  # Clear the log file
            safe_print(f"[INFO] Cleared log file for new session: {log_file}")
    except Exception as e:
        safe_print(f"[WARNING] Could not clear log file: {e}")
    
    start_time = datetime.now()
    safe_print(f"[START] Starting EPC Deep Scraper at {start_time.strftime('%Y-%m-%d %H:%M:%S')}...")

    outcode_list = _parse_outcode_list(outcode)
    if outcode_list:
        if len(outcode_list) == 1:
            safe_print(f"[INFO] Filtering by outcode: {outcode_list[0]}")
        else:
            safe_print(f"[INFO] Filtering by outcodes: {', '.join(outcode_list)}")
    else:
        safe_print(f"[INFO] No outcode filter provided - scraping all eligible properties.")
    
    safe_print(f"[INFO] Fetching properties from database...")

    # Fetch properties for one or more outcodes
    if outcode_list:
        properties = []
        for oc in outcode_list:
            safe_print(f"[INFO] Fetching properties for outcode '{oc}'...")
            props = get_properties_with_epc(outcode=oc)
            if props:
                properties.extend(props)
    else:
        properties = get_properties_with_epc(outcode=None)
    if not properties:
        if outcode:
            safe_print(f"[SUCCESS] No records to scrape for outcode '{outcode}'. All properties have been deep-scraped.")
        else:
            safe_print("[SUCCESS] No records to scrape. All properties have been deep-scraped.")
        return {"status": "no_records", "count": 0}

    total_properties = len(properties)
    if outcode_list:
        if len(outcode_list) == 1:
            safe_print(f"[INFO] Found {total_properties} properties needing deep scraping for outcode '{outcode_list[0]}'.")
        else:
            safe_print(f"[INFO] Found {total_properties} properties needing deep scraping across outcodes: {', '.join(outcode_list)}.")
    else:
        safe_print(f"[INFO] Found {total_properties} properties needing deep scraping.")
    safe_print(f"[INFO] Starting processing...\n")

    count = 0
    failed_count = 0
    total_epc_inserted = 0
    
    safe_print(f"\n{'='*60}")
    safe_print(f"[LIVE STATS] Starting deep scraping...")
    safe_print(f"[LIVE STATS] Total Properties: {total_properties}")
    safe_print(f"[LIVE STATS] EPC Records Inserted: {total_epc_inserted}")
    safe_print(f"{'='*60}\n")
    
    for idx, row in enumerate(properties, 1):
        property_id = row.get("PropertyId")
        postcode = row.get("PropertyPostCode")
        property_address = row.get("PropertyAddress")
        property_outcode = row.get("PropertyOutcode")  # Get PropertyOutcode from database
        
        # Live progress update - show current postcode being scraped
        safe_print(f"\n{'='*60}")
        safe_print(f"[LIVE PROGRESS] [{idx}/{total_properties}] Processing Postcode: {postcode}")
        safe_print(f"[LIVE PROGRESS] PropertyId: {property_id}")
        safe_print(f"[LIVE STATS] EPC Records Inserted So Far: {total_epc_inserted}")
        safe_print(f"[LIVE STATS] Properties Completed: {count}/{total_properties}")
        safe_print(f"{'='*60}")
        
        # Inform if PropertyId is missing (will use postcode as fallback)
        if not property_id:
            safe_print(f"[INFO] Property at index {idx} has no PropertyId - will use postcode={postcode} to mark as scraped.")
            safe_print(f"[INFO] Address: {property_address if property_address else 'N/A'}")
        
        # Additional validation: Check for data inconsistencies and warn
        # Only perform strict outcode consistency checks when a single outcode filter is used
        single_filter_outcode = outcode_list[0] if 'outcode_list' in locals() and outcode_list and len(outcode_list) == 1 else None
        if single_filter_outcode:
            # Since we already filtered by PropertyOutcode in the query, trust that filter
            # But check if PropertyPostCode matches and warn about mismatches
            if property_outcode:
                # Check if PropertyOutcode matches the filter (should always match since we filtered)
                if property_outcode.upper() != single_filter_outcode.upper():
                    safe_print(f"[WARNING] PropertyId={property_id}: PropertyOutcode={property_outcode} doesn't match filter '{single_filter_outcode}' (query filtered incorrectly)")
                    # Continue anyway since query already filtered
                else:
                    # Check if Postcode outcode matches PropertyOutcode (data consistency check)
                    postcode_outcode = postcode.split()[0] if postcode and ' ' in postcode else postcode
                    if postcode_outcode and postcode_outcode.upper() != property_outcode.upper():
                        safe_print(f"[WARNING] PropertyId={property_id}: Data inconsistency - PropertyOutcode={property_outcode} but Postcode={postcode} (outcode '{postcode_outcode}')")
                        safe_print(f"[INFO] Processing anyway based on PropertyOutcode filter '{single_filter_outcode}'...")
        
        safe_print(f"[{idx}/{total_properties}] Processing PropertyId={property_id}, Postcode={postcode}")
        
        try:
            scraper = EPCScraper()
            # Track EPC records inserted for this property
            epc_inserted_this_property = scraper.scrape_postcode(postcode, property_id, property_address)
            total_epc_inserted += epc_inserted_this_property
            
            count += 1
            safe_print(f"[SUCCESS] Completed PropertyId={property_id} ({count}/{total_properties} successful)")
            safe_print(f"[LIVE STATS] EPC Records Inserted: {total_epc_inserted} | Properties Completed: {count}/{total_properties} | Current Postcode: {postcode}\n")
        except Exception as e:
            failed_count += 1
            safe_print(f"[ERROR] Failed PropertyId={property_id}: {e}")
            safe_print(f"[LIVE STATS] EPC Records Inserted: {total_epc_inserted} | Properties Completed: {count}/{total_properties} | Failed: {failed_count} | Current Postcode: {postcode}\n")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    safe_print(f"\n{'='*60}")
    safe_print(f"[COMPLETE] Completed EPC Deep Scraper")
    safe_print(f"[STATS] Total Properties: {total_properties}")
    safe_print(f"[STATS] Successful: {count}")
    safe_print(f"[STATS] Failed: {failed_count}")
    safe_print(f"[STATS] EPC Records Inserted: {total_epc_inserted}")
    safe_print(f"[STATS] Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
    safe_print(f"[STATS] Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    safe_print(f"{'='*60}")
    
    return {"status": "success", "records_scraped": count, "records_failed": failed_count, "total": total_properties, "epc_inserted": total_epc_inserted}

async def run_deep_scraper_async(outcode=None, concurrency=10):
    """
    Async version of run_deep_scraper with concurrent processing.
    
    Args:
        outcode: Optional postcode outcode filter
        concurrency: Number of concurrent property scrapes (default: 10)
    """
    # Clear log file at start of new session
    current_file_dir = os.path.dirname(os.path.abspath(__file__))  # epc_deep_scraper/src
    parent_dir = os.path.dirname(current_file_dir)  # epc_deep_scraper
    project_root = os.path.dirname(parent_dir)  # Endpoints
    log_file = os.path.join(project_root, "logs", "deep_scraper.log")
    try:
        if os.path.exists(log_file):
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write('')  # Clear the log file
            safe_print(f"[INFO] Cleared log file for new session: {log_file}")
    except Exception as e:
        safe_print(f"[WARNING] Could not clear log file: {e}")
    
    start_time = datetime.now()
    safe_print(f"[START] Starting EPC Deep Scraper (ASYNC) at {start_time.strftime('%Y-%m-%d %H:%M:%S')}...")
    safe_print(f"[INFO] Concurrency: {concurrency}")

    outcode_list = _parse_outcode_list(outcode)
    if outcode_list:
        if len(outcode_list) == 1:
            safe_print(f"[INFO] Filtering by outcode: {outcode_list[0]}")
        else:
            safe_print(f"[INFO] Filtering by outcodes: {', '.join(outcode_list)}")
    else:
        safe_print(f"[INFO] No outcode filter provided - scraping all eligible properties.")
    
    safe_print(f"[INFO] Fetching properties from database...")
    
    # Fetch properties for one or more outcodes
    if outcode_list:
        properties = []
        for oc in outcode_list:
            safe_print(f"[INFO] Fetching properties for outcode '{oc}'...")
            props = get_properties_with_epc(outcode=oc)
            if props:
                properties.extend(props)
    else:
        properties = get_properties_with_epc(outcode=None)
    if not properties:
        if outcode:
            safe_print(f"[SUCCESS] No records to scrape for outcode '{outcode}'. All properties have been deep-scraped.")
        else:
            safe_print("[SUCCESS] No records to scrape. All properties have been deep-scraped.")
        return {"status": "no_records", "count": 0}
    
    total_properties = len(properties)
    if outcode_list:
        if len(outcode_list) == 1:
            safe_print(f"[INFO] Found {total_properties} properties needing deep scraping for outcode '{outcode_list[0]}'.")
        else:
            safe_print(f"[INFO] Found {total_properties} properties needing deep scraping across outcodes: {', '.join(outcode_list)}.")
    else:
        safe_print(f"[INFO] Found {total_properties} properties needing deep scraping.")
    safe_print(f"[INFO] Starting async processing...\n")
    
    # Create async scraper
    scraper = AsyncEPCScraper(concurrency=concurrency, max_retries=5)
    
    # Create aiohttp session
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(ssl=ssl_context, limit=concurrency * 2, limit_per_host=concurrency)
    
    count = 0
    failed_count = 0
    total_epc_inserted = 0
    total_epc_failed = 0
    
    async with aiohttp.ClientSession(connector=connector) as session:
        # Process properties concurrently
        tasks = []
        for idx, row in enumerate(properties, 1):
            property_id = row.get("PropertyId")
            postcode = row.get("PropertyPostCode")
            property_address = row.get("PropertyAddress")
            property_outcode = row.get("PropertyOutcode")
            
            # Create task for each property
            task = scraper.scrape_postcode_async(session, postcode, property_id, property_address)
            tasks.append((idx, property_id, postcode, task))
        
        # Process all tasks concurrently
        async def process_property(idx, property_id, postcode, property_address, task):
            """Process a single property and mark as scraped if successful."""
            from epc_deep_scraper.database.db import mark_property_scraped
            try:
                success_count, failed_count_prop, failed_urls = await task
                
                # Mark as scraped if all certificates were successfully inserted
                if success_count > 0 and failed_count_prop == 0:
                    # Mark property as scraped
                    loop = asyncio.get_event_loop()
                    if property_id:
                        result = await loop.run_in_executor(None, mark_property_scraped, property_id, None, None)
                        if result:
                            safe_print(f"[MARKED] Marked property {property_id} as scraped (by PropertyId).")
                    else:
                        result = await loop.run_in_executor(None, mark_property_scraped, None, postcode, property_address)
                        if result:
                            safe_print(f"[MARKED] Marked property with postcode={postcode} as scraped (by postcode).")
                
                safe_print(f"[{idx}/{total_properties}] Completed PropertyId={property_id}, Postcode={postcode}")
                safe_print(f"[SUCCESS] Inserted {success_count} EPC records, Failed {failed_count_prop}\n")
                return True, success_count, failed_count_prop
            except Exception as e:
                safe_print(f"[ERROR] Failed PropertyId={property_id}: {e}\n")
                return False, 0, 0
        
        # Process all properties
        processed_tasks = []
        for idx, row in enumerate(properties, 1):
            property_id = row.get("PropertyId")
            postcode = row.get("PropertyPostCode")
            property_address = row.get("PropertyAddress")
            
            # Get the task
            _, _, _, task = tasks[idx - 1]
            processed_task = process_property(idx, property_id, postcode, property_address, task)
            processed_tasks.append(processed_task)
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*processed_tasks, return_exceptions=True)
        
        # Aggregate results
        for result in results:
            if isinstance(result, Exception):
                failed_count += 1
            else:
                success, epc_success, epc_fail = result
                if success:
                    count += 1
                else:
                    failed_count += 1
                total_epc_inserted += epc_success
                total_epc_failed += epc_fail
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    safe_print(f"\n{'='*60}")
    safe_print(f"[COMPLETE] Completed EPC Deep Scraper (ASYNC)")
    safe_print(f"[STATS] Total Properties: {total_properties}")
    safe_print(f"[STATS] Successful: {count}")
    safe_print(f"[STATS] Failed: {failed_count}")
    safe_print(f"[STATS] Total EPC Records Inserted: {total_epc_inserted}")
    safe_print(f"[STATS] Total EPC Records Failed: {total_epc_failed}")
    safe_print(f"[STATS] Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
    safe_print(f"[STATS] Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    safe_print(f"{'='*60}")
    
    return {
        "status": "success",
        "records_scraped": count,
        "records_failed": failed_count,
        "total": total_properties,
        "epc_inserted": total_epc_inserted,
        "epc_failed": total_epc_failed
    }

def verify_db_inserts(outcode=None):
    """
    Verify actual EPC records inserted in database vs expected.
    This helps identify if all records were actually inserted.
    """
    from epc_deep_scraper.database.db import HEADERS
    from dotenv import load_dotenv
    import requests
    
    load_dotenv()
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    
    safe_print(f"\n{'='*60}")
    safe_print(f"[VERIFY] Verifying EPC records in database...")
    safe_print(f"{'='*60}\n")
    
    # Get properties that were supposed to be scraped
    outcode_list = _parse_outcode_list(outcode)
    if outcode_list:
        properties = []
        for oc in outcode_list:
            safe_print(f"[VERIFY] Fetching properties for outcode '{oc}'...")
            props = get_properties_with_epc(outcode=oc)
            if props:
                properties.extend(props)
    else:
        properties = get_properties_with_epc(outcode=None)
    if not properties:
        safe_print("[VERIFY] No properties found to verify.")
        return
    
    total_found = 0
    properties_with_records = 0
    properties_without_records = []
    
    for prop in properties:
        postcode = prop.get("PropertyPostCode")
        property_id = prop.get("PropertyId")
        
        if not postcode:
            continue
        
        # Count EPC records in database for this postcode
        encoded_postcode = postcode.replace(' ', '%20')
        count_url = f"{SUPABASE_URL}/rest/v1/EPCCertificateDeepScrape?postCode=eq.{encoded_postcode}&select=id"
        count_headers = {**HEADERS, "Prefer": "count=exact"}
        
        try:
            r = requests.get(count_url, headers=count_headers, timeout=30)
            if r.status_code == 200:
                content_range = r.headers.get('Content-Range', '0-0/0')
                count = int(content_range.split('/')[-1])
                total_found += count
                
                if count > 0:
                    properties_with_records += 1
                else:
                    properties_without_records.append((property_id, postcode))
                
                safe_print(f"[VERIFY] Postcode {postcode} (PropertyId={property_id}): {count} EPC records in DB")
            else:
                safe_print(f"[VERIFY ERROR] Failed to query DB for {postcode}: Status {r.status_code}")
        except Exception as e:
            safe_print(f"[VERIFY ERROR] Exception querying {postcode}: {e}")
    
    safe_print(f"\n[VERIFY SUMMARY]")
    safe_print(f"  Total properties checked: {len(properties)}")
    safe_print(f"  Properties with EPC records: {properties_with_records}")
    safe_print(f"  Properties without EPC records: {len(properties_without_records)}")
    safe_print(f"  Total EPC records in DB: {total_found}")
    if properties_without_records:
        safe_print(f"\n[VERIFY WARNING] Properties with no EPC records:")
        for prop_id, postcode in properties_without_records[:10]:  # Show first 10
            safe_print(f"    - PropertyId={prop_id}, Postcode={postcode}")
        if len(properties_without_records) > 10:
            safe_print(f"    ... and {len(properties_without_records) - 10} more")
    safe_print(f"{'='*60}\n")

# Main entry point for direct execution
if __name__ == "__main__":
    import sys
    # Accept outcode as command-line argument
    outcode = sys.argv[1] if len(sys.argv) > 1 else None
    # Use async version by default (faster)
    use_async = os.getenv("USE_ASYNC", "true").lower() == "true"
    concurrency = int(os.getenv("DEEP_SCRAPER_CONCURRENCY", "10"))
    
    if use_async:
        asyncio.run(run_deep_scraper_async(outcode=outcode, concurrency=concurrency))
        # Verify DB inserts after scraping
        verify_db_inserts(outcode=outcode)
    else:
        run_deep_scraper(outcode=outcode)
