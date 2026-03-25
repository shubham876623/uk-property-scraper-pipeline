import os
import csv
import time
import re
import asyncio
import pandas as pd
import requests
import urllib.parse
from dotenv import load_dotenv
from datetime import datetime

# === Load environment variables ===
load_dotenv()
CSV_PATH = "data.csv"
TRACKER_PATH = "epc_count_tracker.csv"
UPLOAD_SUCCESS_LOG = 'logs/upload_success_log.csv'  # Track successfully uploaded records
UPLOAD_SUCCESS_LOG = 'logs/upload_success_log.csv'  # Track successfully uploaded records

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BULK_INSERT_URL = os.getenv("BULK_INSERT_URL")
BULK_INSERT_API_KEY = os.getenv("BULK_INSERT_API_KEY")
# Force SimpleScrape table for this scraper (do not allow override)
SUPABASE_TABLE = "EPCCertificateSimpleScrape"
# Warn if environment variable is set to something else
env_table = os.getenv("SUPABASE_TABLE")
if env_table and env_table != SUPABASE_TABLE:
    print(f"[WARNING] SUPABASE_TABLE env var is set to '{env_table}' but forcing '{SUPABASE_TABLE}' for SimpleScraper")

def safe_print(msg):
    try:
        print(msg)
    except UnicodeEncodeError:
        msg = msg.encode("ascii", errors="ignore").decode()
        print(msg)

def clean_value(val):
    if pd.isna(val) or val == "" or str(val).lower() == "nan":
        return None
    return val

def safe_int_from_string(value):
    """Safely convert a value to int, extracting numeric part if needed (e.g., '11a' -> 11)"""
    if not value:
        return None
    try:
        # Try direct conversion first
        return int(value)
    except (ValueError, TypeError):
        # If that fails, try to extract numeric part using regex
        try:
            numeric_match = re.search(r'\d+', str(value))
            if numeric_match:
                return int(numeric_match.group())
        except:
            pass
    return None

def log_upload_success(postcode, inserted, updated, total_successful):
    """Log successfully uploaded records to a CSV file for endpoint tracking"""
    try:
        log_exists = os.path.exists(UPLOAD_SUCCESS_LOG)
        os.makedirs(os.path.dirname(UPLOAD_SUCCESS_LOG) if os.path.dirname(UPLOAD_SUCCESS_LOG) else '.', exist_ok=True)
        
        with open(UPLOAD_SUCCESS_LOG, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['Timestamp', 'Postcode', 'RecordsInserted', 'RecordsUpdated', 'TotalSuccessful'])
            if not log_exists:
                writer.writeheader()
            
            writer.writerow({
                'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'Postcode': postcode,
                'RecordsInserted': inserted,
                'RecordsUpdated': updated,
                'TotalSuccessful': total_successful
            })
    except Exception as e:
        safe_print(f"[ERROR] Failed to log upload success: {e}")

def clear_upload_success_log():
    """Clear the upload success log at the start of each upload session"""
    if os.path.exists(UPLOAD_SUCCESS_LOG):
        try:
            os.remove(UPLOAD_SUCCESS_LOG)
            safe_print(f"[🧹] Cleared {UPLOAD_SUCCESS_LOG}")
        except Exception as e:
            safe_print(f"[ERROR] Failed to clear {UPLOAD_SUCCESS_LOG}: {e}")

def supabase_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }

def get_existing_urns(postcode=None, limit=10000):
    """
    Fetch existing URNs from Supabase database.
    If postcode is provided, only fetch URNs for that postcode.
    Returns a set of URN strings.
    """
    # Check if credentials are available
    if not SUPABASE_URL or not SUPABASE_KEY:
        safe_print(f"[⚠️] Warning: SUPABASE_URL or SUPABASE_KEY not set. Cannot check existing URNs.")
        return set()
    
    headers = supabase_headers()
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?select=URN"
    
    if postcode:
        # URL encode the postcode for the filter
        encoded_postcode = urllib.parse.quote(postcode)
        url += f"&Postcode=eq.{encoded_postcode}"
    
    existing_urns = set()
    range_start = 0
    page_size = 1000
    
    try:
        while True:
            range_end = range_start + page_size - 1
            paginated_headers = {**headers, "Range": f"{range_start}-{range_end}"}
            
            response = requests.get(url, headers=paginated_headers, timeout=60)
            if response.status_code == 401:
                safe_print(f"[❌] Error: Authentication failed (401). Check SUPABASE_KEY in .env file.")
                safe_print(f"[❌] Cannot check existing URNs. Will skip upload to prevent duplicates.")
                return set()  # Return empty set to prevent upload
            elif response.status_code != 200:
                safe_print(f"[⚠️] Warning: Could not fetch existing URNs (status {response.status_code}). Will upload all records.")
                break
            
            data = response.json()
            if not data:
                break
            
            for record in data:
                urn = record.get("URN")
                if urn:
                    existing_urns.add(str(urn))
            
            if len(data) < page_size:
                break
            
            range_start += page_size
            
            # Safety limit to prevent infinite loops
            if len(existing_urns) >= limit:
                safe_print(f"[⚠️] Warning: Reached limit of {limit} existing URNs. Some may be missed.")
                break
                
    except Exception as e:
        safe_print(f"[⚠️] Warning: Error fetching existing URNs: {e}. Will upload all records.")
    
    return existing_urns

def delete_postcode_records(postcode):
    """Delete all rows from Supabase for a specific postcode."""
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            safe_print(f"[❌] Error: SUPABASE_URL or SUPABASE_KEY not set. Cannot delete records.")
            return False
        
        encoded_postcode = urllib.parse.quote(postcode)
        url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?Postcode=eq.{encoded_postcode}"
        response = requests.delete(url, headers=supabase_headers(), timeout=60)
        if response.status_code in [200, 204]:
            # Check how many records were deleted (if response includes count)
            deleted_count = 0
            try:
                if response.text:
                    result = response.json() if response.text else {}
                    deleted_count = result.get('count', 0) if isinstance(result, dict) else 0
            except:
                pass
            safe_print(f"[DELETE 🗑️] Removed all existing rows for {postcode} (deleted: {deleted_count if deleted_count > 0 else 'unknown'})")
            return True
        else:
            safe_print(f"[DELETE ❌] Failed to delete {postcode}: Status {response.status_code}, Response: {response.text}")
            return False
    except Exception as e:
        safe_print(f"[DELETE ERROR] Could not delete {postcode}: {e}")
        import traceback
        safe_print(f"[DELETE ERROR] Traceback: {traceback.format_exc()}")
        return False

def update_supabase_total(postcode):
    """Recalculate and update EPCTotalPerPostcode field for all rows of this postcode."""
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            return
        
        encoded_postcode = urllib.parse.quote(postcode)
        url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?Postcode=eq.{encoded_postcode}&select=URN"
        r = requests.get(url, headers=supabase_headers(), timeout=30)
        if r.status_code == 200:
            actual_count = len(r.json())
            safe_print(f"[INFO] Updating EPCTotalPerPostcode → {actual_count} for {postcode}")
            update_payload = {"EPCTotalPerPostcode": actual_count}
            patch_url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?Postcode=eq.{encoded_postcode}"
            requests.patch(patch_url, headers=supabase_headers(), json=update_payload, timeout=30)
        else:
            safe_print(f"[WARN] Could not fetch count for {postcode}: {r.text}")
    except Exception as e:
        safe_print(f"[WARN] Failed to update total for {postcode}: {e}")

def convert_to_dd_mm_yyyy(date_str):
    """Convert YYYY-MM-DD 00:00:00.000 to DD/MM/YYYY format"""
    if not date_str or pd.isna(date_str) or str(date_str).lower() == "nan":
        return None
    try:
        # Handle format: "2026-02-16 00:00:00.000" or "2026-02-16"
        date_part = str(date_str).split()[0]
        dt = datetime.strptime(date_part, "%Y-%m-%d")
        return dt.strftime("%d/%m/%Y")
    except:
        return None

def load_tracker():
    tracker = {}
    if not os.path.exists(TRACKER_PATH):
        return tracker
    with open(TRACKER_PATH, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            postcode = row["Postcode"]
            count = safe_int_from_string(row.get("EPCTotalPerPostcode")) or 0
            dates_str = row.get("ValidUntilDates", "").strip()
            # Use comma separator (client requirement: SG1 2AW,10,16/02/2026,18/09/2030)
            dates = set(d.strip() for d in dates_str.split(",") if d.strip())
            urns_str = row.get("URNs", "").strip()
            urns = set(u.strip() for u in urns_str.split(",") if u.strip())
            tracker[postcode] = {"count": count, "dates": dates, "urns": urns}
    return tracker

def save_tracker(tracker):
    with open(TRACKER_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Postcode", "EPCTotalPerPostcode", "ValidUntilDates", "URNs"])
        writer.writeheader()
        for postcode, val in tracker.items():
            # Use comma separator (client requirement)
            dates_str = ",".join(sorted(val["dates"])) if val["dates"] else ""
            urns_str = ",".join(sorted(val.get("urns", set()))) if val.get("urns") else ""
            writer.writerow({
                "Postcode": postcode,
                "EPCTotalPerPostcode": val["count"],
                "ValidUntilDates": dates_str,
                "URNs": urns_str
            })

def upload_to_supabase_bulk(records, postcode, use_upsert=False):
    headers = {
        "x-api-key": BULK_INSERT_API_KEY.strip(),
        "Content-Type": "application/json"
    }

    seen = set()
    unique_records = []
    for r in records:
        urn = r.get("URN")
        if urn and urn not in seen:
            seen.add(urn)
            unique_records.append(r)

    # Use "upsert" operation (API may require it), but we've already filtered out existing records
    # So it will effectively only insert new records
    # Use "upsert" if use_upsert=True (for date changes), otherwise "insert" (for count changes after delete)
    operation = "upsert" if use_upsert else "insert"
    payload = {
        "table_name": SUPABASE_TABLE,
        "data": unique_records,
        "operation": operation,
        "conflict_columns": ["URN"]
    }

    try:
        safe_print(f"[📤] Uploading {len(unique_records)} records to {SUPABASE_TABLE}...")
        safe_print(f"[🔗] Using BULK_INSERT_URL: {BULK_INSERT_URL[:50]}...")  # Show first 50 chars
        safe_print(f"[INFO] Payload table_name: {payload['table_name']}")
        safe_print(f"[INFO] Payload operation: {payload['operation']}")
        safe_print(f"[INFO] Payload conflict_columns: {payload['conflict_columns']}")
        if unique_records and len(unique_records) > 0:
            safe_print(f"[INFO] Sample record keys: {list(unique_records[0].keys())}")
        
        r = requests.post(BULK_INSERT_URL, headers=headers, json=payload, timeout=120)
        safe_print(f"[📥] Response status: {r.status_code}")
        
        result = r.json() if r.status_code in [200, 207] else {"error": r.text}
        
        if r.status_code in [200, 207]:
            inserted = result.get("records_inserted", 0)
            updated = result.get("records_updated", 0)
            failed = result.get("records_failed", 0)
            safe_print(f"[✅] Uploaded {len(unique_records)} records for {postcode} (Inserted={inserted}, Updated={updated}, Failed={failed})")
            
            if failed > 0:
                safe_print(f"[⚠️] {failed} records failed to upload. Check response: {result}")
                # Show detailed error information
                errors = result.get("errors", [])
                if errors:
                    safe_print(f"[ERROR DETAILS] Showing errors:")
                    for i, error_info in enumerate(errors[:3], 1):  # Show first 3 errors
                        safe_print(f"   Error {i}: {error_info.get('error', 'Unknown error')}")
                        if 'record_index' in error_info:
                            safe_print(f"   Record index: {error_info['record_index']}")
                        if 'record' in error_info and isinstance(error_info['record'], dict):
                            safe_print(f"   Record URN: {error_info['record'].get('URN', 'N/A')}")
                else:
                    # Check if there's error message in result
                    error_msg = result.get("error", "")
                    if error_msg:
                        safe_print(f"[ERROR MESSAGE] {error_msg}")
                # Show full result if it's small enough
                if len(str(result)) < 1000:
                    safe_print(f"[FULL RESPONSE] {result}")
            
            return inserted, updated, failed
        else:
            safe_print(f"[❌] Upload failed for {postcode}: Status {r.status_code}")
            safe_print(f"[❌] Error response: {result}")
            safe_print(f"[❌] Full response text: {r.text[:500]}")  # First 500 chars
            return 0, 0, len(unique_records)
    except requests.exceptions.RequestException as e:
        safe_print(f"[❌] Network error for {postcode}: {e}")
        import traceback
        safe_print(f"[❌] Traceback: {traceback.format_exc()}")
        return 0, 0, len(unique_records)
    except Exception as e:
        safe_print(f"[❌] Upload error for {postcode}: {e}")
        import traceback
        safe_print(f"[❌] Traceback: {traceback.format_exc()}")
        return 0, 0, len(unique_records)

async def upload_batch():
    # Log configuration at startup
    safe_print("Uploader started...")
    safe_print(f"[CONFIG] Table: {SUPABASE_TABLE}")
    env_table = os.getenv("SUPABASE_TABLE")
    if env_table and env_table != SUPABASE_TABLE:
        safe_print(f"[WARNING] SUPABASE_TABLE env var was '{env_table}' but forcing '{SUPABASE_TABLE}' for SimpleScraper")
    safe_print(f"[CONFIG] BULK_INSERT_URL: {BULK_INSERT_URL[:60] if BULK_INSERT_URL else 'NOT SET'}...")
    safe_print("")
    tracker = load_tracker()

    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        safe_print("✅ data.csv is empty or missing. Nothing to upload.")
        return

    try:
        # Read CSV with error handling for extra columns
        # Sometimes _OriginalTrackerDates column exists but isn't in header
        # Use python engine with error handling for malformed rows
        df = pd.read_csv(
            CSV_PATH, 
            on_bad_lines='skip',  # Skip malformed lines instead of warning
            engine='python',
            quoting=1,  # QUOTE_ALL
            skipinitialspace=True
        )
        
        # Expected columns (14)
        expected_columns = [
            'CertificateNumber', 'SourceUrl', 'Address', 'Postcode',
            'Rating', 'ValidUntilDate', 'Expired', 'ValidFromDate',
            'IsEmailSent', 'EPCTotalPerPostcode', 'HouseNumber', 'Street', 'City', 'Country'
        ]
        
        # Check if _OriginalTrackerDates column exists (it might be added by scraper)
        # If it exists but isn't in header, pandas might have read it as an unnamed column
        # Find any columns that don't match expected columns
        unexpected_cols = [col for col in df.columns if col not in expected_columns and not str(col).startswith('Unnamed')]
        
        # If _OriginalTrackerDates exists as a column or unnamed column, keep it for processing
        if '_OriginalTrackerDates' in df.columns:
            # Already a named column, keep it
            pass
        elif len(unexpected_cols) > 0:
            # Might be an extra column, rename the last one if it looks like dates
            last_col = df.columns[-1]
            if isinstance(last_col, str) and (',' in str(df[last_col].iloc[0] if len(df) > 0 else '') or last_col.startswith('Unnamed')):
                df = df.rename(columns={last_col: '_OriginalTrackerDates'})
        
        # Keep only expected columns plus _OriginalTrackerDates if it exists
        columns_to_keep = [col for col in expected_columns if col in df.columns]
        if '_OriginalTrackerDates' in df.columns:
            columns_to_keep.append('_OriginalTrackerDates')  # Keep for processing
        
        # Filter to only columns we want
        df = df[columns_to_keep]
        
    except pd.errors.EmptyDataError:
        safe_print("⚠️ data.csv is malformed.")
        return
    except pd.errors.ParserError as e:
        safe_print(f"⚠️ CSV parsing error: {e}")
        safe_print("Attempting to fix CSV by reading with error handling...")
        # Try reading with skip bad lines
        try:
            df = pd.read_csv(
                CSV_PATH, 
                on_bad_lines='skip', 
                engine='python',
                quoting=1,
                skipinitialspace=True
            )
            
            # Clean up columns
            expected_columns = [
                'CertificateNumber', 'SourceUrl', 'Address', 'Postcode',
                'Rating', 'ValidUntilDate', 'Expired', 'ValidFromDate',
                'IsEmailSent', 'EPCTotalPerPostcode', 'HouseNumber', 'Street', 'City', 'Country'
            ]
            
            columns_to_keep = [col for col in expected_columns if col in df.columns]
            if '_OriginalTrackerDates' in df.columns:
                columns_to_keep.append('_OriginalTrackerDates')
            
            df = df[columns_to_keep]
            safe_print("✅ CSV fixed and loaded successfully (some rows may have been skipped)")
        except Exception as e2:
            safe_print(f"❌ Failed to read CSV: {e2}")
            return
    except Exception as e:
        safe_print(f"⚠️ Error reading CSV: {e}")
        import traceback
        safe_print(traceback.format_exc())
        return

    if df.empty:
        safe_print("✅ No rows in CSV. Exiting.")
        return

    total_inserted = total_updated = total_skipped = total_failed = 0
    postcodes = df["Postcode"].dropna().unique()

    for postcode in postcodes:
        batch = df[df["Postcode"] == postcode]
     
        epc_count = None
        if not batch.empty and "EPCTotalPerPostcode" in batch.columns:
            epc_count_values = batch["EPCTotalPerPostcode"].dropna().unique()
            if len(epc_count_values) > 0:
                try:
                    # Extract numeric part only (handle cases like '11a' -> 11)
                    value = str(epc_count_values[0]).strip()
                    # Try to extract first numeric part
                    numeric_match = re.search(r'\d+', value)
                    if numeric_match:
                        epc_count = int(numeric_match.group())
                    else:
                        epc_count = None
                except (ValueError, TypeError, AttributeError):
                    epc_count = None
        
        # Fallback to batch length if EPCTotalPerPostcode not available
        if epc_count is None:
            epc_count = len(batch)
            safe_print(f"[⚠️] Using batch length as count for {postcode} (EPCTotalPerPostcode not found)")

        safe_print(f"\n🔍 Checking {postcode}: {epc_count} total EPCs (from first page), {len(batch)} records in batch")

        # Convert scraped dates to DD/MM/YYYY format for tracker update
        scraped_dates_dd_mm_yyyy = set()
        for _, row in batch.iterrows():
            date_dd_mm_yyyy = convert_to_dd_mm_yyyy(row.get("ValidUntilDate"))
            if date_dd_mm_yyyy:
                scraped_dates_dd_mm_yyyy.add(date_dd_mm_yyyy)

        # First time: Add to tracker, skip upload (as per original design)
        if postcode not in tracker:
            safe_print(f"[FIRST TIME] {postcode} → adding to tracker (no upload)")
            tracker[postcode] = {"count": epc_count, "dates": scraped_dates_dd_mm_yyyy}
            df = df[df["Postcode"] != postcode]
            df.to_csv(CSV_PATH, index=False)
            continue

        # Not first time: Check if this is a date change (partial update) or count change (full replace)
        # IMPORTANT: Use ORIGINAL tracker dates (before scraper update) if available in data.csv
        # Check if _OriginalTrackerDates field exists in the batch (set by scraper when dates changed)
        original_tracker_dates = None
        is_date_change_only = False
        if "_OriginalTrackerDates" in batch.columns:
            original_dates_str = batch["_OriginalTrackerDates"].dropna().iloc[0] if len(batch) > 0 else None
            if original_dates_str and original_dates_str.strip():
                original_tracker_dates = set(d.strip() for d in original_dates_str.split(',') if d.strip())
                # If we have original dates, this is likely a date change (not count change)
                # Count changes would send all records, date changes send only changed ones
                is_date_change_only = len(batch) < epc_count
        
        # Use original tracker dates if available, otherwise use current tracker dates
        existing_dates = original_tracker_dates if original_tracker_dates is not None else tracker[postcode]["dates"]
        new_dates = scraped_dates_dd_mm_yyyy - existing_dates
        
        safe_print(f"[📊] {postcode}: Tracker count: {tracker[postcode]['count']} → New count: {epc_count}")
        safe_print(f"[📊] {postcode}: Tracker dates: {len(existing_dates)} → Scraped dates: {len(scraped_dates_dd_mm_yyyy)} → New dates: {len(new_dates)}")
        
        # Prepare records from batch (these are only the records with changed dates if date change)
        records = []
        for _, row in batch.iterrows():
            record = {
                "URN": clean_value(row.get("CertificateNumber")),
                "SourceUrl": clean_value(row.get("SourceUrl")),
                "Address": clean_value(row.get("Address")),
                "Postcode": clean_value(row.get("Postcode")),
                "EnergyRating": clean_value(row.get("Rating")),
                "ValidUntilDate": clean_value(row.get("ValidUntilDate")),
                "Expired": clean_value(row.get("Expired")),
                "ValidFromDate": clean_value(row.get("ValidFromDate")),
                "EPCTotalPerPostcode": safe_int_from_string(clean_value(row.get("EPCTotalPerPostcode"))),
                "IsEmailSent": clean_value(row.get("IsEmailSent")),
                "HouseNumber": clean_value(row.get("HouseNumber")),
                "Street": clean_value(row.get("Street")),
                "City": clean_value(row.get("City")),
                "Country": clean_value(row.get("Country")),
            }
            records.append(record)
        
        # Count records with new dates for status reporting
        new_records_with_new_dates_count = 0
        for record in records:
            date_dd_mm_yyyy = convert_to_dd_mm_yyyy(record.get("ValidUntilDate"))
            if date_dd_mm_yyyy and date_dd_mm_yyyy in new_dates:
                new_records_with_new_dates_count += 1
        
        # Log STATUS line
        safe_print(f"[STATUS] Postcode {postcode}: new_records_found={len(records)}, new_records_found_with_new_dates={new_records_with_new_dates_count}, existing_records_skipped=0")
        
        # If this is a date change only (partial update), use UPSERT to update existing records
        # If this is a count change (full replace), delete all and insert all
        if is_date_change_only:
            # Date change: Update only the records that changed (using UPSERT)
            safe_print(f"[🔄] {postcode}: Date change detected - updating {len(records)} records (UPSERT)")
            if records:
                safe_print(f"[SUPABASE 🔄] Upserting {len(records)} record(s) for {postcode}...")
                inserted, updated, failed = upload_to_supabase_bulk(records, postcode, use_upsert=True)
                safe_print(f"[STATUS] Postcode {postcode}: records_inserted={inserted}, records_updated={updated}, records_failed={failed}")
                total_inserted += inserted
                total_updated += updated
                total_failed += failed
                
                # Log successfully uploaded records to tracking file
                if inserted > 0 or updated > 0:
                    log_upload_success(postcode, inserted, updated, inserted + updated)
                
                update_supabase_total(postcode)
            else:
                safe_print(f"[⏭️] {postcode}: No records to update")
                safe_print(f"[STATUS] Postcode {postcode}: records_inserted=0, records_updated=0, records_failed=0")
        else:
            # Count change: Delete all and insert all (as before)
            safe_print(f"[🗑️] {postcode}: Count change detected - deleting all existing records from database...")
            deleted = delete_postcode_records(postcode)
            
            if not deleted:
                safe_print(f"[⚠️] {postcode}: Failed to delete old records, skipping upload to prevent duplicates")
                # Remove from CSV but don't update tracker
                df = df[df["Postcode"] != postcode]
                df.to_csv(CSV_PATH, index=False)
                continue
            
            # Small delay to ensure delete completes before insert
            time.sleep(0.5)
            
            safe_print(f"[⬆️] {postcode}: Inserting all {len(records)} records")
            
            # Insert all records
            if records:
                safe_print(f"[SUPABASE ⬆️] Inserting {len(records)} record(s) for {postcode}...")
                inserted, updated, failed = upload_to_supabase_bulk(records, postcode)
                safe_print(f"[STATUS] Postcode {postcode}: records_inserted={inserted}, records_updated={updated}, records_failed={failed}")
                total_inserted += inserted
                total_updated += updated
                total_failed += failed
                
                # Log successfully uploaded records to tracking file
                if inserted > 0 or updated > 0:
                    log_upload_success(postcode, inserted, updated, inserted + updated)
                
                update_supabase_total(postcode)
            else:
                safe_print(f"[⏭️] {postcode}: No records to insert")
                safe_print(f"[STATUS] Postcode {postcode}: records_inserted=0, records_updated=0, records_failed=0")

        # Update tracker with all scraped dates (including new ones)
        tracker[postcode]["count"] = epc_count
        tracker[postcode]["dates"].update(scraped_dates_dd_mm_yyyy)

        # Remove from CSV
        df = df[df["Postcode"] != postcode]
        df.to_csv(CSV_PATH, index=False)
        safe_print(f"[🧹] Cleaned up {postcode} from data.csv")

    # Final save
    save_tracker(tracker)
    
    safe_print("\n📊 Upload Summary")
    safe_print(f"✅ Inserted : {total_inserted}")
    safe_print(f"🔁 Updated  : {total_updated}")
    safe_print(f"⏭️ Skipped  : {total_skipped}")
    safe_print(f"⚠️ Failed   : {total_failed}")
    safe_print("=================================================\n")

if __name__ == "__main__":
    asyncio.run(upload_batch())
