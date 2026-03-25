import os
import requests
import time
import json
from dotenv import load_dotenv
from datetime import datetime

# === Load environment variables ===
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = "ExtractedProperties"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

def safe_print(msg: str):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode("ascii", errors="ignore").decode())

# --------------------------------------------------------------------
# FETCH FUNCTION (with pagination to get all records)
# --------------------------------------------------------------------
def fetch_image_urls(propertyoutcode):
    """Fetch image URLs and related data from Supabase with pagination."""
    safe_print(f"Fetching records for PropertyOutcode={propertyoutcode} ...")
    
    all_data = []
    page_size = 1000  # Supabase default limit
    offset = 0
    
    while True:
        # Query with pagination - use limit and offset
        url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?PropertyOutcode=eq.{propertyoutcode}&select=PropertyId,PropertyEPC,Rating,CurrentScore,PotentialScore,PropertyOutcode,AlreadyScrapedEPC&limit={page_size}&offset={offset}"
        
        try:
            response = requests.get(url, headers=HEADERS, timeout=60)
            if response.status_code != 200:
                safe_print(f"Error fetching records: {response.text}")
                break
            
            data = response.json()
            
            if not data:
                break  # No more records
            
            all_data.extend(data)
            safe_print(f"Fetched {len(data)} records (offset {offset}, total so far: {len(all_data)})")
            
            # If we got fewer than page_size, we've reached the end
            if len(data) < page_size:
                break
            
            offset += page_size
            
        except Exception as e:
            safe_print(f"Exception during fetch: {e}")
            import traceback
            traceback.print_exc()
            break
    
    safe_print(f"Found {len(all_data)} properties needing EPC scrape.\n")
    # Return tuple: (PropertyId, PropertyEPC, Rating, CurrentScore, PotentialScore, PropertyOutcode)
    return [(item.get("PropertyId"), item.get("PropertyEPC"), item.get("Rating"), item.get("CurrentScore"), item.get("PotentialScore"), item.get("PropertyOutcode")) for item in all_data]


# --------------------------------------------------------------------
# UPDATE FUNCTION (Enhanced with Retry + Verification)
# --------------------------------------------------------------------
def update_supabase_record(property_id, rating, current_score, potential_score, already_scraped=1, retries=2):
    """
    Updates Rating, CurrentScore, PotentialScore, and AlreadyScrapedEPC fields
    in Supabase ExtractedProperties table for a given PropertyId.

    Features:
    - Retries on statement timeout (code 57014)
    - Prints clear logs (success, fail, retry)
    - Verifies the update after success
    - Logs any failures to logs/update_errors.txt
    """

    payload = {
        "Rating": rating,
        "CurrentScore": current_score,
        "PotentialScore": potential_score,
        "AlreadyScrapedEPC": already_scraped
    }

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?PropertyId=eq.{property_id}"

    for attempt in range(1, retries + 1):
        try:
            safe_print(f"[UPDATE] Updating PropertyId {property_id} -> {payload}")
            response = requests.patch(url, headers=HEADERS, json=payload)

            # --- Supabase statement timeout or transient error ---
            if "57014" in response.text:
                safe_print(f"[WARNING] Supabase timeout (Attempt {attempt}/{retries}) for PropertyId {property_id}. Retrying in 2s...")
                time.sleep(2)
                continue

            # --- Successful update ---
            if response.status_code in [200, 204]:
                safe_print(f"[SUCCESS] Updated PropertyId {property_id}: "
                      f"Rating={rating}, Current={current_score}, Potential={potential_score}")

                # --- Verify the update ---
                verify = requests.get(url, headers=HEADERS, timeout=30)
                if verify.status_code == 200:
                    result = verify.json()
                    if result:
                        safe_print(f"[VERIFIED] Update confirmed -> {json.dumps(result[0], indent=2)}\n")
                    else:
                        safe_print(f"[WARNING] Verification returned empty for PropertyId {property_id}\n")
                else:
                    safe_print(f"[WARNING] Verification request failed ({verify.status_code})\n")
                return True

            # --- Record not found (insert fallback) ---
            elif response.status_code == 404:
                insert_payload = {"PropertyId": property_id, **payload}
                insert_url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
                insert_response = requests.post(insert_url, headers=HEADERS, json=insert_payload, timeout=60)

                if insert_response.status_code in [200, 201]:
                    safe_print(f"[INSERT] Inserted PropertyId {property_id} into Supabase.\n")
                    return True
                else:
                    safe_print(f"[ERROR] Insert failed for PropertyId {property_id}: {insert_response.text}\n")
                    log_failed_update(property_id, insert_response.text)
                    return False

            # --- Any other API error ---
            else:
                safe_print(f"[ERROR] Failed (HTTP {response.status_code}) for PropertyId {property_id}: {response.text}\n")
                log_failed_update(property_id, response.text)
                return False

        except requests.exceptions.Timeout:
            safe_print(f"[TIMEOUT] Timeout while updating PropertyId {property_id} (Attempt {attempt}/{retries})")
            time.sleep(2)
            continue

        except Exception as e:
            safe_print(f"[ERROR] Exception updating PropertyId {property_id}: {e}")
            log_failed_update(property_id, str(e))
            time.sleep(2)
            continue

    safe_print(f"[FAILED] Gave up updating PropertyId {property_id} after {retries} retries.\n")
    log_failed_update(property_id, "Statement timeout or repeated failure")
    return False


# --------------------------------------------------------------------
# ##logging FUNCTION
# --------------------------------------------------------------------
def log_failed_update(property_id, error_message):
    """Log failed updates to a file for later review."""
    os.makedirs("logs", exist_ok=True)
    log_path = "logs/update_errors.txt"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now()}] PropertyId={property_id} -> {error_message}\n")
    safe_print(f"[LOG] Logged failure for PropertyId {property_id} -> {error_message[:100]}...\n")
