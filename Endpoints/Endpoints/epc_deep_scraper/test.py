import os
import requests
from dotenv import load_dotenv
import sys

# Configure stdout encoding for Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

# Safe print function for Windows
def safe_print(msg):
    """Print message safely, handling Unicode encoding errors on Windows."""
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        safe_msg = str(msg).encode("ascii", errors="ignore").decode("ascii")
        print(safe_msg, flush=True)

# === Load environment variables ===
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = "ExtractedProperties"
DEEP_SCRAPE_TABLE = "EPCCertificateDeepScrape"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}"
}

# ------------------------------------------------------------------
# ✅ Function to verify specific EPC IDs in database
# ------------------------------------------------------------------
def verify_epc_ids(epc_ids):
    """
    Verify if specific EPC IDs exist in the EPCCertificateDeepScrape table.
    
    Args:
        epc_ids: List of EPC IDs to verify (e.g., ['0561-2841-6535-0601-1955', '8106-2786-8422-1126-7203'])
    """
    safe_print(f"\n{'='*60}")
    safe_print(f"[VERIFY] Verifying {len(epc_ids)} EPC IDs in database...")
    safe_print(f"{'='*60}\n")
    
    found_count = 0
    not_found = []
    
    for epc_id in epc_ids:
        # Query for this specific EPC ID
        query_url = f"{SUPABASE_URL}/rest/v1/{DEEP_SCRAPE_TABLE}?id=eq.{epc_id}&select=id,address,postCode,rating,currentScore,potentialScore,url"
        
        try:
            response = requests.get(query_url, headers=HEADERS, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data and len(data) > 0:
                    record = data[0]
                    safe_print(record)
                    found_count += 1
                    safe_print(f"[FOUND] EPC ID={epc_id}")
                    safe_print(f"   Address: {record.get('address', 'N/A')}")
                    safe_print(f"   Postcode: {record.get('postCode', 'N/A')}")
                    safe_print(f"   Rating: {record.get('rating', 'N/A')}")
                    safe_print(f"   Current Score: {record.get('currentScore', 'N/A')}")
                    safe_print(f"   Potential Score: {record.get('potentialScore', 'N/A')}")
                    safe_print(f"   URL: {record.get('url', 'N/A')}")
                    safe_print("")
                else:
                    not_found.append(epc_id)
                    safe_print(f"[NOT FOUND] EPC ID={epc_id}")
                    safe_print("")
            else:
                safe_print(f"[ERROR] Failed to query EPC ID={epc_id} - Status {response.status_code}")
                safe_print(f"   Response: {response.text}")
                not_found.append(epc_id)
                safe_print("")
        except Exception as e:
            safe_print(f"[EXCEPTION] Error querying EPC ID={epc_id}: {e}")
            not_found.append(epc_id)
            safe_print("")
    
    safe_print(f"{'='*60}")
    safe_print(f"[VERIFY SUMMARY]")
    safe_print(f"  Total EPC IDs checked: {len(epc_ids)}")
    safe_print(f"  Found in database: {found_count}")
    safe_print(f"  Not found: {len(not_found)}")
    
    if not_found:
        safe_print(f"\n[WARNING] Missing EPC IDs:")
        for epc_id in not_found:
            safe_print(f"   - {epc_id}")
    else:
        safe_print(f"\n[SUCCESS] All EPC IDs verified and found in database!")
    
    safe_print(f"{'='*60}\n")
    
    return found_count, not_found

# ------------------------------------------------------------------
# ✅ Function to show all deep scraped data
# ------------------------------------------------------------------
def show_all_deep_scraped_data(limit=100, postcode=None, property_id=None):
    """
    Show all deep scraped EPC records from the database.
    
    Args:
        limit: Maximum number of records to show (default: 100)
        postcode: Optional postcode filter (e.g., "EX1 1AE")
        property_id: Optional property ID filter
    """
    safe_print(f"\n{'='*60}")
    safe_print(f"[DEEP SCRAPED DATA] Fetching records from database...")
    if postcode:
        safe_print(f"[FILTER] Postcode: {postcode}")
    if property_id:
        safe_print(f"[FILTER] Property ID: {property_id}")
    safe_print(f"{'='*60}\n")
    
    # Build query URL
    query_url = f"{SUPABASE_URL}/rest/v1/{DEEP_SCRAPE_TABLE}?select=id,address,postCode,rating,currentScore,potentialScore,url,assessmentDate,certificateDate&limit={limit}&order=id.desc"
    
    # Add filters
    if postcode:
        encoded_postcode = postcode.replace(' ', '%20')
        query_url += f"&postCode=eq.{encoded_postcode}"
    
    # Get count first
    count_headers = {**HEADERS, "Prefer": "count=exact"}
    try:
        count_url = query_url.split('&limit=')[0] + "&select=id"
        count_response = requests.get(count_url, headers=count_headers, timeout=30)
        if count_response.status_code == 200:
            content_range = count_response.headers.get('Content-Range', '0-0/0')
            total_count = int(content_range.split('/')[-1])
        else:
            total_count = 0
    except:
        total_count = 0
    
    safe_print(f"[INFO] Total records in database: {total_count}")
    safe_print(f"[INFO] Showing up to {limit} records\n")
    safe_print("-" * 100)
    
    try:
        response = requests.get(query_url, headers=HEADERS, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            safe_print(data)
            if not data or len(data) == 0:
                safe_print("[WARNING] No deep scraped records found.")
                if postcode:
                    safe_print(f"   (Filtered by postcode: {postcode})")
                if property_id:
                    safe_print(f"   (Filtered by Property ID: {property_id})")
            else:
                safe_print(f"\n[SUCCESS] Found {len(data)} record(s):\n")
                
                for idx, record in enumerate(data, 1):
                    safe_print(f"\n{idx}. EPC ID: {record.get('id', 'N/A')}")
                    safe_print(f"   Address: {record.get('address', 'N/A')}")
                    safe_print(f"   Postcode: {record.get('postCode', 'N/A')}")
                    safe_print(f"   Rating: {record.get('rating', 'N/A')}")
                    safe_print(f"   Current Score: {record.get('currentScore', 'N/A')}")
                    safe_print(f"   Potential Score: {record.get('potentialScore', 'N/A')}")
                    safe_print(f"   Assessment Date: {record.get('assessmentDate', 'N/A')}")
                    safe_print(f"   Certificate Date: {record.get('certificateDate', 'N/A')}")
                    safe_print(f"   URL: {record.get('url', 'N/A')}")
                
                if total_count > limit:
                    safe_print(f"\n[INFO] Showing {limit} of {total_count} total records.")
                    safe_print(f"[INFO] Use limit parameter to see more records.")
        else:
            safe_print(f"[ERROR] Failed to fetch data: Status {response.status_code}")
            safe_print(f"Response: {response.text}")
    
    except Exception as e:
        safe_print(f"[ERROR] Exception: {e}")
        import traceback
        traceback.print_exc()
    
    safe_print(f"\n{'='*60}\n")

# ------------------------------------------------------------------
# ✅ Function to check EPC records count in EPCCertificateDeepScrape table
# ------------------------------------------------------------------
def check_epc_count_by_postcode(postcode):
    """Check how many EPC records exist in EPCCertificateDeepScrape table for a given postcode."""
    # URL encode the postcode (spaces become %20)
    encoded_postcode = postcode.replace(' ', '%20')
    
    # First, get the count using Prefer: count=exact header
    count_url = f"{SUPABASE_URL}/rest/v1/{DEEP_SCRAPE_TABLE}?postCode=eq.{encoded_postcode}&select=id"
    count_headers = {
        **HEADERS,
        "Prefer": "count=exact"
    }
    
    try:
        r = requests.get(count_url, headers=count_headers, timeout=30)
        if r.status_code == 200:
            # Extract count from Content-Range header
            content_range = r.headers.get('Content-Range', '0-0/0')
            total_count = int(content_range.split('/')[-1])
        else:
            safe_print(f"[WARNING] Could not get count (Status {r.status_code}), trying to count from data...")
            total_count = 0
    except Exception as e:
        safe_print(f"[WARNING] Error getting count: {e}")
        total_count = 0
    
    safe_print(f"\n[INFO] Checking EPC records for postcode: {postcode}")
    safe_print(f"[INFO] Total records in EPCCertificateDeepScrape table: {total_count}")
    
    # Now get the actual data (first 50 records)
    data_url = f"{SUPABASE_URL}/rest/v1/{DEEP_SCRAPE_TABLE}?postCode=eq.{encoded_postcode}&select=id,address,postCode,rating,currentScore,potentialScore,url&limit=50&order=id.desc"
    r = requests.get(data_url, headers=HEADERS, timeout=30)
    
    if r.status_code == 200:
        data = r.json()
        safe_print(f"\n[INFO] Showing {len(data)} record(s) (showing up to 50):")
        safe_print("-" * 100)
        
        if len(data) > 0:
            for idx, record in enumerate(data, 1):
                safe_print(f"\n{idx}. EPC ID: {record.get('id', 'N/A')}")
                safe_print(f"   Address: {record.get('address', 'N/A')}")
                safe_print(f"   Postcode: {record.get('postCode', 'N/A')}")
                safe_print(f"   Rating: {record.get('rating', 'N/A')}")
                safe_print(f"   Current Score: {record.get('currentScore', 'N/A')}")
                safe_print(f"   Potential Score: {record.get('potentialScore', 'N/A')}")
                safe_print(f"   URL: {record.get('url', 'N/A')}")
        else:
            safe_print("[WARNING] No records found for this postcode.")
    else:
        safe_print(f"[ERROR] Error fetching data: Status {r.status_code}")
        safe_print(f"Response: {r.text}")
    
    return total_count

# ------------------------------------------------------------------
# ✅ Main execution
# ------------------------------------------------------------------

if __name__ == "__main__":
    # Check if showing all deep scraped data
    if len(sys.argv) > 1:
        first_arg = sys.argv[1]
        
        # If it's "all" or "show-all", show all deep scraped data
        if first_arg.lower() in ["all", "show-all", "list-all"]:
            limit = 100
            postcode = None
            property_id = None
            
            # Check for additional arguments
            for arg in sys.argv[2:]:
                if arg.startswith("limit="):
                    limit = int(arg.split("=")[1])
                elif arg.startswith("postcode="):
                    postcode = arg.split("=")[1]
                elif arg.startswith("property="):
                    property_id = arg.split("=")[1]
            
            show_all_deep_scraped_data(limit=limit, postcode=postcode, property_id=property_id)
            sys.exit(0)
        
        # If it's a verification request (verify:epc_id1,epc_id2,...) or multiple EPC IDs
        if first_arg.startswith("verify:") or (len(first_arg.split('-')) == 5):
            if first_arg.startswith("verify:"):
                epc_ids_str = first_arg.replace("verify:", "")
                epc_ids = [id.strip() for id in epc_ids_str.split(',')]
            else:
                # Treat all args as EPC IDs
                epc_ids = [arg.strip() for arg in sys.argv[1:] if len(arg.split('-')) == 5]
            
            if epc_ids:
                safe_print("=" * 100)
                safe_print("[DEEP SCRAPER] Verifying EPC IDs in database")
                safe_print("=" * 100)
                verify_epc_ids(epc_ids)
                sys.exit(0)
    
    # Otherwise, continue with normal postcode check
    postcode = sys.argv[1] if len(sys.argv) > 1 else "EX1 3SL"
    postcode = postcode.strip()
    
# Check if it's a full postcode (with space) - if so, check EPCCertificateDeepScrape table
if ' ' in postcode:
    safe_print("=" * 100)
    safe_print("[DEEP SCRAPER] Checking EPCCertificateDeepScrape table")
    safe_print("=" * 100)
    check_epc_count_by_postcode(postcode)
    safe_print("\n" + "=" * 100)
    sys.exit(0)
    
    # Otherwise, check ExtractedProperties table by outcode
    postcode = postcode.upper().strip()
    
    # Filter by PropertyOutcode (exact match)
    url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?select=*&PropertyOutcode=eq.{postcode}&limit=100"

safe_print(f"[INFO] Fetching records for postcode outcode: {postcode}\n")
safe_print(f"[INFO] Query URL: {url}\n")

try:
    response = requests.get(url, headers=HEADERS, timeout=30)
    if response.status_code == 200:
        data = response.json()

        if not data:
            safe_print(f"[WARNING] No records found for postcode outcode: {postcode}")
        else:
            safe_print(f"[SUCCESS] Retrieved {len(data)} record(s) for {postcode}:\n")
            
            # Show summary first
            safe_print(f"[SUMMARY]")
            safe_print(f"   Total records: {len(data)}")
            
            # Count records with/without PropertyId
            with_id = sum(1 for row in data if row.get('PropertyId'))
            without_id = len(data) - with_id
            safe_print(f"   Records with PropertyId: {with_id}")
            safe_print(f"   Records without PropertyId: {without_id}")
            
            # Count records with AlreadyDeepScrapedEPC status
            null_scraped = sum(1 for row in data if row.get('AlreadyDeepScrapedEPC') is None)
            false_scraped = sum(1 for row in data if row.get('AlreadyDeepScrapedEPC') is False)
            true_scraped = sum(1 for row in data if row.get('AlreadyDeepScrapedEPC') is True)
            safe_print(f"   AlreadyDeepScrapedEPC = NULL: {null_scraped}")
            safe_print(f"   AlreadyDeepScrapedEPC = false: {false_scraped}")
            safe_print(f"   AlreadyDeepScrapedEPC = true: {true_scraped}")
            safe_print("")
            
            # Show first 5 records in detail
            safe_print(f"[INFO] First 5 records (detailed):\n")
            for i, row in enumerate(data, start=1):
                safe_print(f"--- Record #{i} ---")
                safe_print(f"PropertyId: {row.get('PropertyId', 'NULL')}")
                safe_print(f"PropertyOutcode: {row.get('PropertyOutcode', 'NULL')}")
                safe_print(f"PropertyPostCode: {row.get('PropertyPostCode', 'NULL')}")
                safe_print(f"PropertyAddress: {row.get('PropertyAddress', 'NULL')}")
                safe_print(f"PropertyReferanceNumber: {row.get('PropertyReferanceNumber', 'NULL')}")
                safe_print(f"AlreadyDeepScrapedEPC: {row.get('AlreadyDeepScrapedEPC', 'NULL')}")
                safe_print(f"PropertyEPC: {row.get('PropertyEPC', 'NULL')}")
                safe_print("-----------------------------\n")
            
            if len(data) > 5:
                safe_print(f"... and {len(data)} more records (showing first 5 only)\n")

    else:
        safe_print(f"[ERROR] Error ({response.status_code}): {response.text}")

except Exception as e:
    safe_print(f"[ERROR] Exception: {e}")
    import traceback
    traceback.print_exc()