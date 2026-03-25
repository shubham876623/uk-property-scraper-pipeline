import os
import random
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import time
load_dotenv()

# === Supabase Configuration ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = "EPCCertificateDeepScrape"
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

# ============================================================
# #logging UTILITY
# ============================================================
def log(message):
    """Simple consistent logger with Unicode safety"""
    try:
        print(message, flush=True)
    except UnicodeEncodeError:
        safe_msg = str(message).encode("ascii", errors="ignore").decode("ascii")
        print(safe_msg, flush=True)

# ============================================================
# GET PROPERTIES WITH EPC (Previously from MSSQL)
# ============================================================
def get_properties_with_epc(outcode=None):
    """
    Fetch all unscripted properties with EPC from Supabase ExtractedProperties table.
    Uses pagination to fetch ALL records, not just the first 1000.
    Fetches records where AlreadyDeepScrapedEPC is false OR null (not yet scraped).
    
    Args:
        outcode: Optional postcode outcode filter (e.g., 'SG1', 'SG2'). 
                 If provided, only returns properties with postcodes starting with this outcode.
    
    Returns a list of dicts including PropertyId, PropertyPostCode, PropertyEPC, PropertyAddress.
    """
    all_properties = []
    page_size = 1000
    offset = 0
    
    # Query for records where AlreadyDeepScrapedEPC IS NULL
    # These are properties that have never been deep-scraped
    # Since Supabase doesn't support OR in a single filter, we query NULL and false separately
    log(f"[INFO] Fetching properties with AlreadyDeepScrapedEPC IS NULL for outcode: {outcode if outcode else 'all'}")
    while True:
        # Build query to match SQL structure: SELECT PropertyId, PropertyOutcode, PropertyPostCode, PropertyEPC, PropertyAddress
        # WHERE PropertyOutcode = 'HP1' AND AlreadyDeepScrapedEPC IS NULL AND PropertyEPC IS NOT NULL
        url = (
            f"{SUPABASE_URL}/rest/v1/ExtractedProperties"
            "?select=PropertyId,PropertyOutcode,PropertyPostCode,PropertyEPC,PropertyAddress"
            "&AlreadyDeepScrapedEPC=is.null&PropertyEPC=not.is.null"
            f"&limit={page_size}&offset={offset}"
        )
        
        # Add outcode filter if provided (matches SQL: WHERE PropertyOutcode = 'HP1')
        if outcode:
            # Use PropertyOutcode field for exact match (matches SQL WHERE clause)
            url += f"&PropertyOutcode=eq.{outcode.upper()}"
        
        # Log the query URL for debugging (first page only)
        if offset == 0:
            log(f"[DEBUG] Query URL (null records): {url}")

        try:
            response = requests.get(url, headers=HEADERS, timeout=60)
            if response.status_code != 200:
                log(f"[SUPABASE ERROR] Failed to fetch properties (null records): Status {response.status_code}")
                log(f"[SUPABASE ERROR] URL: {url}")
                log(f"[SUPABASE ERROR] Response: {response.text}")
                break

            data = response.json()
            # Check if response returned empty array (different from None)
            if data is None or (isinstance(data, list) and len(data) == 0):
                if offset == 0:
                    log(f"[INFO] No null records found for outcode filter: {outcode if outcode else 'all'}")
                break  # No more records
            
            # Debug: Log first record to verify structure
            if offset == 0 and len(data) > 0:
                log(f"[DEBUG] Sample null record: PropertyId={data[0].get('PropertyId')}, PropertyOutcode={data[0].get('PropertyOutcode')}, AlreadyDeepScrapedEPC={data[0].get('AlreadyDeepScrapedEPC')}")
            
            all_properties.extend(data)
            if offset == 0:
                log(f"[INFO] Found {len(data)} null records (page 1) for outcode: {outcode if outcode else 'all'}")
            else:
                log(f"[INFO] Found {len(data)} more null records (page {offset // page_size + 1})")
            
            # If we got fewer records than page_size, we've reached the end
            if len(data) < page_size:
                break
            
            offset += page_size
            
        except Exception as e:
            log(f"[SUPABASE ERROR] Error fetching properties (null records): {e}")
            log(f"[SUPABASE ERROR] URL: {url}")
            break
    
    # Also fetch records where AlreadyDeepScrapedEPC is explicitly false (if any exist)
    # This is a separate query since Supabase doesn't support OR in filters
    # We process BOTH NULL and false records to ensure all unscraped properties are included
    log(f"[INFO] Fetching properties with AlreadyDeepScrapedEPC = false for outcode: {outcode if outcode else 'all'}")
    offset_false = 0
    while True:
        # Build query for false records (matches SQL structure)
        # WHERE PropertyOutcode = 'HP1' AND AlreadyDeepScrapedEPC = false AND PropertyEPC IS NOT NULL
        url_false = (
            f"{SUPABASE_URL}/rest/v1/ExtractedProperties"
            "?select=PropertyId,PropertyOutcode,PropertyPostCode,PropertyEPC,PropertyAddress"
            "&AlreadyDeepScrapedEPC=eq.false&PropertyEPC=not.is.null"
            f"&limit={page_size}&offset={offset_false}"
        )
        
        if outcode:
            # Use PropertyOutcode field for exact match (matches SQL WHERE clause)
            url_false += f"&PropertyOutcode=eq.{outcode.upper()}"
        
        # Log the query URL for debugging (first page only)
        if offset_false == 0:
            log(f"[DEBUG] Query URL (false records): {url_false}")

        try:
            response_false = requests.get(url_false, headers=HEADERS, timeout=60)
            if response_false.status_code != 200:
                log(f"[SUPABASE ERROR] Failed to fetch properties (false records): Status {response_false.status_code}")
                log(f"[SUPABASE ERROR] URL: {url_false}")
                log(f"[SUPABASE ERROR] Response: {response_false.text}")
                break

            data_false = response_false.json()
            # Check if response returned empty array (different from None)
            if data_false is None or (isinstance(data_false, list) and len(data_false) == 0):
                if offset_false == 0:
                    log(f"[INFO] No false records found for outcode filter: {outcode if outcode else 'all'}")
                    # Debug: Check if the PropertyEPC filter is excluding records
                    # Try querying without PropertyEPC filter to see if that's the issue
                    test_url_no_epc = f"{SUPABASE_URL}/rest/v1/ExtractedProperties?select=PropertyId,PropertyOutcode,PropertyPostCode,PropertyEPC&AlreadyDeepScrapedEPC=eq.false&PropertyOutcode=eq.{outcode.upper()}&limit=5"
                    try:
                        test_resp = requests.get(test_url_no_epc, headers=HEADERS, timeout=30)
                        if test_resp.status_code == 200:
                            test_data = test_resp.json()
                            if test_data and len(test_data) > 0:
                                log(f"[DEBUG] Found {len(test_data)} false records WITHOUT PropertyEPC filter! PropertyEPC filter might be excluding records.")
                                log(f"[DEBUG] Sample record PropertyEPC: {test_data[0].get('PropertyEPC')}")
                    except Exception as e:
                        log(f"[DEBUG] Test query failed: {e}")
                break
            
            all_properties.extend(data_false)
            if offset_false == 0:
                log(f"[INFO] Found {len(data_false)} false records (page 1) for outcode: {outcode if outcode else 'all'}")
            
            if len(data_false) < page_size:
                break
            
            offset_false += page_size
            
        except Exception as e:
            log(f"[SUPABASE ERROR] Error fetching properties (false records): {e}")
            log(f"[SUPABASE ERROR] URL: {url_false}")
            break
    
    # Remove duplicates based on PropertyId (in case there's any overlap)
    # If PropertyId is null, use PropertyPostCode + PropertyAddress as fallback identifier
    seen_ids = set()
    seen_fallback_ids = set()  # For records without PropertyId
    unique_properties = []
    properties_without_id = []
    
    # Log summary of collected records
    null_count = len([p for p in all_properties if p.get('AlreadyDeepScrapedEPC') is None])
    false_count = len([p for p in all_properties if p.get('AlreadyDeepScrapedEPC') is False])
    log(f"[INFO] Total properties collected before deduplication: {len(all_properties)}")
    log(f"[INFO]   - Records with AlreadyDeepScrapedEPC IS NULL: {null_count}")
    log(f"[INFO]   - Records with AlreadyDeepScrapedEPC = false: {false_count}")
    
    for prop in all_properties:
        prop_id = prop.get('PropertyId')
        if prop_id:
            # Record has PropertyId - use it for deduplication
            if prop_id not in seen_ids:
                seen_ids.add(prop_id)
                unique_properties.append(prop)
            else:
                log(f"[DEBUG] Duplicate PropertyId found: {prop_id}, skipping")
        else:
            # Record doesn't have PropertyId - use PropertyPostCode + PropertyAddress as fallback
            # Note: These records can still be marked as scraped using postcode
            postcode = prop.get('PropertyPostCode', '')
            address = prop.get('PropertyAddress', '')
            fallback_id = f"{postcode}|{address}"
            
            if fallback_id not in seen_fallback_ids:
                seen_fallback_ids.add(fallback_id)
                unique_properties.append(prop)
                properties_without_id.append(prop)
            else:
                log(f"[DEBUG] Duplicate fallback ID found (Postcode+Address): {fallback_id}, skipping")
    
    if properties_without_id:
        log(f"[WARNING] Found {len(properties_without_id)} properties without PropertyId (will use postcode to mark as scraped).")
        log(f"[WARNING] First few examples: {properties_without_id[:3]}")
    
    log(f"[SUPABASE SUCCESS] Found {len(unique_properties)} unscripted properties needing deep scrape.")
    if len(all_properties) != len(unique_properties):
        log(f"[DEBUG] Removed {len(all_properties) - len(unique_properties)} duplicate properties.")
    return unique_properties

# ============================================================
# PROXY MANAGEMENT
# ============================================================
def load_proxies(file_path=None):
    """Load proxy list from file."""
    if file_path is None:
        # Default to proxies.txt in epc_deep_scraper directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_dir, "proxies.txt")
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            proxies = [line.strip() for line in f if line.strip()]
        return proxies
    except FileNotFoundError:
        log("[WARNING] proxies.txt not found. Running without proxies.")
        return []

PROXIES = load_proxies()

def get_random_proxy():
    """Return random proxy dict for requests."""
    proxy_url = random.choice(PROXIES) if PROXIES else None
    return {"http": proxy_url, "https": proxy_url} if proxy_url else None

# ============================================================
# GET EPC LINKS FROM WEB (with proxy rotation and retry)
# ============================================================
def get_epc_links_from_web(postcode, max_retries=5):
    """
    Fetch all EPC links for a given postcode from the UK EPC portal.
    Uses proxy rotation and retry logic to handle 403 Forbidden errors.
    """
    from epc_deep_scraper.src.headers import Headers, Cookies
    
    headers = Headers()
    cookies = Cookies()
    url = f"https://find-energy-certificate.service.gov.uk/find-a-certificate/search-by-postcode?postcode={postcode.replace(' ', '%20')}"
    
    # Try without proxy first (faster)
    try:
        response = requests.get(url, headers=headers, cookies=cookies, timeout=15)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            links = [
                (
                    "https://find-energy-certificate.service.gov.uk" + a["href"],
                    a.get_text(strip=True)
                )
                for a in soup.select("a[href^='/energy-certificate/']")
            ]
            log(f"[WEB SUCCESS] Found {len(links)} EPC links for {postcode} (no proxy)")
            return links
    except Exception as e:
        log(f"[WEB WARNING] Direct request failed for {postcode}: {e}")
    
    # If direct request fails or returns 403, try with proxies
    if not PROXIES:
        log(f"[WEB ERROR] No proxies available for {postcode}. Cannot retry.")
        return []
    
    # Try with different proxies
    used_proxies = set()
    for attempt in range(max_retries):
        proxy_dict = get_random_proxy()
        if not proxy_dict:
            break
        
        proxy_url = proxy_dict.get('http', '')
        if proxy_url in used_proxies and len(used_proxies) < len(PROXIES):
            # Try to get a different proxy
            continue
        used_proxies.add(proxy_url)
        
        try:
            log(f"[WEB RETRY] Attempt {attempt + 1}/{max_retries} for {postcode} using proxy: {proxy_url[:50]}...")
            response = requests.get(url, headers=headers, cookies=cookies, proxies=proxy_dict, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                links = [
                    (
                        "https://find-energy-certificate.service.gov.uk" + a["href"],
                        a.get_text(strip=True)
                    )
                    for a in soup.select("a[href^='/energy-certificate/']")
                ]
                log(f"[WEB SUCCESS] Found {len(links)} EPC links for {postcode} (proxy: {proxy_url[:30]}...)")
                return links
            elif response.status_code == 403:
                log(f"[WEB ERROR] 403 Forbidden for {postcode} with proxy {proxy_url[:30]}... (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(2 * (attempt + 1))  # Exponential backoff
                    continue
            else:
                log(f"[WEB ERROR] Status {response.status_code} for {postcode} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(1 * (attempt + 1))
                    continue
                    
        except requests.exceptions.Timeout:
            log(f"[WEB ERROR] Timeout for {postcode} with proxy {proxy_url[:30]}... (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(2 * (attempt + 1))
                continue
        except Exception as e:
            log(f"[WEB ERROR] Exception for {postcode} with proxy {proxy_url[:30]}...: {e} (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(1 * (attempt + 1))
                continue
    
    log(f"[WEB ERROR] Failed EPC lookup for {postcode} after {max_retries} attempts with proxies")
    return []

# ============================================================
# INSERT EPC DATA INTO SUPABASE
# ============================================================

def insert_epc_data(data):
    """
    Upserts EPC data into Supabase:
    - First checks if ID exists
    - If yes → updates the record
    - If no → inserts new record
    """

    epc_id = data.get("id")
    if not epc_id:
        log("[WARNING] Missing EPC ID - skipping insert.")
        return False

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            # Step 1: Check if record already exists
            check_url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?id=eq.{epc_id}&select=id"
            check_resp = requests.get(check_url, headers=HEADERS, timeout=30)

            if check_resp.status_code != 200:
                log(f"[CHECK ERROR] {epc_id} | Attempt {attempt}/{max_retries} | Status {check_resp.status_code} -> {check_resp.text}")
                if attempt < max_retries:
                    time.sleep(1 * attempt)
                    continue
                return False

            exists = len(check_resp.json()) > 0

            # Step 2: Update if exists, else insert
            if exists:
                update_url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?id=eq.{epc_id}"
                resp = requests.patch(update_url, headers=HEADERS, json=data, timeout=60)
                action = "Updated"
            else:
                insert_url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}"
                resp = requests.post(insert_url, headers=HEADERS, json=data, timeout=60)
                action = "Inserted"

            if resp.status_code in [200, 201, 204]:
                log(f"[SUCCESS] {action} EPC ID={epc_id}")
                # Reduced sleep time to speed up processing (was 0.2-0.6s, now 0.1-0.3s)
                time.sleep(random.uniform(0.1, 0.3))
                return True
            else:
                error_msg = resp.text if hasattr(resp, 'text') else str(resp.status_code)
                log(f"[{action} ERROR] EPC ID={epc_id} | Attempt {attempt}/{max_retries} | Status {resp.status_code} -> {error_msg}")
                if attempt < max_retries:
                    time.sleep(1 * attempt)  # Exponential backoff
                    continue
                return False

        except requests.exceptions.Timeout:
            log(f"[ERROR] EPC ID={epc_id} | Attempt {attempt}/{max_retries} | Request timeout")
            if attempt < max_retries:
                time.sleep(2 * attempt)
                continue
            return False
        except requests.exceptions.ConnectionError as e:
            log(f"[ERROR] EPC ID={epc_id} | Attempt {attempt}/{max_retries} | Connection error: {e}")
            if attempt < max_retries:
                time.sleep(2 * attempt)
                continue
            return False
        except Exception as e:
            log(f"[ERROR] Error processing EPC ID={epc_id} | Attempt {attempt}/{max_retries}: {e}")
            if attempt < max_retries:
                time.sleep(1 * attempt)
                continue
            return False
    
    log(f"[ERROR] EPC ID={epc_id} failed after {max_retries} attempts.")
    return False


# ============================================================
# MARK PROPERTY AS SCRAPED
# ============================================================
def mark_property_scraped(property_id=None, postcode=None, property_address=None, max_retries=3):
    """
    Update property row(s) to set AlreadyDeepScrapedEPC = true in Supabase ExtractedProperties table.
    Can update by PropertyId (preferred) or by postcode + address (fallback when PropertyId is missing).
    Note: AlreadyDeepScrapedEPC is a boolean field, so we use True (which JSON serializes to true).
    
    Args:
        property_id: The PropertyId to update (preferred method)
        postcode: The PropertyPostCode to match (fallback when property_id is None)
        property_address: Optional PropertyAddress to match (for more precise matching when property_id is None)
        max_retries: Maximum number of retry attempts (default: 3)
    
    Returns:
        bool: True if update succeeded, False otherwise
    """
    # Method 1: Update by PropertyId (preferred)
    if property_id:
        update_url = f"{SUPABASE_URL}/rest/v1/ExtractedProperties?PropertyId=eq.{property_id}"
        payload = {"AlreadyDeepScrapedEPC": True}  # Use boolean True, not integer 1
        
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.patch(update_url, headers=HEADERS, json=payload, timeout=60)
                
                # Check for successful update
                if response.status_code in [200, 204]:
                    # Verify the update actually succeeded by checking the record
                    verify_url = f"{SUPABASE_URL}/rest/v1/ExtractedProperties?PropertyId=eq.{property_id}&select=AlreadyDeepScrapedEPC"
                    verify_resp = requests.get(verify_url, headers=HEADERS, timeout=30)
                    
                    if verify_resp.status_code == 200:
                        verify_data = verify_resp.json()
                        if verify_data and len(verify_data) > 0:
                            if verify_data[0].get("AlreadyDeepScrapedEPC") is True:
                                log(f"[UPDATE SUCCESS] Marked PropertyId={property_id} as deep-scraped (verified).")
                                return True
                            else:
                                log(f"[UPDATE WARNING] PropertyId={property_id} update returned success but verification failed. Retrying...")
                                if attempt < max_retries:
                                    time.sleep(1 * attempt)  # Exponential backoff: 1s, 2s, 3s
                                    continue
                        else:
                            log(f"[UPDATE WARNING] PropertyId={property_id} not found for verification. Retrying...")
                            if attempt < max_retries:
                                time.sleep(1 * attempt)
                                continue
                    else:
                        log(f"[UPDATE WARNING] Verification failed for PropertyId={property_id}. Status: {verify_resp.status_code}")
                        # Still consider it success if the update returned 200/204
                        if attempt == max_retries:
                            log(f"[UPDATE SUCCESS] Marked PropertyId={property_id} as deep-scraped (update succeeded, verification skipped).")
                            return True
                    
                elif response.status_code == 404:
                    log(f"[UPDATE ERROR] PropertyId={property_id} not found in database.")
                    return False
                elif response.status_code == 409:
                    log(f"[UPDATE ERROR] PropertyId={property_id} conflict: {response.text}")
                    if attempt < max_retries:
                        time.sleep(2 * attempt)  # Wait longer for conflicts
                        continue
                    return False
                else:
                    error_msg = response.text if hasattr(response, 'text') else str(response.status_code)
                    log(f"[UPDATE ERROR] PropertyId={property_id} | Attempt {attempt}/{max_retries} | Status {response.status_code}: {error_msg}")
                    if attempt < max_retries:
                        time.sleep(1 * attempt)  # Exponential backoff
                        continue
                    return False
                    
            except requests.exceptions.Timeout:
                log(f"[UPDATE ERROR] PropertyId={property_id} | Attempt {attempt}/{max_retries} | Request timeout")
                if attempt < max_retries:
                    time.sleep(2 * attempt)  # Wait longer for timeouts
                    continue
                return False
            except requests.exceptions.ConnectionError as e:
                log(f"[UPDATE ERROR] PropertyId={property_id} | Attempt {attempt}/{max_retries} | Connection error: {e}")
                if attempt < max_retries:
                    time.sleep(2 * attempt)
                    continue
                return False
            except Exception as e:
                log(f"[UPDATE ERROR] PropertyId={property_id} | Attempt {attempt}/{max_retries} | Unexpected error: {e}")
                if attempt < max_retries:
                    time.sleep(1 * attempt)
                    continue
                return False
        
        log(f"[UPDATE FAILED] PropertyId={property_id} failed after {max_retries} attempts.")
        return False
    
    # Method 2: Update by postcode (fallback when property_id is None)
    if postcode:
        # Build query filter for postcode
        # URL encode the postcode (spaces become %20)
        encoded_postcode = postcode.replace(' ', '%20')
        update_url = f"{SUPABASE_URL}/rest/v1/ExtractedProperties?PropertyPostCode=eq.{encoded_postcode}"
        
        # Optionally add address filter for more precise matching
        if property_address:
            # For address matching, we might need to use ilike for partial matches
            # But Supabase REST API doesn't support complex filters easily
            # So we'll update all matching postcodes and log a warning
            log(f"[UPDATE INFO] Updating by postcode={postcode} (address provided but using postcode only for matching)")
        
        payload = {"AlreadyDeepScrapedEPC": True}
        
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.patch(update_url, headers=HEADERS, json=payload, timeout=60)
                
                # Check for successful update
                if response.status_code in [200, 204]:
                    # Verify by checking how many records were updated
                    verify_url = f"{SUPABASE_URL}/rest/v1/ExtractedProperties?PropertyPostCode=eq.{encoded_postcode}&AlreadyDeepScrapedEPC=eq.true&select=PropertyId"
                    verify_resp = requests.get(verify_url, headers=HEADERS, timeout=30)
                    
                    if verify_resp.status_code == 200:
                        verify_data = verify_resp.json()
                        updated_count = len(verify_data) if verify_data else 0
                        if updated_count > 0:
                            log(f"[UPDATE SUCCESS] Marked {updated_count} property/properties with postcode={postcode} as deep-scraped (verified).")
                            return True
                        else:
                            log(f"[UPDATE WARNING] Postcode={postcode} update returned success but no records found for verification. Retrying...")
                            if attempt < max_retries:
                                time.sleep(1 * attempt)
                                continue
                    else:
                        log(f"[UPDATE WARNING] Verification failed for postcode={postcode}. Status: {verify_resp.status_code}")
                        # Still consider it success if the update returned 200/204
                        if attempt == max_retries:
                            log(f"[UPDATE SUCCESS] Marked property/properties with postcode={postcode} as deep-scraped (update succeeded, verification skipped).")
                            return True
                
                elif response.status_code == 404:
                    log(f"[UPDATE ERROR] No properties found with postcode={postcode} in database.")
                    return False
                else:
                    error_msg = response.text if hasattr(response, 'text') else str(response.status_code)
                    log(f"[UPDATE ERROR] Postcode={postcode} | Attempt {attempt}/{max_retries} | Status {response.status_code}: {error_msg}")
                    if attempt < max_retries:
                        time.sleep(1 * attempt)
                        continue
                    return False
                    
            except requests.exceptions.Timeout:
                log(f"[UPDATE ERROR] Postcode={postcode} | Attempt {attempt}/{max_retries} | Request timeout")
                if attempt < max_retries:
                    time.sleep(2 * attempt)
                    continue
                return False
            except requests.exceptions.ConnectionError as e:
                log(f"[UPDATE ERROR] Postcode={postcode} | Attempt {attempt}/{max_retries} | Connection error: {e}")
                if attempt < max_retries:
                    time.sleep(2 * attempt)
                    continue
                return False
            except Exception as e:
                log(f"[UPDATE ERROR] Postcode={postcode} | Attempt {attempt}/{max_retries} | Unexpected error: {e}")
                if attempt < max_retries:
                    time.sleep(1 * attempt)
                    continue
                return False
        
        log(f"[UPDATE FAILED] Postcode={postcode} failed after {max_retries} attempts.")
        return False
    
    # No valid identifier provided
    log(f"[UPDATE ERROR] Cannot mark property as scraped - neither PropertyId nor postcode provided.")
    return False
