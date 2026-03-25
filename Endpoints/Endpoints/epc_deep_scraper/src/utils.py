import sys
import os
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import random

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
        safe_msg = str(msg).encode("ascii", errors="ignore").decode("ascii")
        print(safe_msg, flush=True)

# Now use absolute imports
from epc_deep_scraper.src.address_parser import parse_unstructured_address
from epc_deep_scraper.src.headers import Headers, Cookies
# --- Fix and Format Date ---
def fix_date(date_str):
    if not date_str or date_str == "NULL":
        return None
    if "00:00:00" in date_str:  # Already in final format
        return date_str
    try:
        return datetime.strptime(date_str, "%d %B %Y").strftime("%Y-%m-%d 00:00:00")
    except:
        return None

# --- Load Proxies from File ---
def load_proxies():
    # Get the absolute path to proxies.txt relative to this file
    base_dir = os.path.dirname(os.path.dirname(__file__))  # goes up to epc_deep_scraper/
    file_path = os.path.join(base_dir, "proxies.txt")

    if not os.path.exists(file_path):
        safe_print(f"[WARNING] proxies.txt not found at: {file_path}. Running without proxies.")
        return []

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            proxies = [line.strip() for line in f if line.strip()]
        safe_print(f"[SUCCESS] Loaded {len(proxies)} proxies from {file_path}")
        return proxies
    except Exception as e:
        safe_print(f"[WARNING] Error loading proxies: {e}. Running without proxies.")
        return []

PROXIES = load_proxies()

def get_random_proxy():
    proxy_url = random.choice(PROXIES)
    return {
        "http": proxy_url,
        "https": proxy_url
    }

# --- Retry HTML Fetch (with improved proxy rotation) ---
def fetch_html_with_retry(url, retries=5, wait=2):
    """
    Fetch HTML with retry mechanism and proxy rotation.
    Improved to handle 403 Forbidden errors with multiple proxy attempts.
    """
    headers = Headers()
    cookies = Cookies()
    
    # Try without proxy first (faster)
    try:
        response = requests.get(url, headers=headers, cookies=cookies, timeout=15)
        if response.status_code == 200:
            return response.text
        elif response.status_code == 403:
            safe_print(f"[WARNING] 403 Forbidden (no proxy) - will try with proxies")
    except Exception as e:
        safe_print(f"[WARNING] Direct request failed: {e}")
    
    # If direct request fails or returns 403, try with proxies
    if not PROXIES:
        safe_print("[WARNING] No proxies available. Skipping retry.")
        return None
    
    used_proxies = set()
    for i in range(retries):
        proxy = get_random_proxy()
        if not proxy:
            break
        
        proxy_url = proxy.get('http', '')
        if proxy_url in used_proxies and len(used_proxies) < len(PROXIES):
            # Try to get a different proxy
            continue
        used_proxies.add(proxy_url)
        
        try:
            safe_print(f"[RETRY] Attempt {i + 1}/{retries} with proxy: {proxy_url[:50]}...")
            response = requests.get(url, headers=headers, cookies=cookies, proxies=proxy, timeout=15)
            if response.status_code == 200:
                safe_print(f"[SUCCESS] Success with proxy")
                return response.text
            elif response.status_code == 403:
                safe_print(f"[ERROR] 403 Forbidden with proxy {proxy_url[:30]}...")
                if i < retries - 1:
                    time.sleep(wait * (i + 1))  # Exponential backoff
                    continue
            else:
                safe_print(f"[ERROR] Status Code: {response.status_code}")
                if i < retries - 1:
                    time.sleep(wait)
                    continue
        except Exception as e:
            safe_print(f"[WARNING] Error with proxy {proxy_url[:30]}...: {e}")
            if i < retries - 1:
                time.sleep(wait)
                continue
    
    safe_print("[ERROR] All retries failed.")
    return None

# --- Utility ---
def extract_text_or_default(element, default=""):
    return element.text.strip() if element else default

# --- Extract EPC Data from HTML ---
def extract_epc_data(html, url, anchor_text=None):
    soup = BeautifulSoup(html, "html.parser")

    # Extract rating and address
    rating = extract_text_or_default(soup.find('p', {'class': 'epc-rating-result govuk-body'}))
    address_tag = soup.find('p', {'class': 'epc-address govuk-body'})
    address = extract_text_or_default(address_tag)
    address_parts = parse_unstructured_address(address)

    # Scores
    score_tags = soup.find_all('text', {'class': 'govuk-!-font-weight-bold'})
    current_score = extract_text_or_default(score_tags[0]) if len(score_tags) > 0 else ""
    potential_score = extract_text_or_default(score_tags[1]) if len(score_tags) > 1 else ""

    # Certificate details
    bold_tags = soup.find_all('p', {'class': 'govuk-body govuk-!-font-weight-bold'})
    expiry_date = extract_text_or_default(bold_tags[0]) if len(bold_tags) > 0 else ""
    certificate_id = extract_text_or_default(bold_tags[1]) if len(bold_tags) > 1 else ""

    # Summary data
    summary_data = {}
    for row in soup.find_all('div', {'class': 'govuk-summary-list__row'}):
        key_tag = row.find('dt', {'class': 'govuk-summary-list__key govuk-!-width-one-half'})
        value_tag = row.find('dd', {'class': 'govuk-summary-list__value govuk-!-width-one-half'})
        if key_tag and value_tag:
            summary_data[key_tag.text.strip()] = value_tag.text.strip()

    # Assessor data
    assessor = soup.find('div', {'class': 'govuk-body epc-blue-bottom printable-area epc-contact-assessor'}).find('dl')
    key_tags = assessor.find_all('dt', {'class': 'govuk-summary-list__key govuk-!-width-one-half'})
    value_tags = assessor.find_all('dd', {'class': 'govuk-summary-list__value govuk-!-width-one-half'})
    for k, v in zip(key_tags, value_tags):
        summary_data[k.text.strip()] = v.text.strip()

    # Expiry check
    expired = 1 if soup.find('div', {'class': 'govuk-warning-text'}) else 0
    if expired:
        expired_cert = summary_data.get("Certificate number", "NULL")
        expired_date = fix_date(summary_data.get("Expired on", "NULL"))
    else:
        expired_cert = url.split("/")[-1]
        expired_date = fix_date(expiry_date)

    # Construct Output
    output = {
        "accreditationAssessorID": summary_data.get("Assessor’s ID", "NULL"),
        "address": f"{address_parts.get('HouseNumber', '')} {address_parts.get('Street', '')} {address_parts.get('City', '')}".strip().replace("None", " "),
        "assessmentDate": fix_date(summary_data.get("Date of assessment", "NULL")),
        "assessorEmail": summary_data.get("Email", "NULL"),
        "assessorName": summary_data.get("Assessor’s name", "NULL"),
        "assessorPhone": summary_data.get("Telephone", "NULL"),
        "certificateDate": fix_date(summary_data.get("Date of certificate", "NULL")),
        "currentScore": current_score.strip(),
        "expired": expired,
        "expiryDate": fix_date(expiry_date),
        "floorArea": summary_data.get("Total floor area", "NULL"),
        "id": url.split("/")[-1],
        "locality": address_parts.get("City", "NULL"),
        "postCode": address_parts.get("Postcode", "NULL"),
        "potentialScore": potential_score.strip(),
        "propertyType": summary_data.get("Property type", "NULL"),
        "rating": rating,
        "url": url,
        "validtillDate": fix_date(expiry_date),
        "ValidFromDate": fix_date(summary_data.get("Date of certificate", "NULL")),
        "ExpiredCertificateNumber": expired_cert,
        "DateOfExpiredEPC": expired_date,
        "HouseNumber": address_parts.get("HouseNumber", "NULL"),
        "Street": address_parts.get("Street", "NULL"),
        "City": address_parts.get("City", "NULL"),
        "Country": "United Kingdom"
    }

    # print(output["accreditationAssessorID"])
    return output
