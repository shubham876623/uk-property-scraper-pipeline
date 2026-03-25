import sys
import os

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

# Now use absolute imports
from epc_deep_scraper.database.db import get_epc_links_from_web, insert_epc_data, mark_property_scraped
from epc_deep_scraper.src.utils import fetch_html_with_retry, extract_epc_data

class EPCScraper:
    def scrape_postcode(self, postcode, property_id=None, property_address=None):
        safe_print(f"[SCRAPE] Scraping EPC Certificates for {postcode}...")

        cert_urls = get_epc_links_from_web(postcode)
        safe_print(f"[INFO] Total Records for {postcode}: {len(cert_urls)}")

        success_count = 0
        failed_count = 0
        failed_urls = []

        for cert_url, anchor_text in cert_urls:
            html = fetch_html_with_retry(cert_url)
            if not html:
                safe_print(f"[ERROR] Failed to fetch: {cert_url}")
                failed_count += 1
                failed_urls.append(cert_url)
                continue

            try:
                cert_data = extract_epc_data(html, cert_url, anchor_text)
                # Check return value - only count as success if insert_epc_data returns True
                if insert_epc_data(cert_data):
                    success_count += 1
                    epc_id = cert_data.get("id", "unknown")
                    safe_print(f"[LIVE] EPC Record Inserted: {epc_id} (Total: {success_count} for {postcode})")
                else:
                    # insert_epc_data returned False - insertion failed
                    epc_id = cert_data.get("id", "unknown")
                    safe_print(f"[ERROR] Failed to insert/update EPC ID={epc_id} from URL: {cert_url}")
                    failed_count += 1
                    failed_urls.append(cert_url)
            except Exception as e:
                safe_print(f"[WARNING] Error processing {cert_url}: {e}")
                failed_count += 1
                failed_urls.append(cert_url)

        safe_print(f"[SUCCESS] Successfully scraped {success_count}/{len(cert_urls)} certificates for {postcode}.")
        if failed_count > 0:
            safe_print(f"[WARNING] Failed to process {failed_count} certificate(s).")
            if len(failed_urls) <= 10:  # Only show URLs if not too many
                for url in failed_urls:
                    safe_print(f"[FAILED] {url}")
            else:
                safe_print(f"[FAILED] First 10 failed URLs:")
                for url in failed_urls[:10]:
                    safe_print(f"[FAILED] {url}")

        if success_count == len(cert_urls):
            # Try to mark as scraped - use PropertyId if available, otherwise use postcode
            if property_id:
                # Preferred method: Update by PropertyId
                if mark_property_scraped(property_id=property_id):
                    safe_print(f"[MARKED] Marked property {property_id} as scraped (by PropertyId).")
                else:
                    safe_print(f"[WARNING] Failed to mark property {property_id} as scraped.")
            else:
                # Fallback method: Update by postcode
                safe_print(f"[INFO] PropertyId is missing - attempting to mark as scraped using postcode={postcode}.")
                if mark_property_scraped(postcode=postcode):
                    safe_print(f"[MARKED] Marked property with postcode={postcode} as scraped (by postcode).")
                else:
                    safe_print(f"[WARNING] Failed to mark property with postcode={postcode} as scraped.")
        else:
            safe_print(f"[SKIP] Skipping mark - not all EPC records were scraped successfully for {postcode}.")
        
        # Return success count so main.py can track total EPC records inserted
        return success_count