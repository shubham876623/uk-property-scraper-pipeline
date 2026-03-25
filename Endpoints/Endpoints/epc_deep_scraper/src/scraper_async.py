import sys
import os
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import ssl
import certifi
import random

# Add parent directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)  # epc_deep_scraper directory
project_root = os.path.dirname(parent_dir)  # Endpoints directory

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Configure stdout encoding for Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

# Safe print function
def safe_print(msg: str):
    """Print message safely, handling Unicode encoding errors on Windows."""
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        safe_msg = msg.encode("ascii", errors="ignore").decode("ascii")
        print(safe_msg, flush=True)

# Imports
from epc_deep_scraper.database.db import insert_epc_data, mark_property_scraped
from epc_deep_scraper.src.utils import extract_epc_data, load_proxies
from epc_deep_scraper.src.headers import Headers, Cookies

# Load proxies
PROXIES = load_proxies()

class AsyncEPCScraper:
    def __init__(self, concurrency=10, max_retries=5):
        """
        Initialize async EPC scraper with concurrency control.
        
        Args:
            concurrency: Number of concurrent requests (default: 10)
            max_retries: Maximum retry attempts for failed requests (default: 5)
        """
        self.semaphore = asyncio.Semaphore(concurrency)
        self.max_retries = max_retries
        self.proxies = PROXIES
        self.proxy_index = 0
        self.proxy_lock = asyncio.Lock()
        
        # Statistics
        self.total_processed = 0
        self.total_successful = 0
        self.total_failed = 0
        self.failed_urls = []
        
    async def get_next_proxy(self):
        """Get next proxy in rotation (thread-safe)"""
        async with self.proxy_lock:
            if not self.proxies:
                return None
            proxy = self.proxies[self.proxy_index % len(self.proxies)]
            self.proxy_index += 1
            return proxy
    
    async def fetch_epc_links_async(self, session, postcode):
        """
        Fetch EPC links for a postcode using async requests with proxy rotation.
        """
        headers = Headers()
        cookies = Cookies()
        url = f"https://find-energy-certificate.service.gov.uk/find-a-certificate/search-by-postcode?postcode={postcode.replace(' ', '%20')}"
        
        # Try without proxy first
        try:
            async with session.get(url, headers=headers, cookies=cookies, timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    links = [
                        (
                            "https://find-energy-certificate.service.gov.uk" + a["href"],
                            a.get_text(strip=True)
                        )
                        for a in soup.select("a[href^='/energy-certificate/']")
                    ]
                    safe_print(f"[WEB SUCCESS] Found {len(links)} EPC links for {postcode} (no proxy)")
                    return links
        except Exception as e:
            safe_print(f"[WEB WARNING] Direct request failed for {postcode}: {e}")
        
        # If direct request fails or returns 403, try with proxies
        if not self.proxies:
            safe_print(f"[WEB ERROR] No proxies available for {postcode}")
            return []
        
        used_proxies = set()
        for attempt in range(self.max_retries):
            proxy_url = await self.get_next_proxy()
            if not proxy_url:
                break
            
            if proxy_url in used_proxies and len(used_proxies) < len(self.proxies):
                continue
            used_proxies.add(proxy_url)
            
            try:
                proxy_dict = {"http": proxy_url, "https": proxy_url}
                safe_print(f"[WEB RETRY] Attempt {attempt + 1}/{self.max_retries} for {postcode} using proxy: {proxy_url[:50]}...")
                
                async with session.get(url, headers=headers, cookies=cookies, proxy=proxy_url, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, "html.parser")
                        links = [
                            (
                                "https://find-energy-certificate.service.gov.uk" + a["href"],
                                a.get_text(strip=True)
                            )
                            for a in soup.select("a[href^='/energy-certificate/']")
                        ]
                        safe_print(f"[WEB SUCCESS] Found {len(links)} EPC links for {postcode} (proxy: {proxy_url[:30]}...)")
                        return links
                    elif response.status == 403:
                        safe_print(f"[WEB ERROR] 403 Forbidden for {postcode} with proxy {proxy_url[:30]}... (attempt {attempt + 1}/{self.max_retries})")
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(2 * (attempt + 1))
                            continue
                    else:
                        safe_print(f"[WEB ERROR] Status {response.status} for {postcode} (attempt {attempt + 1}/{self.max_retries})")
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(1 * (attempt + 1))
                            continue
            except asyncio.TimeoutError:
                safe_print(f"[WEB ERROR] Timeout for {postcode} with proxy {proxy_url[:30]}... (attempt {attempt + 1}/{self.max_retries})")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 * (attempt + 1))
                    continue
            except Exception as e:
                safe_print(f"[WEB ERROR] Exception for {postcode} with proxy {proxy_url[:30]}...: {e} (attempt {attempt + 1}/{self.max_retries})")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1 * (attempt + 1))
                    continue
        
        safe_print(f"[WEB ERROR] Failed EPC lookup for {postcode} after {self.max_retries} attempts with proxies")
        return []
    
    async def fetch_html_async(self, session, url):
        """
        Fetch HTML for an EPC certificate URL with retry and proxy rotation.
        """
        headers = Headers()
        cookies = Cookies()
        
        # Try without proxy first
        try:
            async with session.get(url, headers=headers, cookies=cookies, timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status == 200:
                    return await response.text()
        except Exception as e:
            pass
        
        # Try with proxies
        if not self.proxies:
            return None
        
        used_proxies = set()
        for attempt in range(self.max_retries):
            proxy_url = await self.get_next_proxy()
            if not proxy_url:
                break
            
            if proxy_url in used_proxies and len(used_proxies) < len(self.proxies):
                continue
            used_proxies.add(proxy_url)
            
            try:
                async with session.get(url, headers=headers, cookies=cookies, proxy=proxy_url, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 403:
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(2 * (attempt + 1))
                            continue
            except Exception as e:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1 * (attempt + 1))
                    continue
        
        return None
    
    async def scrape_postcode_async(self, session, postcode, property_id=None, property_address=None):
        """
        Scrape EPC certificates for a postcode asynchronously.
        """
        async with self.semaphore:
            safe_print(f"[SCRAPE] Scraping EPC Certificates for {postcode}...")
            
            cert_urls = await self.fetch_epc_links_async(session, postcode)
            safe_print(f"[INFO] Total Records for {postcode}: {len(cert_urls)}")
            
            if not cert_urls:
                return 0, 0, []
            
            success_count = 0
            failed_count = 0
            failed_urls = []
            
            # Process certificates concurrently (but with limit)
            tasks = []
            for cert_url, anchor_text in cert_urls:
                task = self.process_certificate_async(session, cert_url, anchor_text)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for idx, result in enumerate(results):
                cert_url = cert_urls[idx][0]
                if isinstance(result, Exception):
                    safe_print(f"[ERROR] Exception processing {cert_url}: {result}")
                    failed_count += 1
                    failed_urls.append(cert_url)
                elif result is True:
                    success_count += 1
                else:
                    failed_count += 1
                    failed_urls.append(cert_url)
            
            safe_print(f"[SUCCESS] Successfully scraped {success_count}/{len(cert_urls)} certificates for {postcode}.")
            if failed_count > 0:
                safe_print(f"[WARNING] Failed to process {failed_count} certificate(s) for {postcode}.")
                if len(failed_urls) <= 10:
                    for url in failed_urls:
                        safe_print(f"[FAILED] {url}")
                else:
                    safe_print(f"[FAILED] First 10 failed URLs:")
                    for url in failed_urls[:10]:
                        safe_print(f"[FAILED] {url}")
            
            return success_count, failed_count, failed_urls
    
    async def process_certificate_async(self, session, cert_url, anchor_text):
        """
        Process a single EPC certificate URL asynchronously.
        Returns True if successful, False otherwise.
        """
        html = await self.fetch_html_async(session, cert_url)
        if not html:
            safe_print(f"[ERROR] Failed to fetch: {cert_url}")
            return False
        
        try:
            cert_data = extract_epc_data(html, cert_url, anchor_text)
            # insert_epc_data is synchronous, so we run it in executor
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, insert_epc_data, cert_data)
            return result
        except Exception as e:
            safe_print(f"[WARNING] Error processing {cert_url}: {e}")
            return False
