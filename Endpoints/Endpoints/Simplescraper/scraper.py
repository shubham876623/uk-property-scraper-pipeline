import aiohttp
import asyncio
from bs4 import BeautifulSoup
from datetime import datetime
import time
import os
import csv
import ssl
import certifi
from collections import deque
from threading import Lock

# Handle both relative and absolute imports
try:
    from .utils import calculate_valid_from_date
    from .headers import Headers
    from .proxy_handler import ProxyManager
    from .address_parser import extract_address_fields
except ImportError:
    # Fallback to absolute imports
    import sys
    import importlib.util
    
    # Load utils
    utils_path = os.path.join(os.path.dirname(__file__), 'utils.py')
    spec = importlib.util.spec_from_file_location("utils", utils_path)
    utils = importlib.util.module_from_spec(spec)
    sys.modules["utils"] = utils
    spec.loader.exec_module(utils)
    calculate_valid_from_date = utils.calculate_valid_from_date
    
    # Load headers
    headers_path = os.path.join(os.path.dirname(__file__), 'headers.py')
    spec = importlib.util.spec_from_file_location("headers", headers_path)
    headers_mod = importlib.util.module_from_spec(spec)
    sys.modules["headers"] = headers_mod
    spec.loader.exec_module(headers_mod)
    Headers = headers_mod.Headers
    
    # Load proxy_handler
    proxy_path = os.path.join(os.path.dirname(__file__), 'proxy_handler.py')
    spec = importlib.util.spec_from_file_location("proxy_handler", proxy_path)
    proxy_mod = importlib.util.module_from_spec(spec)
    sys.modules["proxy_handler"] = proxy_mod
    spec.loader.exec_module(proxy_mod)
    ProxyManager = proxy_mod.ProxyManager
    
    # Load address_parser
    parser_path = os.path.join(os.path.dirname(__file__), 'address_parser.py')
    spec = importlib.util.spec_from_file_location("address_parser", parser_path)
    parser_mod = importlib.util.module_from_spec(spec)
    sys.modules["address_parser"] = parser_mod
    spec.loader.exec_module(parser_mod)
    extract_address_fields = parser_mod.extract_address_fields

CSV_FILE = 'data.csv'
CSV_HEADERS = [
    'CertificateNumber', 'SourceUrl', 'Address', 'Postcode',
    'Rating', 'ValidUntilDate', 'Expired', 'ValidFromDate',
    'IsEmailSent', 'EPCTotalPerPostcode', 'HouseNumber', 'Street', 'City', 'Country', '_OriginalTrackerDates'
]

TRACKER_FILE = 'epc_count_tracker.csv'
COMPLETED_POSTCODES_FILE = 'logs/completed_postcodes.csv'
CHANGES_LOG_FILE = 'logs/changes_log.csv'  # Log file for date and count changes

def safe_print(msg):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode("ascii", errors="ignore").decode())

def safe_int(value, default=0):
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return default

def convert_to_dd_mm_yyyy(date_str):
    """Convert YYYY-MM-DD 00:00:00.000 to DD/MM/YYYY format"""
    if not date_str:
        return None
    try:
        # Handle format: "2026-02-16 00:00:00.000"
        dt = datetime.strptime(date_str.split()[0], "%Y-%m-%d")
        return dt.strftime("%d/%m/%Y")
    except:
        return None

def convert_from_dd_mm_yyyy(date_str):
    """Convert DD/MM/YYYY to YYYY-MM-DD 00:00:00.000 format"""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d 00:00:00.000")
    except:
        return None

class EPCScraper:
    def __init__(self, concurrency=150, proxy_file=None, skip_log='logs/skipped_postcodes.txt', batch_size=100):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.base_url = "https://find-energy-certificate.service.gov.uk/find-a-certificate/search-by-postcode?postcode={}"
        self.proxy_manager = ProxyManager(proxy_file)
        self.skip_log = skip_log
        self.total_processed = 0
        self.total_successful = 0
        self.total_skipped = 0
        self.total_failed = 0
        self.total_blocked = 0
        self.start_time = None
        
        # Batch writing optimization
        self.batch_size = batch_size
        self.csv_buffer = deque()  # Buffer for CSV records
        self.epc_log_buffer = deque()  # Buffer for EPC log records
        self.completed_postcodes_buffer = deque()  # Buffer for completed postcodes
        self.csv_lock = Lock()  # Lock for CSV operations
        self.tracker_lock = Lock()  # Lock for tracker operations
        
        # In-memory tracker cache
        self.tracker_cache = {}  # {postcode: {'count': int, 'dates': set}}
        self.tracker_dirty = set()  # Track which postcodes need to be written
        self._load_tracker_cache()

    def write_to_csv(self, cert):
        """Add certificate to buffer for batch writing"""
        with self.csv_lock:
            self.csv_buffer.append(cert)
            # Flush if buffer is full
            if len(self.csv_buffer) >= self.batch_size:
                self._flush_csv_buffer()
    
    def _flush_csv_buffer(self):
        """Flush CSV buffer to file"""
        if not self.csv_buffer:
            return
        
        file_exists = os.path.isfile(CSV_FILE)
        records_to_write = []
        
        # Drain buffer
        while self.csv_buffer:
            records_to_write.append(self.csv_buffer.popleft())
        
        # Write all records at once
        with open(CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADERS)
            if not file_exists or os.stat(CSV_FILE).st_size == 0:
                writer.writeheader()
            writer.writerows(records_to_write)

    def log_scraped_epc(self, cert):
        """Add EPC log entry to buffer for batch writing"""
        with self.csv_lock:
            self.epc_log_buffer.append({
                'DateScraped': datetime.now().strftime("%Y-%m-%d"),
                'Postcode': cert['Postcode'],
                'Address': cert['Address'],
                'CertificateNumber': cert['CertificateNumber'],
                'ValidUntilDate': cert['ValidUntilDate']
            })
            # Flush if buffer is full
            if len(self.epc_log_buffer) >= self.batch_size:
                self._flush_epc_log_buffer()
    
    def _flush_epc_log_buffer(self):
        """Flush EPC log buffer to file"""
        if not self.epc_log_buffer:
            return
        
        log_file = 'logs/scraped_epcs.csv'
        file_exists = os.path.isfile(log_file)
        records_to_write = []
        
        # Drain buffer
        while self.epc_log_buffer:
            records_to_write.append(self.epc_log_buffer.popleft())
        
        # Write all records at once
        with open(log_file, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['DateScraped', 'Postcode', 'Address', 'CertificateNumber', 'ValidUntilDate'])
            if not file_exists:
                writer.writeheader()
            writer.writerows(records_to_write)

    def _load_tracker_cache(self):
        """Load entire tracker file into memory cache at startup"""
        if not os.path.exists(TRACKER_FILE):
            return
        
        with open(TRACKER_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Handle both 'Postcode' and '\ufeffPostcode' (BOM)
                postcode_key = 'Postcode' if 'Postcode' in row else '\ufeffPostcode' if '\ufeffPostcode' in row else None
                if postcode_key:
                    postcode = row.get(postcode_key, '').strip()
                    if postcode:
                        count = safe_int(row.get('EPCTotalPerPostcode', 0), 0)
                        dates_str = row.get('ValidUntilDates', '').strip()
                        dates = set(d.strip() for d in dates_str.split(',') if d.strip()) if dates_str else set()
                        urns_str = row.get('URNs', '').strip()
                        urns = set(u.strip() for u in urns_str.split(',') if u.strip()) if urns_str else set()
                        self.tracker_cache[postcode] = {'count': count, 'dates': dates, 'urns': urns}
    
    def load_tracker_for_postcode(self, postcode):
        """Load existing tracker data for a postcode from memory cache. Returns (count, set of dates in DD/MM/YYYY format, set of URNs)"""
        with self.tracker_lock:
            if postcode in self.tracker_cache:
                data = self.tracker_cache[postcode]
                return data['count'], data['dates'].copy(), data.get('urns', set()).copy()
            return None, set(), set()
    
    def update_tracker_for_postcode(self, postcode, epc_count, valid_until_dates, urns=None):
        """Update tracker cache in memory. Write to file will happen periodically."""
        # Validate postcode - skip if invalid
        if not postcode or len(postcode.strip()) < 3 or not postcode.strip():
            safe_print(f"[WARNING] Skipping invalid postcode in tracker: '{postcode}'")
            return
        
        postcode = postcode.strip()
        
        # Convert dates to list format
        if isinstance(valid_until_dates, list):
            dates_list = valid_until_dates  # Keep all dates as-is (one per URN)
        else:
            dates_list = sorted(list(valid_until_dates))  # Convert set to sorted list
        
        # Update in-memory cache
        with self.tracker_lock:
            self.tracker_cache[postcode] = {
                'count': epc_count,
                'dates': set(dates_list),  # Store as set for comparison
                'urns': set(urns) if urns else set()
            }
            self.tracker_dirty.add(postcode)
    
    def _flush_tracker_cache(self):
        """Write all dirty tracker entries to file"""
        if not self.tracker_dirty:
            return
        
        with self.tracker_lock:
            # Read existing tracker file
            existing = {}
            if os.path.exists(TRACKER_FILE):
                try:
                    with open(TRACKER_FILE, 'r', encoding='utf-8-sig') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            postcode_key = 'Postcode' if 'Postcode' in row else '\ufeffPostcode' if '\ufeffPostcode' in row else None
                            if postcode_key:
                                postcode = row.get(postcode_key, '').strip()
                                if postcode and postcode not in self.tracker_cache:
                                    # Keep entries not in cache
                                    count = safe_int(row.get('EPCTotalPerPostcode', 0), 0)
                                    dates_str = row.get('ValidUntilDates', '').strip()
                                    dates = set(d.strip() for d in dates_str.split(',') if d.strip()) if dates_str else set()
                                    urns_str = row.get('URNs', '').strip()
                                    urns = set(u.strip() for u in urns_str.split(',') if u.strip()) if urns_str else set()
                                    existing[postcode] = {'count': count, 'dates': dates, 'urns': urns}
                except Exception as e:
                    safe_print(f"[WARNING] Error reading tracker file: {e}")
            
            # Merge cache into existing
            existing.update(self.tracker_cache)
            
            # Write back to file
            with open(TRACKER_FILE, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['Postcode', 'EPCTotalPerPostcode', 'ValidUntilDates', 'URNs'])
                writer.writeheader()
                for postcode, data in existing.items():
                    dates_list = sorted(list(data['dates']))
                    dates_str = ','.join(dates_list) if dates_list else ''
                    urns_list = sorted(list(data.get('urns', set())))
                    urns_str = ','.join(urns_list) if urns_list else ''
                    writer.writerow({
                        'Postcode': postcode,
                        'EPCTotalPerPostcode': str(data['count']),
                        'ValidUntilDates': dates_str,
                        'URNs': urns_str
                    })
            
            self.tracker_dirty.clear()

    async def run(self, postcodes):
        self.start_time = time.time()
        
        # Clear completed_postcodes.csv at the start of each scraping run
        self.clear_completed_postcodes()
        
        # Clear changes_log.csv at the start of each scraping run
        self.clear_changes_log()
        
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        # Increase connection pool size for better concurrency
        connector = aiohttp.TCPConnector(ssl=ssl_context, limit=200, limit_per_host=50)

        async with aiohttp.ClientSession(connector=connector) as session:
            # Start periodic flush task
            flush_task = asyncio.create_task(self._periodic_flush())
            
            try:
                tasks = [self.scrape_postcode(session, postcode) for postcode in postcodes]
                await asyncio.gather(*tasks)
            finally:
                # Cancel periodic flush and flush all buffers before finishing
                flush_task.cancel()
                try:
                    await flush_task
                except asyncio.CancelledError:
                    pass
                self._flush_all_buffers()
                self.print_summary()
    
    async def _periodic_flush(self):
        """Periodically flush buffers every 10 seconds"""
        try:
            while True:
                await asyncio.sleep(10)  # Flush every 10 seconds
                self._flush_all_buffers()
        except asyncio.CancelledError:
            pass
    
    def _flush_all_buffers(self):
        """Flush all pending buffers to disk"""
        with self.csv_lock:
            self._flush_csv_buffer()
            self._flush_epc_log_buffer()
            # Flush completed postcodes buffer
            if self.completed_postcodes_buffer:
                self._flush_completed_postcodes_buffer()
        
        # Flush tracker cache
        self._flush_tracker_cache()
    
    def _flush_completed_postcodes_buffer(self):
        """Flush completed postcodes buffer to file"""
        if not self.completed_postcodes_buffer:
            return
        
        completed_file = COMPLETED_POSTCODES_FILE
        os.makedirs(os.path.dirname(completed_file), exist_ok=True)
        
        file_exists = os.path.isfile(completed_file)
        records_to_write = []
        
        # Drain buffer
        while self.completed_postcodes_buffer:
            records_to_write.append(self.completed_postcodes_buffer.popleft())
        
        # Write all records at once
        with open(completed_file, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['postcode', 'completed_at', 'records_scraped'])
            if not file_exists or os.stat(completed_file).st_size == 0:
                writer.writeheader()
            writer.writerows(records_to_write)

    async def scrape_postcode(self, session, postcode):
        async with self.semaphore:
            url = self.base_url.format(postcode)
            retries, attempt = 3, 0
            success = False

            while attempt < retries and not success:
                proxy = await self.proxy_manager.get_next_proxy()
                cookies = {"_ga": "GA1.1.117...", "_ga_ZDCS1W2ZRM": "GS1.1.174..."}
                headers = Headers()
                attempt += 1

                try:
                    async with session.get(url, headers=headers, cookies=cookies, proxy=proxy, timeout=30) as resp:
                        if resp.status == 403:
                            safe_print(f"[BLOCKED] {postcode} | Attempt {attempt}")
                            if attempt == retries:
                                self.total_blocked += 1
                                self.total_failed += 1
                                self.log_skipped(postcode)
                            continue
                        if resp.status != 200:
                            raise Exception(f"HTTP {resp.status}")

                        html = await resp.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        table_rows = soup.select("table.govuk-table.epb-search-results tbody tr")

                        if not table_rows:
                            self.total_failed += 1
                            self.total_skipped += 1
                            self.log_skipped(postcode)
                            return

                        # Load existing tracker data for this postcode
                        prev_count, prev_dates, prev_urns = self.load_tracker_for_postcode(postcode)

                        # Extract all ValidUntilDates from first page (in DD/MM/YYYY format for tracker)
                        # Store ALL dates (one per URN), not just unique dates
                        scraped_dates_dd_mm_yyyy_list = []  # List to preserve all dates (one per URN)
                        scraped_dates_dd_mm_yyyy = set()  # Set for comparison with previous dates
                        scraped_urns = set()  # Set of all URNs scraped from this postcode
                        all_certs = []
                        epc_count = len(table_rows)

                        for row in table_rows:
                            link = row.select_one("a")
                            tds = row.select("td")

                            address = link.get_text(strip=True) if link else ''
                            source_url = f"https://find-energy-certificate.service.gov.uk{link['href']}" if link else ''
                            urn = link['href'].split('/')[-1] if link else ''
                            if urn:
                                scraped_urns.add(urn)

                            energy_rating = tds[0].get_text(strip=True) if len(tds) > 1 else ''
                            valid_until = tds[1].select_one("span")
                            expired_text = tds[1].select_one("strong")
                            valid_until_raw = valid_until.get_text(strip=True) if valid_until else ''
                            expired = 1 if expired_text and 'expired' in expired_text.get_text(strip=True).lower() else 0

                            valid_until_date, valid_from_date = None, None
                            valid_until_dd_mm_yyyy = None
                            try:
                                parsed_date = datetime.strptime(valid_until_raw, "%d %B %Y")
                                valid_until_date = parsed_date.strftime("%Y-%m-%d 00:00:00.000")
                                valid_until_dd_mm_yyyy = parsed_date.strftime("%d/%m/%Y")
                                valid_from_date = calculate_valid_from_date(valid_until_raw)
                            except Exception:
                                pass

                            if valid_until_dd_mm_yyyy:
                                scraped_dates_dd_mm_yyyy.add(valid_until_dd_mm_yyyy)
                                scraped_dates_dd_mm_yyyy_list.append(valid_until_dd_mm_yyyy)  # Store all dates (one per URN)

                            parsed_address = extract_address_fields(address)

                            cert = {
                                'CertificateNumber': urn,
                                'SourceUrl': source_url,
                                'Address': address,
                                'Postcode': postcode,
                                'Rating': energy_rating,
                                'ValidUntilDate': valid_until_date,
                                'Expired': expired,
                                'ValidFromDate': valid_from_date,
                                'IsEmailSent': 0,
                                'EPCTotalPerPostcode': epc_count,
                                'HouseNumber': str(parsed_address.get('HouseNumber', '')),
                                'Street': parsed_address.get('Street', ''),
                                'City': parsed_address.get('City', ''),
                                'Country': parsed_address.get('Country', ''),
                                'ValidUntilDateDDMMYYYY': valid_until_dd_mm_yyyy  # Store for comparison
                            }
                            all_certs.append(cert)

                        # Check for date change (comparing with tracker)
                        dates_changed = prev_dates != scraped_dates_dd_mm_yyyy

                        # Check for count change (comparing with tracker)
                        count_changed = prev_count is not None and prev_count != epc_count

                        # Check for URN change — only meaningful if we have previous URNs stored
                        # (prev_urns is empty for postcodes scraped before this fix was deployed)
                        urns_changed = bool(prev_urns) and prev_urns != scraped_urns

                        # Determine if this is first time scraping this postcode
                        is_first_time = prev_count is None
                        
                        # If first time: skip processing (just update tracker)
                        if is_first_time:
                            safe_print(f"[FIRST TIME] {postcode} → updating tracker only (no processing)")
                            # Store all dates (one per URN) in tracker
                            self.update_tracker_for_postcode(postcode, epc_count, scraped_dates_dd_mm_yyyy_list, scraped_urns)
                            self.log_completed_postcode(postcode, epc_count)
                            self.total_skipped += 1
                            self.log_skipped(postcode)
                            success = True
                            self.total_processed += 1
                            return

                        # Not first time: Only write to data.csv if there's a date change OR count change
                        # If change detected: write ALL records for this postcode to data.csv
                        # Uploader will DELETE old records and INSERT all new ones
                        if not dates_changed and not count_changed and not urns_changed:
                            # No changes detected - skip writing to data.csv
                            safe_print(f"[NO CHANGES] {postcode}: No date, count, or URN changes detected - skipping (count: {prev_count}→{epc_count})")
                            self.total_skipped += 1
                            self.log_skipped(postcode)
                            # Still update tracker with current values (all dates and URNs, one per URN)
                            self.update_tracker_for_postcode(postcode, epc_count, scraped_dates_dd_mm_yyyy_list, scraped_urns)
                            self.log_completed_postcode(postcode, epc_count)
                            success = True
                            self.total_processed += 1
                            return
                        
                        # Changes detected: identify which records to send
                        records_to_send = []
                        
                        if dates_changed:
                            removed_dates = prev_dates - scraped_dates_dd_mm_yyyy
                            added_dates = scraped_dates_dd_mm_yyyy - prev_dates
                            safe_print(f"[DATES CHANGED] {postcode}: Dates changed detected")
                            safe_print(f"   Previous dates: {sorted(list(prev_dates))}")
                            safe_print(f"   New dates: {sorted(list(scraped_dates_dd_mm_yyyy))}")
                            if added_dates:
                                safe_print(f"   Added dates: {sorted(list(added_dates))}")
                            if removed_dates:
                                safe_print(f"   Removed dates: {sorted(list(removed_dates))}")
                            
                            # Only include records with changed dates (dates that are new or were removed)
                            changed_dates = added_dates | removed_dates
                            
                            for cert in all_certs:
                                cert_date_dd_mm_yyyy = cert.get('ValidUntilDateDDMMYYYY')
                                # Include if this record's date is in the changed dates set
                                if cert_date_dd_mm_yyyy and cert_date_dd_mm_yyyy in changed_dates:
                                    records_to_send.append(cert)
                            
                            safe_print(f"   Writing {len(records_to_send)} records with changed dates to data.csv (out of {len(all_certs)} total)")
                            # Log date change to changes_log.csv
                            self.log_change(postcode, 'DATE_CHANGED', len(records_to_send), prev_count, epc_count, sorted(list(prev_dates)), sorted(list(scraped_dates_dd_mm_yyyy)))
                        elif count_changed:
                            safe_print(f"[COUNT CHANGED] {postcode}: Count changed ({prev_count}→{epc_count}) - writing all {len(all_certs)} records to data.csv (uploader will delete old and insert new)")
                            # For count changes, send all records (as before)
                            records_to_send = all_certs
                            # Log count change to changes_log.csv
                            self.log_change(postcode, 'COUNT_CHANGED', len(all_certs), prev_count, epc_count, sorted(list(prev_dates)), sorted(list(scraped_dates_dd_mm_yyyy)))

                        elif urns_changed:
                            # A certificate was replaced by a new one (same count, same dates, different URN)
                            # e.g. old EPC superseded by new EPC with same expiry date
                            removed_urns = prev_urns - scraped_urns
                            added_urns = scraped_urns - prev_urns
                            safe_print(f"[URN CHANGED] {postcode}: Certificate(s) replaced (count/dates unchanged) - writing all {len(all_certs)} records (uploader will delete old and insert new)")
                            if added_urns:
                                safe_print(f"   New URNs: {sorted(list(added_urns))}")
                            if removed_urns:
                                safe_print(f"   Removed URNs: {sorted(list(removed_urns))}")
                            # Send all records so uploader can do full delete + insert
                            records_to_send = all_certs
                            # Log URN change to changes_log.csv
                            self.log_change(postcode, 'URN_CHANGED', len(all_certs), prev_count, epc_count, sorted(list(prev_dates)), sorted(list(scraped_dates_dd_mm_yyyy)))

                        # Write only selected records to data.csv
                        for cert in records_to_send:
                            cert_copy = cert.copy()
                            cert_copy.pop('ValidUntilDateDDMMYYYY', None)
                            # Add original tracker dates to the record for the uploader
                            # Use the list of all dates for _OriginalTrackerDates
                            cert_copy['_OriginalTrackerDates'] = ','.join(sorted(list(prev_dates))) if prev_dates else ''
                            self.write_to_csv(cert_copy)
                            self.log_scraped_epc(cert_copy)
                            self.total_successful += 1

                        # Update tracker with all scraped dates, URNs, and count
                        self.update_tracker_for_postcode(postcode, epc_count, scraped_dates_dd_mm_yyyy_list, scraped_urns)
                        self.log_completed_postcode(postcode, len(all_certs))
                        safe_print(f"✅ {postcode}: Completed - {len(all_certs)} records written to data.csv")
                        success = True
                        self.total_processed += 1

                except asyncio.TimeoutError:
                    safe_print(f"[TIMEOUT] {postcode} | Attempt {attempt}")
                    if attempt == retries:
                        self.total_failed += 1
                        self.log_skipped(postcode)
                except Exception as e:
                    safe_print(f"[ERROR] {postcode} | Attempt {attempt} | {e}")
                    if attempt == retries:
                        self.total_failed += 1
                        self.log_skipped(postcode)

    def log_change(self, postcode, change_type, records_count, prev_count, new_count, prev_dates, new_dates):
        """Log date or count changes to changes_log.csv"""
        try:
            log_exists = os.path.exists(CHANGES_LOG_FILE)
            os.makedirs(os.path.dirname(CHANGES_LOG_FILE) if os.path.dirname(CHANGES_LOG_FILE) else '.', exist_ok=True)
            with open(CHANGES_LOG_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['Timestamp', 'Postcode', 'ChangeType', 'RecordsCount', 'PrevCount', 'NewCount', 'PrevDates', 'NewDates'])
                if not log_exists:
                    writer.writeheader()
                
                prev_dates_str = ','.join(prev_dates) if prev_dates else ''
                new_dates_str = ','.join(new_dates) if new_dates else ''
                
                writer.writerow({
                    'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'Postcode': postcode,
                    'ChangeType': change_type,
                    'RecordsCount': records_count,
                    'PrevCount': prev_count if prev_count is not None else '',
                    'NewCount': new_count,
                    'PrevDates': prev_dates_str,
                    'NewDates': new_dates_str
                })
        except Exception as e:
            safe_print(f"[ERROR] Failed to log change: {e}")

    def log_skipped(self, postcode):
        with open(self.skip_log, 'a', encoding='utf-8') as f:
            f.write(f"{postcode}\n")

    def clear_changes_log(self):
        """Clear changes_log.csv at the start of each scraping run"""
        try:
            if os.path.exists(CHANGES_LOG_FILE):
                os.remove(CHANGES_LOG_FILE)
        except Exception as e:
            safe_print(f"[WARNING] Failed to clear changes log: {e}")

    def clear_completed_postcodes(self):
        """Clear completed_postcodes.csv at the start of each scraping run."""
        completed_file = COMPLETED_POSTCODES_FILE
        # Ensure logs directory exists
        os.makedirs(os.path.dirname(completed_file), exist_ok=True)
        # Clear the file and write header
        with open(completed_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['postcode', 'completed_at', 'records_scraped'])
            writer.writeheader()
        safe_print(f"[🧹] Cleared {completed_file} for new scraping run")

    def log_completed_postcode(self, postcode, records_count):
        """Add completed postcode to buffer for batch writing"""
        with self.csv_lock:
            completed_at = datetime.now().isoformat()
            self.completed_postcodes_buffer.append({
                'postcode': postcode,
                'completed_at': completed_at,
                'records_scraped': records_count
            })
            # Flush if buffer is full
            if len(self.completed_postcodes_buffer) >= self.batch_size:
                self._flush_completed_postcodes_buffer()

    def print_summary(self):
        duration = time.time() - self.start_time
        safe_print("\n=================== SCRAPER SUMMARY ===================")
        safe_print(f"✅ Total Processed       : {self.total_processed}")
        safe_print(f"📥 Successfully Inserted : {self.total_successful}")
        safe_print(f"⚠️  Skipped Postcodes     : {self.total_skipped}")
        safe_print(f"🔒 Blocked (403)          : {self.total_blocked}")
        safe_print(f"⏱️  Total Time Taken       : {duration:.2f} seconds")
        safe_print("======================================================\n")
