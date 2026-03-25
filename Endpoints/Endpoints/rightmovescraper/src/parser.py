
import os
import sys
import re
import json
from urllib.parse import quote
import asyncio
import ssl
import requests
import aiohttp
import certifi
import traceback
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from src.headers import get_headers
from src.cookies import get_cookies
from src.get_epc_image_url import get_valid_epc_url
from src.appendindb import appendingintodb
import logging
# Ensure UTF-8 console output
sys.stdout.reconfigure(encoding='utf-8')

TRANSACTION_HISTORY_URL = "https://www.rightmove.co.uk/properties/api/soldProperty/transactionHistory"


def fetch_sold_property_transactions(enc_id, headers=None, cookies=None, referer_url=None):
    """
    Fetch sold property transaction history for a property by encId.
    Returns the list soldPropertyTransactions from the API, or [] on failure.
    """
    if not enc_id:
        return []
    url = f"{TRANSACTION_HISTORY_URL}?encId={quote(enc_id, safe='')}"
    log("url for sold property transactions: " + url)
    try:
        h = dict(headers or get_headers())
        h["Accept"] = "application/json"
        if referer_url:
            h["Referer"] = referer_url
        resp = requests.get(url, headers=h, cookies=cookies or get_cookies(), timeout=15)
        resp.raise_for_status()

        data = resp.json()
        transactions = data.get("soldPropertyTransactions")
        if not isinstance(transactions, list):
            transactions = []
        if transactions:
            log(f"📋 Got {len(transactions)} sold transaction(s) for encId")
        else:
            log(f"📋 Transaction history API returned empty list for encId (no past sales data)")
        return transactions
    except requests.exceptions.HTTPError as e:
        log(f"⚠️ Transaction history API HTTP error: {e.response.status_code} for encId - {getattr(e.response, 'text', '')[:200]}", "WARN")
        return []
    except Exception as e:
        log(f"⚠️ Failed to fetch transaction history for encId: {e}", "WARN")
        return []


def format_uk_date(dt):
    """Return date in UK format DD/MM/YYYY."""
    if not dt:
        return None
    if isinstance(dt, str):
        return dt  # assume it's already in DD/MM/YYYY from Rightmove
    try:
        return dt.strftime("%d/%m/%Y")
    except:
        return None

# --------------------------------------------------------------------
# --- LOGGING UTILITIES ----------------------------------------------
# --------------------------------------------------------------------
os.makedirs("logs", exist_ok=True)

def log(msg, level="INFO"):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] [{level}] {msg}")

_logged_refs = set()

def log_data(data: dict, filename="logs/scraped_output.log"):
    """Append structured scraped data to a JSON log file safely. Skips duplicates."""
    try:
        ref = data.get("PropertyReference")
        if ref and ref in _logged_refs:
            log(f"⏭️ Skipping duplicate log for {ref}")
            return
        if ref:
            _logged_refs.add(ref)
        safe_data = json.loads(json.dumps(data, default=str, ensure_ascii=False))
        with open(filename, "a", encoding="utf-8") as f:
            f.write(json.dumps(safe_data, ensure_ascii=False) + "\n")
        log(f"🧾 Saved scraped data to {filename}")
    except Exception as e:
        log(f"⚠️ Failed to write scraped data: {e}", "WARN")

# --------------------------------------------------------------------
# --- JSON HISTORY EXTRACTOR -----------------------------------------
# --------------------------------------------------------------------
def _extract_history_from_json(data, history_type='price'):
    """Recursively search JSON structure for price or marketing history."""
    if isinstance(data, dict):
        # Check for direct keys
        if history_type == 'price':
            if 'priceHistory' in data:
                return data['priceHistory']
            if 'price_history' in data:
                return data['price_history']
        elif history_type == 'marketing':
            if 'marketingHistory' in data:
                return data['marketingHistory']
            if 'marketing_history' in data:
                return data['marketing_history']
        
        # Recursively search nested structures
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                result = _extract_history_from_json(value, history_type)
                if result:
                    return result
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, (dict, list)):
                result = _extract_history_from_json(item, history_type)
                if result:
                    return result
    return None

# --------------------------------------------------------------------
# --- DATETIME PARSER ------------------------------------------------
# --------------------------------------------------------------------
def parse_sql_datetime(dt_value):
    """Convert messy date strings into datetime objects; fallback to yesterday if invalid."""
    try:
        if isinstance(dt_value, datetime):
            return dt_value

        if isinstance(dt_value, str):
            match = re.search(r'(\d{2}/\d{2}/\d{4})', dt_value)
            if match:
                return datetime.strptime(match.group(1), "%d/%m/%Y")

            for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(dt_value, fmt)
                except ValueError:
                    continue

        raise ValueError(f"Unrecognized date format: {dt_value}")
    except Exception:
        log(f"⚠️ Failed to parse datetime '{dt_value}', defaulting to yesterday.", "WARN")
        return datetime.today() - timedelta(days=1)

# --------------------------------------------------------------------
# --- PROPERTY DETAIL PARSER (shared by sync and async) ---------------
# --------------------------------------------------------------------
def parse_property_details_from_html(html_text, url):
    """
    Parse property details from HTML. Used by both sync and async scrapers.
    Returns dict or None on parse error.
    """
    try:
        soup = BeautifulSoup(html_text, 'html.parser')

        propertyrefno = url.split('/')[-2].replace("#", " ").strip()
        rightmoveurl = url

        # ---------------------------------------------------------------
        # Basic info
        # ---------------------------------------------------------------
        property_address_tag = soup.find('h1', itemprop='streetAddress')
        property_address = property_address_tag.get_text(strip=True) if property_address_tag else None

        # Price extraction with multiple fallback selectors
        price_text = None
        for selector in [
            '._1gfnqJ3Vtd1z40MlC0MzXu span',
            '[data-testid="price"] span',
            'p._1hV1kqpVceE9SQ3horOEYN span',
        ]:
            price_tag = soup.select_one(selector)
            if price_tag:
                price_text = price_tag.get_text(strip=True)
                if price_text and '£' in price_text:
                    break
                price_text = None

        # Last resort: find any element with £ that looks like a price
        if not price_text:
            for tag in soup.find_all(string=lambda t: t and '£' in t and any(c.isdigit() for c in t)):
                candidate = tag.strip()
                # Only accept if it looks like a standalone price (e.g. "£250,000")
                if candidate.startswith('£') and len(candidate) < 20:
                    price_text = candidate
                    break

        final_price = price_text.replace('£', '').replace(',', '').strip() if price_text else "POA"

        # Sanity check: reject if price equals property ref number (data corruption)
        price_digits = re.sub(r'[^\d]', '', final_price)
        if price_digits and price_digits == str(propertyrefno):
            log(f"⚠️ Price '{final_price}' matches property ref {propertyrefno} — rejecting as corrupt, setting POA")
            final_price = "POA"

        # Bedrooms / Bathrooms
        bedrooms_tag = soup.find('dt', string=re.compile("BEDROOMS", re.I))
        bathrooms_tag = soup.find('dt', string=re.compile("BATHROOMS", re.I))
        num_bedrooms = bedrooms_tag.find_next('dd').get_text(strip=True) if bedrooms_tag else None
        num_bathrooms = bathrooms_tag.find_next('dd').get_text(strip=True) if bathrooms_tag else None

        # ---------------------------------------------------------------
        # Property Type + Tenure (Fixed)
        # ---------------------------------------------------------------
        propertylandtype, tenure_value = None, None
        detail_blocks = soup.find_all('div', {'class': '_3gIoc-NFXILAOZEaEjJi1n'})
        for div in detail_blocks:
            text = div.get_text(separator=' ', strip=True).upper()
            if "PROPERTY TYPE" in text:
                p_tag = div.find('p')
                propertylandtype = p_tag.get_text(strip=True) if p_tag else None
            elif "TENURE" in text:
                p_tag = div.find('p')
                tenure_value = p_tag.get_text(strip=True) if p_tag else None

        # ---------------------------------------------------------------
        # Agent details
        # ---------------------------------------------------------------
        agent_name = soup.select_one('.titleBar h3')
        agent_address = soup.select_one('.titleBar .address')
        agent_phone = soup.select_one('a[href^="tel:"]')

        agent_name_text = agent_name.get_text(strip=True) if agent_name else None
        # Remove Rightmove's leading "About" from agent name (e.g. "AboutHunters, Sedgley" -> "Hunters, Sedgley")
        if agent_name_text and agent_name_text.lower().startswith("about"):
            agent_name_text = agent_name_text[5:].lstrip()
        agent_address_text = agent_address.get_text(strip=True) if agent_address else None
        agent_phone_text = agent_phone.get('href').replace('tel:', '') if agent_phone else None

        # ---------------------------------------------------------------
        # Description, features, images
        # ---------------------------------------------------------------
        features = ",".join(li.get_text(strip=True) for li in soup.select('ul._1uI3IvdF5sIuBtRIvKrreQ li'))
        desc_div = soup.select_one('._3nPVwR0HZYQah5tkVJHFh5')
        description = desc_div.get_text(separator=',', strip=True) if desc_div else None

        floorplan_tag = soup.select_one('a[href*="floorplan"] img')
        floorplan_url = floorplan_tag['src'] if floorplan_tag else None
        image_tag = soup.find('meta', {'itemprop': 'contentUrl'})
        image_url = image_tag.get('content') if image_tag else None

        # ---------------------------------------------------------------
        # Metadata: lat/lng, outcode, incode, deliveryPointId, encId
        # ---------------------------------------------------------------
        script_tags = soup.find_all('script')
        latitude = longitude = incode = outcode = agent_profile_url = None
        delivery_point_id = None
        enc_id = None
        epc1 = "NULL"
        epc2 = "NULL"

        for script in script_tags:
            text = script.text
            if '"deliveryPointId"' in text or "'deliveryPointId'" in text:
                dp_match = re.search(r'"deliveryPointId"\s*:\s*"([^"]+)"', text)
                if not dp_match:
                    dp_match = re.search(r'"deliveryPointId"\s*:\s*(\d+)', text)
                if dp_match:
                    delivery_point_id = dp_match.group(1).strip()
                    log(f"📬 deliveryPointId: {delivery_point_id}")
            if '"encId"' in text or "'encId'" in text:
                enc_match = re.search(r'"encId"\s*:\s*"([^"]+)"', text)
                if enc_match:
                    enc_id = enc_match.group(1).strip()
                    log(f"🔐 encId: {enc_id}")
            if '"latitude"' in text and '"longitude"' in text:
                lat_match = re.search(r'"latitude"\s*:\s*([0-9\.\-]+)', text)
                lon_match = re.search(r'"longitude"\s*:\s*([0-9\.\-]+)', text)
                if lat_match and lon_match:
                    latitude = lat_match.group(1)
                    longitude = lon_match.group(1)
            if '"incode"' in text and '"outcode"' in text:
                incode_match = re.search(r'"incode"\s*:\s*"(.*?)"', text)
                outcode_match = re.search(r'"outcode"\s*:\s*"(.*?)"', text)
                if incode_match and outcode_match:
                    incode = incode_match.group(1)
                    outcode = outcode_match.group(1)
            if '"customerProfileUrl"' in text:
                match = re.search(r'"customerProfileUrl"\s*:\s*"(.*?)"', text)
                if match:
                    agent_profile_url = "https://www.rightmove.co.uk/" + match.group(1)
            if '"epcGraphs"' in text:
                # Try multiple regex patterns to catch different JSON structures
                epc_urls = []
                
                # Pattern 1: Look for epcGraphs array with url fields
                pattern1 = r'"epcGraphs"\s*:\s*\[(.*?)\]'
                match1 = re.search(pattern1, text, re.DOTALL)
                if match1:
                    epc_array_content = match1.group(1)
                    # Extract all URLs from the array content
                    url_matches = re.findall(r'"url"\s*:\s*"([^"]+)"', epc_array_content)
                    epc_urls.extend(url_matches)
                
                # Pattern 2: Direct URL extraction with full path
                if not epc_urls:
                    pattern2 = r'"epcGraphs"\s*:\s*\[.*?"url"\s*:\s*"(https://[^"]+\.gif)"'
                    epc_urls = re.findall(pattern2, text, re.DOTALL)
                
                # Pattern 3: More flexible pattern for relative or absolute URLs
                if not epc_urls:
                    pattern3 = r'"epcGraphs"\s*:\s*\[.*?"url"\s*:\s*"([^"]+\.gif)"'
                    epc_urls = re.findall(pattern3, text, re.DOTALL)
                
                # Pattern 4: Look for any URL containing "epc" or "media.rightmove"
                if not epc_urls:
                    pattern4 = r'(https://media\.rightmove\.co\.uk/[^"]*epc[^"]*\.gif)'
                    epc_urls = re.findall(pattern4, text, re.IGNORECASE)
                
                log(f"🔍 Found {len(epc_urls)} EPC URLs: {epc_urls}")
                
                # Process found URLs
                if len(epc_urls) >= 1:
                    url1 = epc_urls[0].strip()
                    if url1.startswith("https://"):
                        epc1 = url1
                    elif url1.startswith("//"):
                        epc1 = "https:" + url1
                    elif url1.startswith("/"):
                        epc1 = "https://media.rightmove.co.uk" + url1
                    else:
                        epc1 = "https://media.rightmove.co.uk/" + url1 if url1 else "NULL"
                    log(f"✅ EPC1 set to: {epc1}")
                else:
                    log(f"⚠️ No EPC1 URL found in epcGraphs")
                
                if len(epc_urls) >= 2:
                    url2 = epc_urls[1].strip()
                    if url2.startswith("https://"):
                        epc2 = url2
                    elif url2.startswith("//"):
                        epc2 = "https:" + url2
                    elif url2.startswith("/"):
                        epc2 = "https://media.rightmove.co.uk" + url2
                    else:
                        epc2 = "https://media.rightmove.co.uk/" + url2 if url2 else "NULL"
                    log(f"✅ EPC2 set to: {epc2}")
                else:
                    log(f"⚠️ No EPC2 URL found in epcGraphs")


        property_postcode = f"{outcode} {incode}".strip() if outcode and incode else None

   
        # Created / Added Date (UK Format)
        # ---------------------------------------------------------------
        created_date = None
        added_tag = soup.find("div", {'class': '_2nk2x6QhNB1UrxdI5KpvaF'})

        if added_tag:
            added_text = added_tag.text.strip()

            if "Added on" in added_text:
                try:
                    created_date = datetime.strptime(
                        added_text.replace("Added on", "").strip(),
                        "%d/%m/%Y"
                    )
                except Exception:
                    created_date = None
            # Do NOT set created_date from "Reduced on" - that is the reduction date, not when the
            # property was first listed. PropertyCreatedDate must stay as first list date (e.g. from
            # JSON/API or fallback). "Reduced on" is only for price history.

            elif "today" in added_text.lower():
                created_date = datetime.utcnow()

            elif "yesterday" in added_text.lower():
                created_date = datetime.utcnow() - timedelta(days=1)

        # ✅ FINAL SAFETY FALLBACK (THIS IS THE IMPORTANT PART)
        if not created_date:
            created_date = datetime.utcnow()

        created_date_iso = created_date.strftime("%Y-%m-%d")


        # ...
        # "PropertyCreatedDate": created_date_iso,


        # ---------------------------------------------------------------
        # Status / EPC / Agent image
        # ---------------------------------------------------------------
        status_removed = soup.find('div', {'class': '_1_ReydbZyb288nsZPkRSw_'})
        status_removed_text = status_removed.get_text(strip=True) if status_removed else None
        
        # Check if property is removed/withdrawn
        status_removed_lower = status_removed_text.lower() if status_removed_text else ""
        if status_removed_text and ("removed" in status_removed_lower or "withdrawn" in status_removed_lower):
            # Property has been removed/withdrawn - mark as "Removed"
            status = "Removed"
            log(f"⚠️ Property {propertyrefno} is marked as removed/withdrawn on Rightmove")
        else:
            # Normal status badge
            status_tag = soup.find('span', {'class': 'ksc_lozenge berry _2WqVSGdiq2H4orAZsyHHgz'})
            status = status_tag.text.strip() if status_tag else "For Sale"


        # epc1 = get_valid_epc_url(image_url)
        # epc2 = "NULL"
        agent_image_url = None
        try:
            agent_image_url = soup.find('a', {'class': '_3uq285qlcTkSZrCuXYW-zQ'}).find('img').get('src')
        except Exception:
            pass

        # ---------------------------------------------------------------
        # Price History Extraction
        # ---------------------------------------------------------------
        price_history = []
        marketing_history = []
        
        # Try to extract from JSON data in script tags
        for script in script_tags:
            text = script.text
            try:
                # Look for JSON data - try multiple patterns
                json_patterns = [
                    r'window\.__[A-Z_]+__\s*=\s*({.*?});',
                    r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
                    r'window\.__PRELOADED_STATE__\s*=\s*({.*?});',
                    r'var\s+\w+\s*=\s*({.*?});',
                    r'const\s+\w+\s*=\s*({.*?});',
                ]
                
                for pattern in json_patterns:
                    json_matches = re.finditer(pattern, text, re.DOTALL)
                    for json_match in json_matches:
                        try:
                            json_data = json.loads(json_match.group(1))
                            # Recursively search for history data
                            if not price_history:
                                found_price = _extract_history_from_json(json_data, 'price')
                                if found_price and isinstance(found_price, list):
                                    # Normalize the data format
                                    normalized_price = []
                                    for item in found_price:
                                        if isinstance(item, dict):
                                            # Try different possible field names
                                            price_val = item.get('price') or item.get('Price') or item.get('amount') or item.get('value')
                                            date_val = item.get('date') or item.get('Date') or item.get('changedDate') or item.get('timestamp')
                                            if price_val:
                                                # Convert date if it's a string (include "YYYY-MM-DD HH:MM:SS")
                                                if isinstance(date_val, str):
                                                    try:
                                                        date_part = date_val.split('T')[0].split()[0] if date_val else ""
                                                        for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y", "%d-%m-%Y"]:
                                                            try:
                                                                date_val = datetime.strptime(date_part, fmt)
                                                                break
                                                            except (ValueError, TypeError):
                                                                continue
                                                    except Exception:
                                                        date_val = created_date
                                                elif not date_val:
                                                    date_val = created_date
                                                normalized_price.append({
                                                    'price': str(price_val),
                                                    'date': date_val.strftime("%Y-%m-%d")
                                                })

                                    price_history = normalized_price
                                    if price_history:
                                        log(f"✅ Found {len(price_history)} price history records in JSON for {propertyrefno}")
                            
                            if not marketing_history:
                                found_marketing = _extract_history_from_json(json_data, 'marketing')
                                if found_marketing and isinstance(found_marketing, list):
                                    # Normalize the data format
                                    normalized_marketing = []
                                    for item in found_marketing:
                                        if isinstance(item, dict):
                                            # Try different possible field names
                                            status_val = item.get('status') or item.get('Status') or item.get('state') or item.get('type')
                                            date_val = item.get('date') or item.get('Date') or item.get('changedDate') or item.get('timestamp')
                                            if status_val:
                                                # Convert date if it's a string
                                                if isinstance(date_val, str):
                                                    try:
                                                        # Try common date formats
                                                        for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y", "%d-%m-%Y"]:
                                                            try:
                                                                date_val = datetime.strptime(date_val.split('T')[0], fmt)
                                                                break
                                                            except:
                                                                continue
                                                    except:
                                                        date_val = created_date
                                                elif not date_val:
                                                    date_val = created_date
                                                normalized_marketing.append({'status': str(status_val), 'date':  date_val.strftime("%Y-%m-%d")})
                                    marketing_history = normalized_marketing
                                    if marketing_history:
                                        log(f"✅ Found {len(marketing_history)} marketing history records in JSON for {propertyrefno}")
                            
                            # If we found both, we can break
                            if price_history and marketing_history:
                                break
                        except (json.JSONDecodeError, ValueError) as e:
                            continue  # Try next pattern
                
                # Also look for simple JSON arrays in the text
                if not price_history or not marketing_history:
                    # Look for patterns like: "priceHistory": [...]
                    simple_price_match = re.search(r'"priceHistory"\s*:\s*(\[.*?\])', text, re.DOTALL)
                    if simple_price_match and not price_history:
                        try:
                            price_history = json.loads(simple_price_match.group(1))
                            log(f"✅ Found price history array in JSON for {propertyrefno}")
                        except:
                            pass
                    
                    simple_marketing_match = re.search(r'"marketingHistory"\s*:\s*(\[.*?\])', text, re.DOTALL)
                    if simple_marketing_match and not marketing_history:
                        try:
                            marketing_history = json.loads(simple_marketing_match.group(1))
                            log(f"✅ Found marketing history array in JSON for {propertyrefno}")
                        except:
                            pass
                            
            except Exception as e:
                log(f"⚠️ Error extracting history from script tag: {e}", "WARN")

        # If JSON extraction failed, try HTML-based extraction
        if not price_history:
            # Look for price history section in HTML
            price_history_section = soup.find('div', {'data-testid': 'price-history'}) or \
                                   soup.find('section', string=re.compile('Price History', re.I)) or \
                                   soup.find('h2', string=re.compile('Price History', re.I))
            
            if price_history_section:
                # Find parent container
                container = price_history_section.find_parent() if price_history_section else None
                if container:
                    # Look for list items or table rows with price data
                    history_items = container.find_all(['li', 'tr', 'div'], class_=re.compile('history|price', re.I))
                    for item in history_items:
                        text = item.get_text(strip=True)
                        # Try to extract price and date
                        price_match = re.search(r'£?([\d,]+)', text)
                        date_match = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})|(\d{1,2}\s+\w+\s+\d{4})', text)
                        if price_match:
                            price_val = price_match.group(1).replace(',', '')
                            date_val = None
                            if date_match:
                                date_str = date_match.group(0)
                                try:
                                    if '/' in date_str:
                                        date_val = datetime.strptime(date_str, "%d/%m/%Y")
                                    elif '-' in date_str:
                                        date_val = datetime.strptime(date_str, "%d-%m-%Y")
                                    else:
                                        date_val = datetime.strptime(date_str, "%d %B %Y")
                                except:
                                    pass
                            if date_val:
                                price_history.append({
                                                'price': price_val,
                                                'date': created_date_iso
                                                })


        # Extract marketing history from HTML if not found in JSON
        if not marketing_history:
            marketing_section = soup.find('div', {'data-testid': 'marketing-history'}) or \
                               soup.find('section', string=re.compile('Marketing History', re.I)) or \
                               soup.find('h2', string=re.compile('Marketing History', re.I))
            
            if marketing_section:
                container = marketing_section.find_parent() if marketing_section else None
                if container:
                    history_items = container.find_all(['li', 'tr', 'div'], class_=re.compile('history|status|marketing', re.I))
                    for item in history_items:
                        text = item.get_text(strip=True)
                        # Try to extract status and date
                        status_match = re.search(r'(For Sale|Sold|Under Offer|Reduced|Price Changed|Withdrawn)', text, re.I)
                        date_match = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})|(\d{1,2}\s+\w+\s+\d{4})', text)
                        if status_match:
                            status_val = status_match.group(1)
                            date_val = None
                            if date_match:
                                date_str = date_match.group(0)
                                try:
                                    if '/' in date_str:
                                        date_val = datetime.strptime(date_str, "%d/%m/%Y")
                                    elif '-' in date_str:
                                        date_val = datetime.strptime(date_str, "%d-%m-%Y")
                                    else:
                                        date_val = datetime.strptime(date_str, "%d %B %Y")
                                except:
                                    pass
                            if date_val:
                                marketing_history.append({
                                    'status': status_val,
                                    'date': created_date_iso
                                })

        # Also check for "reduced" indicator and try to find previous price
        if not price_history:
            # Look for elements containing "reduced" text
            reduced_elements = soup.find_all(string=re.compile('reduced|price.*reduced', re.I))
            for reduced_text in reduced_elements:
                # Get the parent element
                parent = reduced_text.parent if hasattr(reduced_text, 'parent') else None
                if parent:
                    # Look for price in the parent or nearby elements
                    prev_price_text = parent.get_text()
                    # Also check siblings
                    if parent.parent:
                        prev_price_text += " " + parent.parent.get_text()
                    
                    prev_price_match = re.search(r'£?([\d,]+)', prev_price_text)
                    if prev_price_match:
                        prev_price = prev_price_match.group(1).replace(',', '')
                        # Try to find date
                        date_match = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})|(\d{1,2}\s+\w+\s+\d{4})', prev_price_text)
                        prev_date = created_date  # Default to created date if not found
                        if date_match:
                            date_str = date_match.group(0)
                            try:
                                if '/' in date_str:
                                    prev_date = datetime.strptime(date_str, "%d/%m/%Y")
                                elif '-' in date_str:
                                    prev_date = datetime.strptime(date_str, "%d-%m-%Y")
                                else:
                                    prev_date = datetime.strptime(date_str, "%d %B %Y")
                            except:
                                pass
                        # Add previous price to history if different from current
                        current_price_clean = final_price.replace('£', '').replace(',', '').strip()
                        if prev_price != current_price_clean and prev_price != "POA":
                            price_history.append({
                                'price': prev_price,
                                'date': prev_date
                            })
                            break  # Found one, no need to continue

        # ---------------------------------------------------------------
        # Normalize and sanitize price history
        # ---------------------------------------------------------------
        # 1) Normalize raw items (e.g. from simple_price_match) to {price, date}; support amount/changedDate
        if price_history and isinstance(price_history, list):
            normalized = []
            for item in price_history:
                if not isinstance(item, dict):
                    continue
                price_val = item.get('price') or item.get('Price') or item.get('amount') or item.get('value')
                if isinstance(price_val, dict):
                    price_val = price_val.get('amount') or price_val.get('value') or price_val.get('price')
                date_val = item.get('date') or item.get('Date') or item.get('changedDate') or item.get('timestamp')
                if not date_val:
                    continue
                if isinstance(date_val, str):
                    try:
                        for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y", "%d-%m-%Y"]:
                            try:
                                dt = datetime.strptime(date_val.split('T')[0].split()[0], fmt)
                                date_val = dt
                                break
                            except (ValueError, IndexError):
                                continue
                    except (ValueError, TypeError, IndexError):
                        pass
                if date_val and hasattr(date_val, 'strftime'):
                    date_str = date_val.strftime("%Y-%m-%d")
                else:
                    date_str = str(date_val)[:10] if date_val else None
                if date_str and price_val is not None:
                    normalized.append({'price': str(price_val).replace(',', ''), 'date': date_str})
            price_history = normalized
        # 2) Fix bogus prices: Rightmove sometimes returns day-of-month (e.g. 20) as "price"
        try:
            current_price_clean = (final_price.replace('£', '').replace(',', '').strip() if final_price else None) or None
            if current_price_clean and current_price_clean != 'POA' and price_history:
                for entry in price_history:
                    if isinstance(entry, dict) and entry.get('price'):
                        try:
                            p = int(str(entry['price']).replace(',', ''))
                            if 1 <= p <= 31 or p < 100:
                                entry['price'] = current_price_clean
                        except (ValueError, TypeError):
                            pass
        except Exception:
            pass

        # ---------------------------------------------------------------
        # Build Output
        # ---------------------------------------------------------------
        # ---------------------------------------------------------------
        if status:
            raw = status.strip().lower()

            if "removed" in raw or "withdrawn" in raw:
                # Keep as "Removed" - will be handled by appendindb based on previous status
                status = "Removed"
            elif "sold stc" in raw:
                status = "Sold STC"
            elif raw == "sold":
                status = "Sold"
            elif "under offer" in raw:
                status = "Under Offer"
            elif "Land" in raw:
                status = "Land"
            else:
                # Default formatting (capitalize each word)
                status = status.strip().title()
        output = {
            "propertytitle": soup.find('h1').get_text(strip=True) if soup.find('h1') else None,
            "propertylandtype": propertylandtype,
            "propertyisnewbuild": 1 if soup.find('span', {'class': 'premiumDarkGreen'}) else 0,
            "PropertyReference": propertyrefno,
            "PropertyURL": rightmoveurl,
            "PropertyAddress": property_address,
            "PropertyPostCode": property_postcode,
            "PropertyOutcode": outcode,
            "PropertyIncode": incode,
            "deliveryPointId": delivery_point_id,
            "encId": enc_id,
            "status": status,
            "Bedrooms": num_bedrooms,
            "Bathrooms": num_bathrooms,
            "Price": final_price,
            "Tenure": tenure_value,
            "PropertyCreatedDate": created_date_iso,
            "AgentName": agent_name_text,
            "AgentAddress": agent_address_text,
            "AgentPhoneNumber": agent_phone_text,
            "CouncilTaxBand": None,
            "PropertyFeatures": features,
            "PropertyDescription": description,
            "FloorplanImage": floorplan_url,
            "PropertyImage": image_url,
            "PropertyEpc1": epc1,
            "PropertyEpc2": epc2,
            "AgentProfileUrl": agent_profile_url,
            "Latitude": latitude,
            "Longitude": longitude,
            "AgentUnsubscribedFromEPCEmail": 0,
            "AgentImageURL": agent_image_url,
            "PriceHistory": price_history,
            "MarketingHistory": marketing_history,
            "soldPropertyTransactions": []  # Filled by caller via transactionHistory API when encId present
        }

        log(f"✅ Successfully scraped details for {propertyrefno}")
        if delivery_point_id is not None:
            print(f"[deliveryPointId] {delivery_point_id}")
        if enc_id is not None:
            print(f"[encId] {enc_id}")
        return output

    except Exception as e:
        log(f"❌ Error parsing property {url}: {e}", "ERROR")
        traceback.print_exc()
        return None


# --------------------------------------------------------------------
# --- SYNC PROPERTY DETAIL SCRAPER ------------------------------------
# --------------------------------------------------------------------
def scrape_property_details(url, headers=None, cookies=None):
    """Fetch property page and parse details (sync)."""
    log(f"🔎 Fetching property details: {url}")
    try:
        resp = requests.get(url, headers=headers, cookies=cookies, timeout=25)
        resp.raise_for_status()
    except Exception as e:
        log(f"❌ Failed to fetch property page: {url} | {e}", "ERROR")
        return None
    return parse_property_details_from_html(resp.text, url)


# --------------------------------------------------------------------
# --- LISTING SCRAPER FOR POSTCODE -----------------------------------
# --------------------------------------------------------------------
def fetch_properties_for_postcode(postcode, outcode):
    """
    Fetch and insert properties for a postcode.
    
    Returns:
        tuple: (records_inserted, seen_ref_ids) - Number of records inserted and set of seen property reference IDs
    """
    records_inserted = 0
    seen_ref_ids = set()  # Track all property reference IDs seen during scraping
    stats = {"listing_ok": 0, "listing_fail": 0, "property_ok": 0, "property_fail": 0, "total_listing_count": 0}

    try:
        headers = get_headers()
        cookies = get_cookies()

        base_url = f'https://www.rightmove.co.uk/property-for-sale/find.html?searchLocation={postcode}&locationIdentifier=OUTCODE%{outcode}&radius=0.0&_includeSSTC=on&includeSSTC=true&index=0&sortType=2&channel=BUY'
        logger = logging.getLogger("uvicorn.error")
        logger.info(f"🔗 BASE URL → {base_url}")

        try:
            response = requests.get(base_url, headers=headers, cookies=cookies, timeout=25)
            response.raise_for_status()
            stats["listing_ok"] += 1
            log(f"[REQUEST] listing page 1 -> {response.status_code} OK")
        except Exception as e:
            stats["listing_fail"] += 1
            log(f"[REQUEST] listing page 1 -> FAILED ({e})", "ERROR")
            raise
        soup = BeautifulSoup(response.text, "html.parser")

        page_info = soup.find("div", {"class": "Pagination_pageSelectContainer__zt0rg"})
        total_page_count = int(page_info.find_all('span')[-1].text.strip().replace("of", "").strip()) if page_info else 1
        log(f"📄 Total pages found: {total_page_count}")

        for index in range(0, total_page_count * 24, 24):
            page_url = f'https://www.rightmove.co.uk/property-for-sale/find.html?searchLocation={postcode}&locationIdentifier=OUTCODE%{outcode}&radius=0.0&_includeSSTC=on&includeSSTC=true&index={index}&sortType=2&channel=BUY'
            log(f"➡️ Scraping page {index // 24 + 1}: {page_url}")

            try:
                response = requests.get(page_url, headers=headers, cookies=cookies, timeout=25)
                response.raise_for_status()
                stats["listing_ok"] += 1
                log(f"[REQUEST] listing page {index // 24 + 1} -> {response.status_code} OK")
            except Exception as e:
                stats["listing_fail"] += 1
                log(f"[REQUEST] listing page {index // 24 + 1} -> FAILED ({e})", "ERROR")
                log(f"❌ Error fetching listing page {page_url}: {e}", "ERROR")
                traceback.print_exc()
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            detail_links = soup.find_all('a', {'data-testid': 'property-details-lozenge'})
            if not detail_links:
                log(f"⚠️ No listings found on page {index // 24 + 1}", "WARN")
                continue

            stats["total_listing_count"] += len(detail_links)
            for link in detail_links:
                detail_url = "https://www.rightmove.co.uk" + link.get('href')
                listing = scrape_property_details(detail_url, headers=headers, cookies=cookies)
                if listing:
                    stats["property_ok"] += 1
                    if listing.get("encId"):
                        log(f"📋 Fetching transaction history for encId (property {listing.get('PropertyReference', '?')})")
                        listing["soldPropertyTransactions"] = fetch_sold_property_transactions(
                            listing["encId"], headers=headers, cookies=cookies, referer_url=detail_url
                        )
                    elif not listing.get("encId"):
                        log(f"⚠️ No encId for property {listing.get('PropertyReference', '?')} - skipping transaction history", "WARN")
                    log_data(listing)
                    property_ref = listing.get('PropertyReference')
                    if property_ref:
                        seen_ref_ids.add(property_ref)
                    try:
                        result = appendingintodb(postcode, listing)
                        if result:
                            records_inserted += 1
                            log(f"💾 Saved to database: {listing['PropertyReference']}")
                    except Exception as e:
                        log(f"❌ DB Insert failed for {detail_url}: {e}", "ERROR")
                        traceback.print_exc()
                else:
                    stats["property_fail"] += 1
                    log(f"[REQUEST] property -> FAILED (fetch/parse)", "ERROR")

        log(f"[REQUEST STATS] Listing: {stats['listing_ok']} ok, {stats['listing_fail']} failed | Property: {stats['property_ok']} ok, {stats['property_fail']} failed")
        return records_inserted, seen_ref_ids, stats.get("total_listing_count", 0)

    except Exception as e:
        log(f"❌ Critical error fetching properties for postcode {postcode}: {e}", "ERROR")
        traceback.print_exc()
        log(f"[REQUEST STATS] Listing: {stats['listing_ok']} ok, {stats['listing_fail']} failed | Property: {stats['property_ok']} ok, {stats['property_fail']} failed")
        return records_inserted, seen_ref_ids, stats.get("total_listing_count", 0)


# --------------------------------------------------------------------
# --- ASYNC HELPERS --------------------------------------------------
# --------------------------------------------------------------------
def _cookie_header(cookies_dict):
    """Build Cookie header string from dict."""
    if not cookies_dict:
        return ""
    return "; ".join(f"{k}={v}" for k, v in cookies_dict.items())


async def _fetch_html_async(session, url, headers, cookies_dict, timeout_sec=25):
    """
    Fetch URL with aiohttp.
    Returns: (html_text, status_code, error_message).
    On success: (text, status_code, None). On failure: (None, None, str(error)).
    """
    try:
        h = dict(headers)
        cookie = _cookie_header(cookies_dict)
        if cookie:
            h["Cookie"] = cookie
        timeout = aiohttp.ClientTimeout(total=timeout_sec, connect=15)
        async with session.get(url, headers=h, timeout=timeout) as resp:
            resp.raise_for_status()
            text = await resp.text()
            return (text, resp.status, None)
    except aiohttp.ClientResponseError as e:
        log(f"❌ Failed to fetch {url} | HTTP {e.status} | {e.message}", "ERROR")
        return (None, getattr(e, "status", None), str(e))
    except Exception as e:
        log(f"❌ Failed to fetch {url} | {e}", "ERROR")
        return (None, None, str(e))


def _parse_and_save_sync(html, detail_url, postcode, headers=None, cookies=None):
    """Run CPU-bound parse, fetch transaction history if encId present, and DB write in thread."""
    listing = parse_property_details_from_html(html, detail_url)
    if not listing:
        return (0, None, True)
    if listing.get("encId") and headers and cookies:
        log(f"📋 Fetching transaction history for encId (property {listing.get('PropertyReference', '?')})")
        listing["soldPropertyTransactions"] = fetch_sold_property_transactions(
            listing["encId"], headers=headers, cookies=cookies, referer_url=detail_url
        )
    elif listing and not listing.get("encId"):
        log(f"⚠️ No encId for property {listing.get('PropertyReference', '?')} - skipping transaction history", "WARN")
    log_data(listing)
    property_ref = listing.get("PropertyReference")
    try:
        result = appendingintodb(postcode, listing)
        if result:
            log(f"💾 Saved to database: {listing['PropertyReference']}")
            return (1, property_ref, True)
    except Exception as e:
        log(f"❌ DB Insert failed for {detail_url}: {e}", "ERROR")
        traceback.print_exc()
    return (0, property_ref, True)  # Still return ref so it's in seen_ref_ids (property exists on Rightmove)


async def _fetch_one_property_async(session, sem, detail_url, headers, cookies, postcode):
    """
    Fetch one property page, parse (in thread), and insert (in thread).
    Returns: (records_inserted, property_ref or None, request_ok, status_code, error_msg).
    """
    async with sem:
        html, status_code, error_msg = await _fetch_html_async(
            session, detail_url, headers, cookies, timeout_sec=30
        )
    if not html:
        return (0, None, False, status_code, error_msg)
    # Run parse + transaction fetch + DB in thread pool so event loop can do more fetches
    loop = asyncio.get_event_loop()
    inserted, property_ref, request_ok = await loop.run_in_executor(
        None, _parse_and_save_sync, html, detail_url, postcode, headers, cookies
    )
    return (inserted, property_ref, request_ok, status_code, None)


# --------------------------------------------------------------------
# --- ASYNC LISTING SCRAPER ------------------------------------------
# --------------------------------------------------------------------
async def fetch_properties_for_postcode_async(
    postcode,
    outcode,
    concurrency=24,
    page_concurrency=8,
):
    """
    Fetch and insert properties for a postcode using concurrent requests.
    Uses aiohttp for listing and property-detail pages; same DB/parse logic as sync.
    Returns:
        tuple: (records_inserted, seen_ref_ids)
    """
    records_inserted = 0
    seen_ref_ids = set()
    headers = get_headers()
    cookies = get_cookies()

    # Request status tracking
    stats = {"listing_ok": 0, "listing_fail": 0, "property_ok": 0, "property_fail": 0}

    base_url = (
        f"https://www.rightmove.co.uk/property-for-sale/find.html?"
        f"searchLocation={postcode}&locationIdentifier=OUTCODE%{outcode}&radius=0.0"
        f"&_includeSSTC=on&includeSSTC=true&index=0&sortType=2&channel=BUY"
    )
    logger = logging.getLogger("uvicorn.error")
    logger.info(f"🔗 BASE URL → {base_url}")

    timeout = aiohttp.ClientTimeout(total=35, connect=15)
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(
        ssl=ssl_context,
        limit=min(100, concurrency + 30),
        limit_per_host=min(50, concurrency + 10),
    )

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # 1) First page to get total page count
        first_html, first_status, first_err = await _fetch_html_async(
            session, base_url, headers, cookies, timeout_sec=30
        )
        if first_html:
            stats["listing_ok"] += 1
            log(f"[REQUEST] listing page 1 -> {first_status} OK")
        else:
            stats["listing_fail"] += 1
            log(f"[REQUEST] listing page 1 -> FAILED ({first_err or 'unknown'})", "ERROR")
            log(f"❌ Failed to fetch first page for {postcode}", "ERROR")
            log(f"[REQUEST STATS] Listing: {stats['listing_ok']} ok, {stats['listing_fail']} failed | Property: {stats['property_ok']} ok, {stats['property_fail']} failed")
            return records_inserted, seen_ref_ids

        soup = BeautifulSoup(first_html, "html.parser")
        page_info = soup.find("div", {"class": "Pagination_pageSelectContainer__zt0rg"})
        try:
            total_page_count = int(
                page_info.find_all("span")[-1].text.strip().replace("of", "").strip()
            )
        except (AttributeError, ValueError, IndexError):
            total_page_count = 1
        log(f"📄 Total pages found: {total_page_count}")

        # 2) Build listing page URLs for pages 2..N (page 1 already in first_html); fetch concurrently
        page_urls = []
        for index in range(24, total_page_count * 24, 24):
            page_url = (
                f"https://www.rightmove.co.uk/property-for-sale/find.html?"
                f"searchLocation={postcode}&locationIdentifier=OUTCODE%{outcode}&radius=0.0"
                f"&_includeSSTC=on&includeSSTC=true&index={index}&sortType=2&channel=BUY"
            )
            page_urls.append(page_url)

        page_sem = asyncio.Semaphore(min(page_concurrency, 4))  # Limit to 4 concurrent listing pages to avoid 503
        async def fetch_page(idx_url):
            idx, url = idx_url
            await asyncio.sleep(idx * 0.3)  # Stagger requests to avoid rate limiting
            async with page_sem:
                html, status, err = await _fetch_html_async(session, url, headers, cookies, timeout_sec=30)
                return (html, status, err, idx + 2)  # page number (1-based, page 1 already done)

        other_page_htmls = await asyncio.gather(
            *[fetch_page((i, u)) for i, u in enumerate(page_urls)],
            return_exceptions=True
        ) if page_urls else []

        failed_page_urls = []
        for item in other_page_htmls:
            if isinstance(item, Exception):
                stats["listing_fail"] += 1
                log(f"[REQUEST] listing page ? -> FAILED ({item})", "ERROR")
                continue
            html, status, err, page_num = item
            if html:
                stats["listing_ok"] += 1
                log(f"[REQUEST] listing page {page_num} -> {status} OK")
            else:
                stats["listing_fail"] += 1
                log(f"[REQUEST] listing page {page_num} -> FAILED ({err or 'unknown'})", "ERROR")
                # Track failed page URL for retry
                page_idx = page_num - 2  # Convert back to 0-based index
                if 0 <= page_idx < len(page_urls):
                    failed_page_urls.append(page_urls[page_idx])

        # 2b) Retry failed listing pages sequentially (with delay to avoid rate limiting)
        retry_page_htmls = []
        if failed_page_urls:
            log(f"🔄 Retrying {len(failed_page_urls)} failed listing pages...")
            for url in failed_page_urls:
                await asyncio.sleep(2)  # Delay to avoid rate limiting
                html, status, err = await _fetch_html_async(session, url, headers, cookies, timeout_sec=30)
                if html:
                    stats["listing_ok"] += 1
                    stats["listing_fail"] -= 1
                    log(f"[REQUEST] listing page retry -> {status} OK")
                    retry_page_htmls.append(html)
                else:
                    log(f"[REQUEST] listing page retry -> FAILED again ({err or 'unknown'})", "ERROR")

        # 3) Collect detail URLs from all pages (in order)
        ordered_htmls = [first_html] + [
            (item[0] if not isinstance(item, Exception) else None)
            for item in other_page_htmls
        ] + retry_page_htmls
        detail_urls = []
        for i, html in enumerate(ordered_htmls):
            if not html:
                continue
            soup = BeautifulSoup(html, "html.parser")
            detail_links = soup.find_all("a", {"data-testid": "property-details-lozenge"})
            for link in detail_links:
                href = link.get("href")
                if href:
                    detail_urls.append("https://www.rightmove.co.uk" + href)
            if detail_links:
                log(f"➡️ Page {i + 1}: {len(detail_links)} listings")

        # Use expected total from pagination as the baseline for safety checks
        # This accounts for properties on listing pages that failed to load
        expected_total = total_page_count * 24
        total_listing_count = max(len(detail_urls), expected_total)

        if not detail_urls:
            log("⚠️ No listing links found on any page", "WARN")
            log(f"[REQUEST STATS] Listing: {stats['listing_ok']} ok, {stats['listing_fail']} failed | Property: {stats['property_ok']} ok, {stats['property_fail']} failed")
            return records_inserted, seen_ref_ids, 0

        log(f"🔎 Fetching {len(detail_urls)} property details (concurrency={concurrency}) [expected ~{expected_total} from {total_page_count} pages]")

        # 4) Fetch property details concurrently with semaphore
        sem = asyncio.Semaphore(concurrency)
        tasks = [
            _fetch_one_property_async(session, sem, url, headers, cookies, postcode)
            for url in detail_urls
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        failed_urls = []
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                stats["property_fail"] += 1
                log(f"[REQUEST] property -> FAILED (task error: {r})", "ERROR")
                failed_urls.append(detail_urls[i])
                continue
            inserted, ref, request_ok, status_code, error_msg = r
            if request_ok:
                stats["property_ok"] += 1
            else:
                stats["property_fail"] += 1
                log(f"[REQUEST] property -> FAILED ({error_msg or 'unknown'})", "ERROR")
                failed_urls.append(detail_urls[i])
            records_inserted += inserted
            if ref:
                seen_ref_ids.add(ref)

        # 5) Retry failed property fetches once
        if failed_urls:
            log(f"🔄 Retrying {len(failed_urls)} failed property fetches...")
            retry_tasks = [
                _fetch_one_property_async(session, sem, url, headers, cookies, postcode)
                for url in failed_urls
            ]
            retry_results = await asyncio.gather(*retry_tasks, return_exceptions=True)
            for r in retry_results:
                if isinstance(r, Exception):
                    continue
                inserted, ref, request_ok, status_code, error_msg = r
                if request_ok:
                    stats["property_ok"] += 1
                    stats["property_fail"] -= 1
                records_inserted += inserted
                if ref:
                    seen_ref_ids.add(ref)

    log(f"[REQUEST STATS] Listing: {stats['listing_ok']} ok, {stats['listing_fail']} failed | Property: {stats['property_ok']} ok, {stats['property_fail']} failed")
    return records_inserted, seen_ref_ids, total_listing_count
