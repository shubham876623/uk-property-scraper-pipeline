"""
Fix 769 records where PropertyPrice = PropertyReferenceNumber (data corruption).
Re-fetches the actual price from Rightmove for each corrupted record.
"""
import os
import sys
import re
import time
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Setup paths
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, CURRENT_DIR)

from db.db import get_connection, run_query, run_insert, SUPABASE_URL, HEADERS as DB_HEADERS
from src.headers import get_headers

load_dotenv(os.path.join(CURRENT_DIR, "..", ".env"))


def get_corrupted_records():
    """Find records where PropertyPrice = PropertyReferenceNumber."""
    url = f"{SUPABASE_URL}/rest/v1/ExtractedProperties?select=PropertyReferanceNumber,PropertyPrice"
    resp = requests.get(url, headers=DB_HEADERS, timeout=30)
    if resp.status_code != 200:
        print(f"Error fetching records: {resp.status_code}")
        return []

    all_records = resp.json()
    corrupted = []
    for r in all_records:
        ref = str(r.get("PropertyReferanceNumber", "")).strip()
        price = str(r.get("PropertyPrice", "")).strip()
        price_digits = re.sub(r'[^\d]', '', price)
        if ref and price_digits and price_digits == ref:
            corrupted.append(ref)

    return corrupted


def fetch_price_from_rightmove(property_ref):
    """Fetch actual price from Rightmove property page."""
    url = f"https://www.rightmove.co.uk/properties/{property_ref}"
    try:
        resp = requests.get(url, headers=get_headers(), timeout=20)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Try multiple selectors
        for selector in [
            '._1gfnqJ3Vtd1z40MlC0MzXu span',
            '[data-testid="price"] span',
            'p._1hV1kqpVceE9SQ3horOEYN span',
        ]:
            tag = soup.select_one(selector)
            if tag:
                text = tag.get_text(strip=True)
                if text and '£' in text:
                    price = text.replace('£', '').replace(',', '').strip()
                    # Sanity check
                    price_digits = re.sub(r'[^\d]', '', price)
                    if price_digits != property_ref:
                        return price

        # Last resort
        for tag in soup.find_all(string=lambda t: t and '£' in t and any(c.isdigit() for c in t)):
            candidate = tag.strip()
            if candidate.startswith('£') and len(candidate) < 20:
                price = candidate.replace('£', '').replace(',', '').strip()
                price_digits = re.sub(r'[^\d]', '', price)
                if price_digits != property_ref:
                    return price

        return None
    except Exception as e:
        print(f"  Error fetching {property_ref}: {e}")
        return None


def fix_price(property_ref, new_price):
    """Update the price in the database."""
    conn = get_connection()
    run_insert(
        conn,
        "UPDATE ExtractedProperties SET PropertyPrice = ? WHERE PropertyReferanceNumber = ?",
        [new_price, property_ref],
    )


def main():
    print("Finding corrupted records (PropertyPrice = PropertyReferenceNumber)...")
    corrupted = get_corrupted_records()
    print(f"Found {len(corrupted)} corrupted records\n")

    fixed = 0
    failed = 0

    for i, ref in enumerate(corrupted):
        print(f"[{i+1}/{len(corrupted)}] Fixing {ref}...", end=" ")
        price = fetch_price_from_rightmove(ref)

        if price:
            fix_price(ref, price)
            print(f"✅ Updated to £{price}")
            fixed += 1
        else:
            print("❌ Could not fetch price (property may be removed)")
            failed += 1

        # Rate limit to avoid 429s
        time.sleep(0.5)

    print(f"\nDone! Fixed: {fixed}, Failed: {failed}")


if __name__ == "__main__":
    main()
