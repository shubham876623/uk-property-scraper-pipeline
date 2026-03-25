import os
import requests
from dotenv import load_dotenv
from collections import Counter

# === Load environment variables ===
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "EPCCertificateSimpleScrape")

# === Headers ===
headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

def fetch_all_records():
    """
    Fetches all rows from the Supabase table (paginated if needed)
    and returns them as a list of dictionaries.
    """
    print(f"🌐 Fetching all records from {SUPABASE_TABLE} ...\n")
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?select=Postcode,URN"
    all_data = []
    range_start = 0
    page_size = 1000  # Supabase default limit per request

    while True:
        range_end = range_start + page_size - 1
        paginated_headers = {**headers, "Range": f"{range_start}-{range_end}"}

        response = requests.get(url, headers=paginated_headers, timeout=60)
        if response.status_code != 200:
            print(f"[ERROR] Failed to fetch records: {response.status_code} - {response.text}")
            break

        data = response.json()
        if not data:
            break

        all_data.extend(data)
        print(f"   → Fetched {len(data)} rows (Total so far: {len(all_data)})")

        # Stop when we receive less than one full page
        if len(data) < page_size:
            break

        range_start += page_size

    return all_data


def main():
    try:
        data = fetch_all_records()
        total_count = len(data)
        print(f"\n✅ Total records in {SUPABASE_TABLE}: {total_count}\n")

        # Optional: group by postcode
        if data:
            postcodes = [row.get("Postcode", "").strip() for row in data if row.get("Postcode")]
            counts = Counter(postcodes)

            print("📊 Record count per postcode (Top 20):")
            for pc, count in counts.most_common(20):
                print(f"   {pc:<10} → {count}")

    except Exception as e:
        print(f"[ERROR] {e}")


if __name__ == "__main__":
    main()
