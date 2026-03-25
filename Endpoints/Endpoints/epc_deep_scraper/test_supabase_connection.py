import os
import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "EPCCertificateDeepScrape")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

print("✅ Supabase Connection Test")
print(f"📦 Table: {SUPABASE_TABLE}")
print(f"🌐 URL: {SUPABASE_URL}\n")

try:
    # Fetch 5 records to confirm connectivity
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?select=*&limit=5"
    response = requests.get(url, headers=HEADERS, timeout=30)

    if response.status_code == 200:
        data = response.json()
        print(f"✅ Connected successfully! {len(data)} record(s) retrieved.\n")

        if data:
            print("🔹 Sample Records:")
            for row in data[:3]:
                print(f"  - URN: {row.get('URN')}, Postcode: {row.get('Postcode')}, Rating: {row.get('EnergyRating')}")
        else:
            print("⚠️ Table is empty but connection works fine.")
    else:
        print(f"❌ Failed to connect. Status: {response.status_code}\n{response.text}")

except requests.exceptions.RequestException as e:
    print(f"❌ Connection error: {e}")
