import os
import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "") + "/rest/v1/SQLEPCCertificateSimpleScrape"
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")  # 👈 use service_role key here

def fetch_all_records():
    if not SUPABASE_SERVICE_KEY:
        print("❌ Missing SUPABASE_SERVICE_KEY in .env file")
        return

    headers = {
        "apikey": SUPABASE_SERVICE_KEY,                # required by Supabase REST
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",  # also required
        "Accept": "application/json"
    }

    params = {
        "select": "*"   # fetch all columns
    }

    print("📡 Fetching records from Supabase...")

    response = requests.get(SUPABASE_URL, headers=headers, params=params, timeout=60)

    if response.status_code == 200:
        data = response.json()
        print(f"✅ Total Records: {len(data)}")
        for record in data[:10]:  # show first 10 only
            print(record)
        if len(data) > 10:
            print(f"...and {len(data)-10} more records.")
    else:
        print(f"❌ Failed ({response.status_code}): {response.text}")

if __name__ == "__main__":
    fetch_all_records()
