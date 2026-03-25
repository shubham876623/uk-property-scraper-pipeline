import os
import requests
from dotenv import load_dotenv

# --- Load environment variables ---
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = "ExtractedProperties"

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}"
}

# --- Function to check EPC records count for a postcode in EPCCertificateDeepScrape table ---
def check_epc_count_by_postcode(postcode):
    """Check how many EPC records exist in EPCCertificateDeepScrape table for a given postcode."""
    DEEP_SCRAPE_TABLE = "EPCCertificateDeepScrape"
    
    # URL encode the postcode (spaces become %20)
    encoded_postcode = postcode.replace(' ', '%20')
    
    # First, get the count using Prefer: count=exact header
    count_url = f"{SUPABASE_URL}/rest/v1/{DEEP_SCRAPE_TABLE}?postCode=eq.{encoded_postcode}&select=id"
    count_headers = {
        **headers,
        "Prefer": "count=exact"
    }
    
    try:
        r = requests.get(count_url, headers=count_headers)
        if r.status_code == 200:
            # Extract count from Content-Range header
            content_range = r.headers.get('Content-Range', '0-0/0')
            total_count = int(content_range.split('/')[-1])
        else:
            print(f"⚠️ Warning: Could not get count (Status {r.status_code}), trying to count from data...")
            total_count = 0
    except Exception as e:
        print(f"⚠️ Warning: Error getting count: {e}")
        total_count = 0
    
    print(f"\n🔍 Checking EPC records for postcode: {postcode}")
    print(f"📊 Total records in EPCCertificateDeepScrape table: {total_count}")
    
    # Now get the actual data (first 50 records)
    data_url = f"{SUPABASE_URL}/rest/v1/{DEEP_SCRAPE_TABLE}?postCode=eq.{encoded_postcode}&select=id,address,postCode,rating,currentScore,potentialScore,url&limit=50&order=id.desc"
    r = requests.get(data_url, headers=headers)
    
    if r.status_code == 200:
        data = r.json()
        print(f"\n📋 Showing {len(data)} record(s) (showing up to 50):")
        print("-" * 100)
        
        if len(data) > 0:
            for idx, record in enumerate(data, 1):
                print(f"\n{idx}. EPC ID: {record.get('id', 'N/A')}")
                print(f"   Address: {record.get('address', 'N/A')}")
                print(f"   Postcode: {record.get('postCode', 'N/A')}")
                print(f"   Rating: {record.get('rating', 'N/A')}")
                print(f"   Current Score: {record.get('currentScore', 'N/A')}")
                print(f"   Potential Score: {record.get('potentialScore', 'N/A')}")
                print(f"   URL: {record.get('url', 'N/A')}")
        else:
            print("⚠️ No records found for this postcode.")
    else:
        print(f"❌ Error fetching data: Status {r.status_code}")
        print(f"Response: {r.text}")
    
    return total_count

# --- Function to check a specific PropertyId ---
def check_single_property(property_id):
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?PropertyId=eq.{property_id}&select=PropertyId,Rating,CurrentScore,PotentialScore"
    r = requests.get(url, headers=headers)
    print(f"\n🔍 Checking PropertyId {property_id}")
    print("Response code:", r.status_code)
    print("Result:", r.json())

# --- Function to check all updated properties ---
def check_all_updated():
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?select=PropertyId,Rating,CurrentScore,PotentialScore&Rating=not.is.null"
    r = requests.get(url, headers=headers)
    data = r.json()

    print(f"\n📊 Total properties with EPC Rating updated: {len(data)}")

    if len(data) > 0:
        print("\n🔹 Preview of first few updated records:")
        for row in data[:10]:
            print(f"PropertyId={row['PropertyId']}, Rating={row.get('Rating')}, "
                  f"CurrentScore={row.get('CurrentScore')}, PotentialScore={row.get('PotentialScore')}")
    else:
        print("⚠️ No records found with updated Rating field.")

# --- Main Execution ---
if __name__ == "__main__":
    print("✅ Supabase EPC Database Check")
    print(f"📦 Table: {SUPABASE_TABLE}")
    print(f"🌐 URL: {SUPABASE_URL}\n")

    # 1️⃣ Check EPC records count for a postcode (e.g., EX1 3SL)
    check_epc_count_by_postcode("EX1 3SL")  # ← Change postcode here
    
    # 2️⃣ Check a single property (optional)
    # check_single_property(134255042)   # ← change ID if you want

    # 3️⃣ Check all updated records
    # check_all_updated()
