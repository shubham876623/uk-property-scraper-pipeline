import os
import sys
import requests
import csv
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add project root to path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ENDPOINTS_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if ENDPOINTS_ROOT not in sys.path:
    sys.path.insert(0, ENDPOINTS_ROOT)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = "ExtractedProperties"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

def safe_print(msg: str):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode("ascii", errors="ignore").decode())

def get_all_sg1_records():
    """Fetch all records for PropertyOutcode=SG1 from Supabase with pagination."""
    safe_print("Fetching all records for PropertyOutcode=SG1...")
    
    all_data = []
    page_size = 1000  # Supabase default limit
    offset = 0
    
    while True:
        # Query with pagination - use limit and offset
        url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?PropertyOutcode=eq.SG1&select=PropertyId,PropertyEPC,Rating,CurrentScore,PotentialScore,PropertyOutcode,AlreadyScrapedEPC&limit={page_size}&offset={offset}"
        
        try:
            response = requests.get(url, headers=HEADERS, timeout=60)
            if response.status_code != 200:
                safe_print(f"Error fetching records: {response.text}")
                break
            
            data = response.json()
            
            if not data:
                break  # No more records
            
            all_data.extend(data)
            safe_print(f"Fetched {len(data)} records (offset {offset}, total so far: {len(all_data)})")
            
            # If we got fewer than page_size, we've reached the end
            if len(data) < page_size:
                break
            
            offset += page_size
            
        except Exception as e:
            safe_print(f"Exception during fetch: {e}")
            import traceback
            traceback.print_exc()
            break
    
    safe_print(f"\n✅ Found {len(all_data)} total records for SG1\n")
    
    # Count records by status
    all_complete = 0
    missing_rating = 0
    missing_current_score = 0
    missing_potential_score = 0
    missing_all = 0
    invalid_url = 0
    
    for item in all_data:
        rating = item.get("Rating")
        current_score = item.get("CurrentScore")
        potential_score = item.get("PotentialScore")
        image_url = item.get("PropertyEPC")
        
        # Check if all three are present
        rating_valid = rating and rating not in ["0", 0, None, "Not Available", ""]
        current_score_valid = current_score is not None and current_score != "" and current_score != 0
        potential_score_valid = potential_score is not None and potential_score != "" and potential_score != 0
        
        if rating_valid and current_score_valid and potential_score_valid:
            all_complete += 1
        elif not rating_valid and not current_score_valid and not potential_score_valid:
            missing_all += 1
        else:
            if not rating_valid:
                missing_rating += 1
            if not current_score_valid:
                missing_current_score += 1
            if not potential_score_valid:
                missing_potential_score += 1
        
        if not image_url or "None" in str(image_url) or ".pdf" in str(image_url):
            invalid_url += 1
    
    # Print summary
    safe_print("=" * 80)
    safe_print("SUMMARY FOR SG1")
    safe_print("=" * 80)
    safe_print(f"Total records: {len(all_data)}")
    safe_print(f"✅ Complete (all 3 columns): {all_complete}")
    safe_print(f"❌ Missing Rating: {missing_rating}")
    safe_print(f"❌ Missing CurrentScore: {missing_current_score}")
    safe_print(f"❌ Missing PotentialScore: {missing_potential_score}")
    safe_print(f"❌ Missing all three: {missing_all}")
    safe_print(f"⚠️  Invalid/PDF URLs: {invalid_url}")
    safe_print("=" * 80)
    
    # Export to CSV
    csv_filename = "sg1_rating_and_urls.csv"
    csv_path = os.path.join(CURRENT_DIR, csv_filename)
    
    safe_print(f"\n💾 Exporting to CSV: {csv_path}")
    
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['PropertyId', 'Rating', 'CurrentScore', 'PotentialScore', 'ImageURL', 'PropertyOutcode']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for item in all_data:
            writer.writerow({
                'PropertyId': item.get('PropertyId'),
                'Rating': item.get('Rating') or '',
                'CurrentScore': item.get('CurrentScore') or '',
                'PotentialScore': item.get('PotentialScore') or '',
                'ImageURL': item.get('PropertyEPC') or '',
                'PropertyOutcode': item.get('PropertyOutcode') or ''
            })
    
    safe_print(f"✅ CSV file created: {csv_path}")
    safe_print(f"   Total rows: {len(all_data)}")
    
    # Show some examples of incomplete records
    safe_print("\n📋 Examples of incomplete records:")
    safe_print("-" * 80)
    incomplete_count = 0
    for item in all_data:
        if incomplete_count >= 10:  # Show first 10 incomplete
            break
        
        rating = item.get("Rating")
        current_score = item.get("CurrentScore")
        potential_score = item.get("PotentialScore")
        image_url = item.get("PropertyEPC")
        
        rating_valid = rating and rating not in ["0", 0, None, "Not Available", ""]
        current_score_valid = current_score is not None and current_score != "" and current_score != 0
        potential_score_valid = potential_score is not None and potential_score != "" and potential_score != 0
        
        if not (rating_valid and current_score_valid and potential_score_valid):
            missing_fields = []
            if not rating_valid:
                missing_fields.append("Rating")
            if not current_score_valid:
                missing_fields.append("CurrentScore")
            if not potential_score_valid:
                missing_fields.append("PotentialScore")
            
            safe_print(f"PropertyId {item.get('PropertyId')}: Missing {', '.join(missing_fields)}")
            safe_print(f"  Rating={rating}, CurrentScore={current_score}, PotentialScore={potential_score}")
            safe_print(f"  ImageURL={'NULL' if not image_url else image_url[:80]}")
            safe_print("")
            incomplete_count += 1
    
    return all_data

if __name__ == "__main__":
    records = get_all_sg1_records()
