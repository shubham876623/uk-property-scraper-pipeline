import csv
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
csv_file = os.path.join(CURRENT_DIR, "sg1_rating_and_urls.csv")

def safe_print(msg: str):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode("ascii", errors="ignore").decode())

def analyze_csv():
    """Analyze the CSV file to count missing data."""
    safe_print("Analyzing sg1_rating_and_urls.csv...")
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data = list(reader)
    
    total = len(data)
    
    # Count complete records (all 3 columns have valid data)
    complete = 0
    missing_all = 0
    missing_rating = 0
    missing_current_score = 0
    missing_potential_score = 0
    missing_rating_only = 0
    missing_current_only = 0
    missing_potential_only = 0
    missing_two = 0
    invalid_url = 0
    valid_url_but_missing_data = 0
    
    for row in data:
        rating = row.get('Rating', '').strip()
        current_score = row.get('CurrentScore', '').strip()
        potential_score = row.get('PotentialScore', '').strip()
        image_url = row.get('ImageURL', '').strip()
        
        # Check validity
        rating_valid = rating and rating not in ['0', '', 'None']
        current_score_valid = current_score and current_score not in ['', '0', 'None']
        potential_score_valid = potential_score and potential_score not in ['', '0', 'None']
        
        # Count invalid URLs
        if not image_url or image_url == 'None' or '.pdf' in image_url.lower():
            invalid_url += 1
        
        # Count complete records
        if rating_valid and current_score_valid and potential_score_valid:
            complete += 1
        else:
            # Count missing combinations
            missing_count = sum([not rating_valid, not current_score_valid, not potential_score_valid])
            
            if missing_count == 3:
                missing_all += 1
                # Check if has valid URL but missing data
                if image_url and image_url != 'None' and '.pdf' not in image_url.lower():
                    valid_url_but_missing_data += 1
            elif missing_count == 2:
                missing_two += 1
            elif missing_count == 1:
                if not rating_valid:
                    missing_rating_only += 1
                elif not current_score_valid:
                    missing_current_only += 1
                elif not potential_score_valid:
                    missing_potential_only += 1
            
            # Count individual missing fields
            if not rating_valid:
                missing_rating += 1
            if not current_score_valid:
                missing_current_score += 1
            if not potential_score_valid:
                missing_potential_score += 1
    
    # Print summary
    safe_print("\n" + "=" * 80)
    safe_print("CSV ANALYSIS SUMMARY")
    safe_print("=" * 80)
    safe_print(f"Total records: {total}")
    safe_print(f"\n✅ Complete records (all 3 columns): {complete} ({complete/total*100:.1f}%)")
    safe_print(f"\n❌ Missing Data:")
    safe_print(f"   Missing all three: {missing_all}")
    safe_print(f"   Missing two fields: {missing_two}")
    safe_print(f"   Missing only Rating: {missing_rating_only}")
    safe_print(f"   Missing only CurrentScore: {missing_current_only}")
    safe_print(f"   Missing only PotentialScore: {missing_potential_only}")
    safe_print(f"\n📊 Individual Field Missing Counts:")
    safe_print(f"   Missing Rating: {missing_rating}")
    safe_print(f"   Missing CurrentScore: {missing_current_score}")
    safe_print(f"   Missing PotentialScore: {missing_potential_score}")
    safe_print(f"\n🔗 URL Status:")
    safe_print(f"   Invalid/PDF/NULL URLs: {invalid_url}")
    safe_print(f"   Valid URLs but missing data: {valid_url_but_missing_data}")
    safe_print("=" * 80)
    
    # Show examples of records that need scraping
    safe_print("\n📋 Examples of records with valid URLs but missing data:")
    safe_print("-" * 80)
    examples_shown = 0
    for row in data:
        if examples_shown >= 10:
            break
        
        rating = row.get('Rating', '').strip()
        current_score = row.get('CurrentScore', '').strip()
        potential_score = row.get('PotentialScore', '').strip()
        image_url = row.get('ImageURL', '').strip()
        
        rating_valid = rating and rating not in ['0', '', 'None']
        current_score_valid = current_score and current_score not in ['', '0', 'None']
        potential_score_valid = potential_score and potential_score not in ['', '0', 'None']
        
        # Show records with valid URL but missing data
        if (not rating_valid or not current_score_valid or not potential_score_valid):
            if image_url and image_url != 'None' and '.pdf' not in image_url.lower():
                missing_fields = []
                if not rating_valid:
                    missing_fields.append("Rating")
                if not current_score_valid:
                    missing_fields.append("CurrentScore")
                if not potential_score_valid:
                    missing_fields.append("PotentialScore")
                
                safe_print(f"PropertyId {row.get('PropertyId')}: Missing {', '.join(missing_fields)}")
                safe_print(f"  URL: {image_url[:80]}...")
                examples_shown += 1
    
    safe_print("\n")

if __name__ == "__main__":
    analyze_csv()

