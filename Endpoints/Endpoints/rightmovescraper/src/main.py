# rightmovescraper/src/main.py
import os
import sys
from dotenv import load_dotenv
# Add once at the top of main.py
import sys
sys.stdout.reconfigure(encoding='utf-8')

# Dynamically fix import path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CURRENT_DIR)
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from scraper import run_scraper  # ✅ Now works both ways

def main():
    load_dotenv()

    # Clear log files at the start of each run
    logs_dir = os.path.join(ROOT_DIR, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    scraped_output_log = os.path.join(logs_dir, "scraped_output.log")
    supabase_db_log = os.path.join(logs_dir, "supabase_db.log")
    
    # Also clear the main rightmove_scraper.log in root logs directory
    # This is the log file created by trigger_scraper in app.py
    # Path: rightmovescraper/src/main.py -> rightmovescraper -> Endpoints -> logs/rightmove_scraper.log
    workspace_root = os.path.dirname(ROOT_DIR)  # Go up from rightmovescraper to Endpoints
    root_logs_dir = os.path.join(workspace_root, "logs")
    os.makedirs(root_logs_dir, exist_ok=True)
    rightmove_scraper_log = os.path.join(root_logs_dir, "rightmove_scraper.log")
    
    # Clear log files
    try:
        if os.path.exists(scraped_output_log):
            open(scraped_output_log, 'w').close()
            print(f"[INFO] Cleared {scraped_output_log}")
        if os.path.exists(supabase_db_log):
            open(supabase_db_log, 'w').close()
            print(f"[INFO] Cleared {supabase_db_log}")
        if os.path.exists(rightmove_scraper_log):
            open(rightmove_scraper_log, 'w').close()
            print(f"[INFO] Cleared {rightmove_scraper_log}")
    except Exception as e:
        print(f"[WARNING] Failed to clear log files: {e}")

    postcode_csv = os.path.join(ROOT_DIR, "data", "Postcodes.csv")

    if not os.path.exists(postcode_csv):
        print(f"[ERROR] Postcodes file not found at {postcode_csv}")
        return

    print(f"[INFO] Starting Rightmove scraper with {postcode_csv}")
    run_scraper(postcode_csv)

if __name__ == "__main__":
    main()
