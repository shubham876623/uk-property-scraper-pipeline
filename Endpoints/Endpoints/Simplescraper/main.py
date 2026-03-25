import os
import sys
import asyncio
# import logging
import subprocess

# =====================================================
# 🛠️  PATH FIX  —  Ensure project root is on sys.path
# =====================================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CURRENT_DIR)
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)
    print(f"[PATH FIX] Added {ROOT_DIR} to sys.path")

# =====================================================
# 🧱  IMPORTS
# =====================================================
from Simplescraper.scraper import EPCScraper

# =====================================================
# 🧩  #logging SETUP
# =====================================================
LOG_DIR = os.path.join(CURRENT_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# #logging.basicConfig(
#     filename=os.path.join(LOG_DIR, "simplescraper_runtime.log"),
#     level=#logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     encoding="utf-8"
# )

# =====================================================
# 🧾  SAFE PRINT (avoids Windows Unicode errors)
# =====================================================
def safe_print(msg: str):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode("ascii", errors="ignore").decode())

# =====================================================
# 📤  LOAD POSTCODES FROM FILE
# =====================================================
import csv

def load_postcodes_dynamic(csv_path: str):
    postcodes = []

    # Open with utf-8-sig to automatically strip BOM (\ufeff)
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            postcode = row[0].strip()

            # Skip header or empty lines
            if not postcode or postcode.lower() == "postcode":
                continue

            postcodes.append(postcode)

    print(f"Loaded {len(postcodes)} postcodes successfully.")
    # Removed: print(postcodes) - Don't log the entire list of postcodes
    return postcodes



# =====================================================
# 🚀  MAIN SCRAPER LOGIC
# =====================================================
async def run_scraper(csv_path: str):
    """Main async entrypoint for EPC Scraper."""
    # Clear log files at start of each run
    log_file_path = os.path.join(LOG_DIR, "simple_scraper.log")
    if os.path.exists(log_file_path):
        try:
            os.remove(log_file_path)
            safe_print(f"[🧹] Cleared log file: {log_file_path}")
        except Exception as e:
            safe_print(f"[WARNING] Failed to clear log file: {e}")
    
    # Clear uploader runtime log at start of each run
    uploader_log_path = os.path.join(LOG_DIR, "uploader_runtime.log")
    if os.path.exists(uploader_log_path):
        try:
            os.remove(uploader_log_path)
            safe_print(f"[🧹] Cleared uploader log file: {uploader_log_path}")
        except Exception as e:
            safe_print(f"[WARNING] Failed to clear uploader log file: {e}")
    
    postcodes = load_postcodes_dynamic(csv_path)
    if not postcodes:
        safe_print("No postcodes found in file.")
        return

    scraper = EPCScraper(
        concurrency=150,  # Increased from 50 to 150 for faster processing
        proxy_file=os.path.join(CURRENT_DIR, "config", "proxies.txt"),
        skip_log=os.path.join(LOG_DIR, "skipped_postcodes.txt"),
        batch_size=100  # Batch size for file writes
    )

    safe_print(f"Starting EPC scraping for {len(postcodes)} postcodes...")
    #logging.info(f"Starting EPC scraping for {len(postcodes)} postcodes...")

    try:
        await scraper.run(postcodes)
        safe_print("EPC Scraper run completed successfully.")
        #logging.info("EPC Scraper run completed successfully.")
    except Exception as e:
        safe_print(f"Error during scraping: {e}")
        #logging.error(f"Error during scraping: {e}")

    # =====================================================
    # 🧠  AUTO-RUN UPLOADER AFTER SCRAPER FINISHES
    # =====================================================
    try:
        uploader_path = os.path.join(CURRENT_DIR, "uploader.py")
        if not os.path.exists(uploader_path):
            safe_print("⚠️ uploader.py not found, skipping upload step.")
            #logging.warning("uploader.py not found, skipping upload step.")
            return

        safe_print("Launching uploader.py to upload scraped data to Supabase...")
        #logging.info("Launching uploader.py after scraping completed.")

        log_file_path = os.path.join(LOG_DIR, "uploader_runtime.log")
        with open(log_file_path, "a", encoding="utf-8") as log_file:
            subprocess.Popen(
                [sys.executable, uploader_path],
                cwd=CURRENT_DIR,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                shell=True if os.name == "nt" else False
            )

        safe_print(f"Uploader launched successfully in background. Logs → {log_file_path}")
        #logging.info(f"Uploader launched successfully in background. Logs: {log_file_path}")

    except Exception as e:
        safe_print(f"Failed to start uploader: {e}")
        #logging.error(f"Failed to start uploader: {e}")

# =====================================================
# 🧠  ENTRY POINT
# =====================================================
# =====================================================
# 🧠  ENTRY POINT
# =====================================================
if __name__ == "__main__":
    try:
        csv_path = sys.argv[1] if len(sys.argv) > 1 else None
        
        # Resolve CSV path - if relative, make it relative to script directory
        if csv_path:
            if not os.path.isabs(csv_path):
                # Relative path - resolve relative to script directory
                csv_path = os.path.join(CURRENT_DIR, csv_path)
            # Normalize the path
            csv_path = os.path.normpath(csv_path)
        
        # Validate CSV path exists
        if not csv_path or not os.path.exists(csv_path):
            safe_print("No valid CSV file path provided. Example:")
            safe_print("python main.py input/SG.csv")
            if csv_path:
                safe_print(f"Checked path: {csv_path}")
                safe_print(f"Current working directory: {os.getcwd()}")
                safe_print(f"Script directory: {CURRENT_DIR}")
            sys.exit(1)

        safe_print(f"[PATH FIX] Added {ROOT_DIR} to sys.path")
        safe_print(f"Using CSV file: {csv_path}")
        asyncio.run(run_scraper(csv_path))

        # =====================================================
        # 🚀  AUTO START UPLOADER AFTER SCRAPING
        # =====================================================
        from subprocess import Popen, STDOUT

        uploader_path = os.path.join(CURRENT_DIR, "uploader.py")
        uploader_log = os.path.join(LOG_DIR, "uploader_runtime.log")

        safe_print("Launching uploader.py to upload scraped data to Supabase...")

        with open(uploader_log, "a", encoding="utf-8") as log_file:
            Popen(
                ["python", uploader_path],
                stdout=log_file,
                stderr=STDOUT,
                cwd=CURRENT_DIR,
                shell=True if os.name == "nt" else False
            )

        safe_print(f"Uploader launched successfully in background. Logs → {uploader_log}")
        sys.exit(0)  # ✅ ensure scraper terminates cleanly

    except Exception as e:
        safe_print(f"Fatal error in main.py: {e}")
        #logging.error(f"Fatal error in main.py: {e}")

