from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse
import aiofiles
import os
import subprocess
import threading
import psutil
import platform
from datetime import datetime
from Simplescraper.main import load_postcodes_dynamic
from epc_deep_scraper.src.main import run_deep_scraper

import requests
import os
import pandas as pd
from dotenv import load_dotenv

# Check for Supabase credentials
load_dotenv()
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
app = FastAPI(title="EPC Scraper API", version="3.0")

# ======================================
# 📁 GLOBAL PATHS & SETUP
# ======================================
BASE_DIR = os.getcwd()
LOG_DIR = os.path.join(BASE_DIR, "logs")
PID_DIR = os.path.join(LOG_DIR, "pids")
SIMPLE_SCRAPER_DIR = os.path.join(BASE_DIR, "Simplescraper")
UPLOAD_DIR = os.path.join(SIMPLE_SCRAPER_DIR, "input")
IMAGE_SCRAPER_DIR = os.path.join(BASE_DIR, "image_scraper")
RIGHTMOVE_DIR = os.path.join(BASE_DIR, "rightmovescraper")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(PID_DIR, exist_ok=True)
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ======================================
# 🧩 HELPER FUNCTION
# ======================================
def trigger_scraper(script_path, scraper_name, cwd, args=None):
    """Generic function to trigger any scraper asynchronously and store PID."""
    log_path = os.path.join(LOG_DIR, f"{scraper_name}_scraper.log")
    pid_file = os.path.join(PID_DIR, f"{scraper_name}_scraper.pid")

    if not os.path.exists(script_path):
        raise HTTPException(status_code=404, detail=f"{scraper_name} script not found: {script_path}")

    # Build command with optional arguments
    cmd = ["python", script_path]
    if args:
        if isinstance(args, str):
            cmd.append(args)
        elif isinstance(args, list):
            cmd.extend(args)

    # Clear log file at start of each run
    if os.path.exists(log_path):
        try:
            os.remove(log_path)
        except Exception as e:
            pass  # If we can't delete, continue anyway
    
    with open(log_path, "a", encoding="utf-8") as log_file:
        process = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd=cwd,
            shell=True if os.name == "nt" else False
        )

    # ✅ Save PID for StopScraper
    with open(pid_file, "w") as f:
        f.write(str(process.pid))

    return {
        "status": "started",
        "scraper": scraper_name,
        "pid": process.pid,
        "message": f"{scraper_name.capitalize()} scraper started successfully 🚀",
        "log_file": log_path
    }

# ======================================
# 🏠 ROOT
# ======================================
@app.get("/")
def root():
    return {"message": "✅ EPC Scraper API is running"}

# ======================================
# 🧱 EPC SIMPLE SCRAPER
# ======================================
@app.get("/EpcSimpleScraper-list-files/", tags=["EPC Simple Scraper"])
async def list_uploaded_files():
    """List all CSV/TXT files in Simplescraper/input with postcode count."""
    import csv
    try:
        files = []
        for filename in os.listdir(UPLOAD_DIR):
            if filename.lower().endswith((".csv", ".txt")):
                file_path = os.path.join(UPLOAD_DIR, filename)
                size_kb = round(os.path.getsize(file_path) / 1024, 2)
                modified_time = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime("%Y-%m-%d %H:%M:%S")
                
                # Count postcodes in the file
                postcode_count = 0
                try:
                    with open(file_path, 'r', encoding='utf-8') as csvfile:
                        # Try to detect if file has header
                        first_line = csvfile.readline()
                        csvfile.seek(0)
                        
                        # Check if first line looks like a header (contains common header keywords)
                        has_header = any(keyword in first_line.lower() for keyword in ['postcode', 'code', 'address'])
                        
                        reader = csv.reader(csvfile)
                        if has_header:
                            next(reader)  # Skip header row
                        
                        for row in reader:
                            if row and row[0].strip():  # Check if first column has data
                                postcode_count += 1
                except Exception as e:
                    # If counting fails, set to -1 to indicate error
                    postcode_count = -1
                
                files.append({
                    "filename": filename,
                    "size_kb": size_kb,
                    "last_modified": modified_time,
                    "postcode_count": postcode_count if postcode_count >= 0 else None
                })

        if not files:
            return JSONResponse(content={"message": "No CSV/TXT files found.", "files": []})

        return {"count": len(files), "files": sorted(files, key=lambda f: f['filename'])}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/EpcSimpleScraper-scraper-status/", tags=["EPC Simple Scraper"])
def scraper_status(prefix: str = Query(None, description="Postcode prefix, e.g. 'SG'. If not provided, returns status for all scrapers.")):
    """Check the latest log line from the EPC scraper and detect completion."""
    try:
        if prefix:
            # Single scraper status
            log_path = os.path.join(LOG_DIR, f"simple_scraper.log")
            pid_file = os.path.join(PID_DIR, "simple_scraper.pid")
            
            # Check if process is running
            is_running = False
            if os.path.exists(pid_file):
                try:
                    with open(pid_file, 'r') as f:
                        pid = int(f.read().strip())
                    # Check if process exists
                    if psutil.pid_exists(pid):
                        try:
                            proc = psutil.Process(pid)
                            is_running = proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
                        except:
                            is_running = False
                except:
                    is_running = False
            
            if not os.path.exists(log_path):
                return {"status": "not_started", "prefix": prefix, "message": f"No log found for simple_scraper.log", "is_running": False}
            
            # Read log file
            with open(log_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                last_line = lines[-1].strip() if lines else "No log output yet"
                # Check last 50 lines for completion indicators
                recent_lines = [line.strip().lower() for line in lines[-50:]]
            
            # Detect completion
            completion_keywords = [
                "epc scraper run completed successfully",
                "completed successfully",
                "scraping completed",
                "finished scraping",
                "uploader launched successfully"
            ]
            
            is_completed = any(keyword in last_line.lower() for keyword in completion_keywords) or \
                          any(any(kw in line for kw in completion_keywords) for line in recent_lines)
            
            # Check for errors
            error_keywords = ["error", "failed", "exception", "traceback"]
            has_error = any(keyword in last_line.lower() for keyword in error_keywords)
            
            status = "completed" if is_completed else ("error" if has_error else ("running" if is_running else "stopped"))
            
            return {
                "status": status,
                "prefix": prefix,
                "last_log": last_line,
                "is_running": is_running,
                "is_completed": is_completed,
                "has_error": has_error
            }
        else:
            # Return status for all scrapers (list all log files)
            all_statuses = []
            for filename in os.listdir(LOG_DIR):
                if filename.endswith("_scraper.log"):
                    scraper_name = filename.replace("_scraper.log", "")
                    log_path = os.path.join(LOG_DIR, filename)
                    pid_file = os.path.join(PID_DIR, f"{scraper_name}_scraper.pid")
                    
                    is_running = False
                    if os.path.exists(pid_file):
                        try:
                            with open(pid_file, 'r') as f:
                                pid = int(f.read().strip())
                            if psutil.pid_exists(pid):
                                try:
                                    proc = psutil.Process(pid)
                                    is_running = proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
                                except:
                                    pass
                        except:
                            pass
                    
                    if os.path.exists(log_path):
                        with open(log_path, "r", encoding="utf-8") as f:
                            lines = f.readlines()
                            last_line = lines[-1].strip() if lines else "No log output yet"
                            recent_lines = [line.strip().lower() for line in lines[-50:]]
                        
                        completion_keywords = [
                            "epc scraper run completed successfully",
                            "completed successfully",
                            "scraping completed",
                            "finished scraping"
                        ]
                        is_completed = any(keyword in last_line.lower() for keyword in completion_keywords) or \
                                      any(any(kw in line for kw in completion_keywords) for line in recent_lines)
                        
                        status = "completed" if is_completed else ("running" if is_running else "stopped")
                    else:
                        last_line = "No log file"
                        is_completed = False
                        status = "not_started"
                    
                    all_statuses.append({
                        "scraper": scraper_name,
                        "status": status,
                        "last_log": last_line,
                        "is_running": is_running,
                        "is_completed": is_completed
                    })
            
            return {"count": len(all_statuses), "scrapers": all_statuses}
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def safe_int(value, default=0):
    """Safely convert value to int, returning default if conversion fails"""
    try:
        return int(value) if value else default
    except (ValueError, TypeError):
        return default

@app.get("/EpcSimpleScraper-job-status/", tags=["EPC Simple Scraper"])
def get_job_status(filename: str = Query(..., description="CSV filename, e.g. 'SG.csv'")):
    """
    Get comprehensive status for a scraping job including:
    - Scraping completion status
    - Database insertion status
    - Number of records scraped and inserted
    """
    import csv
    import re
    
    try:
        # Extract prefix from filename (e.g., "SG.csv" -> "SG")
        prefix = os.path.splitext(filename)[0]
        
        # Check scraper log
        scraper_log_path = os.path.join(LOG_DIR, "simple_scraper.log")
        scraper_completed = False
        scraper_error = False
        scraper_last_log = ""
        
        if os.path.exists(scraper_log_path):
            with open(scraper_log_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                scraper_last_log = lines[-1].strip() if lines else "No log output yet"
                recent_lines = " ".join([line.strip().lower() for line in lines[-100:]])
                
                # Check for completion
                completion_keywords = [
                    "epc scraper run completed successfully",
                    "completed successfully",
                    "scraping completed",
                    "finished scraping"
                ]
                scraper_completed = any(kw in recent_lines for kw in completion_keywords)
                
                # Check for errors
                error_keywords = ["error", "failed", "exception", "traceback"]
                scraper_error = any(kw in scraper_last_log.lower() for kw in error_keywords)
        
        # Check uploader log - look for uploader output in scraper log or separate uploader log
        uploader_log_path = os.path.join(SIMPLE_SCRAPER_DIR, "logs", "uploader_runtime.log")
        # Also check if uploader runs in same process (check scraper log)
        uploader_completed = False
        uploader_error = False
        uploader_last_log = ""
        records_inserted = 0
        records_updated = 0
        records_failed = 0
        records_successfully_uploaded = 0  # Total successfully uploaded (inserted + updated)
        records_successfully_uploaded = 0  # Total successfully uploaded (inserted + updated)
        
        # Check both scraper log and uploader log
        log_files_to_check = [scraper_log_path]
        if os.path.exists(uploader_log_path):
            log_files_to_check.append(uploader_log_path)
        
        for log_file in log_files_to_check:
            if os.path.exists(log_file):
                with open(log_file, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                    if lines:
                        uploader_last_log = lines[-1].strip()
                        recent_lines = " ".join([line.strip().lower() for line in lines[-200:]])
                        
                        # Check for completion
                        completion_keywords = [
                            "upload summary",
                            "upload completed",
                            "upload finished",
                            "waiting for next batch"
                        ]
                        if any(kw in recent_lines for kw in completion_keywords):
                            uploader_completed = True
                        
                        # Extract insertion counts from simple_scraper.log - look for "Successfully Inserted : X"
                        # Check both possible log locations
                        scraper_log_paths = [
                            os.path.join(LOG_DIR, "simple_scraper.log"),  # Main logs directory
                            os.path.join(SIMPLE_SCRAPER_DIR, "logs", "simple_scraper.log")  # Simplescraper logs directory
                        ]
                        for scraper_log_path in scraper_log_paths:
                            if os.path.exists(scraper_log_path):
                                try:
                                    with open(scraper_log_path, 'r', encoding='utf-8') as scraper_log:
                                        scraper_lines = scraper_log.readlines()
                                        # Search from end to get most recent value
                                        for line in reversed(scraper_lines):
                                            # Look for "Successfully Inserted : X" pattern
                                            success_match = re.search(r'Successfully Inserted\s*:\s*(\d+)', line, re.IGNORECASE)
                                            if success_match:
                                                records_successfully_uploaded = int(success_match.group(1))
                                                break  # Use the most recent value found
                                        if records_successfully_uploaded > 0:
                                            break  # Found value, no need to check other paths
                                except Exception as e:
                                    pass  # Silently fail, will fall back to other parsing
                        
                        # Also check uploader log for STATUS lines
                        for line in lines[-200:]:
                            # Look for pattern: "[STATUS] Postcode X: records_inserted=Y, records_updated=Z, records_failed=W"
                            status_match = re.search(r'\[STATUS\].*?records_inserted\s*=\s*(\d+).*?records_updated\s*=\s*(\d+).*?records_failed\s*=\s*(\d+)', line, re.IGNORECASE)
                            if status_match:
                                inserted = int(status_match.group(1))
                                updated = int(status_match.group(2))
                                failed = int(status_match.group(3))
                                records_inserted += inserted
                                records_updated += updated
                                records_failed += failed
                                # Only update records_successfully_uploaded if not already set from simple_scraper.log
                                if records_successfully_uploaded == 0:
                                    records_successfully_uploaded = inserted + updated
                            
                            # Also check for "Inserted : X" and "Updated  : Y" from Upload Summary
                            summary_inserted_match = re.search(r'Inserted\s*:\s*(\d+)', line, re.IGNORECASE)
                            if summary_inserted_match:
                                inserted_val = int(summary_inserted_match.group(1))
                                if inserted_val > records_inserted:
                                    records_inserted = inserted_val
                            
                            summary_updated_match = re.search(r'Updated\s*:\s*(\d+)', line, re.IGNORECASE)
                            if summary_updated_match:
                                updated_val = int(summary_updated_match.group(1))
                                if updated_val > records_updated:
                                    records_updated = updated_val
                            
                            # Only update if not already set from simple_scraper.log
                            if (summary_inserted_match or summary_updated_match) and records_successfully_uploaded == 0:
                                records_successfully_uploaded = records_inserted + records_updated
                        
                        # Check for errors
                        error_keywords = ["error", "failed", "exception", "traceback", "authentication failed"]
                        if any(kw in uploader_last_log.lower() for kw in error_keywords):
                            uploader_error = True
        
        # Check data.csv for records count and calculate NEW records (not in DB)
        data_csv_path = os.path.join(SIMPLE_SCRAPER_DIR, "data.csv")
        records_in_csv = 0
        new_records_count = 0
        existing_records_count = 0
        
        if os.path.exists(data_csv_path):
            try:
                with open(data_csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    all_records = list(reader)
                    records_in_csv = len(all_records)
                    
                    # Check which URNs already exist in database
                    if records_in_csv > 0:
                        # Get all URNs from data.csv
                        urns_in_csv = set()
                        for row in all_records:
                            urn = row.get("CertificateNumber", "").strip()
                            if urn:
                                urns_in_csv.add(urn)
                        
                        # Fetch existing URNs from Supabase for this prefix
                        # Skip DB comparison for large datasets (>5000 URNs in CSV) to avoid timeout
                        try:
                            import requests as req
                            from dotenv import load_dotenv
                            load_dotenv()
                            supabase_url = os.getenv("SUPABASE_URL")
                            supabase_key = os.getenv("SUPABASE_KEY")
                            supabase_table = os.getenv("SUPABASE_TABLE", "EPCCertificateSimpleScrape")

                            if supabase_url and supabase_key and len(urns_in_csv) <= 5000:
                                headers = {
                                    "apikey": supabase_key,
                                    "Authorization": f"Bearer {supabase_key}",
                                    "Content-Type": "application/json"
                                }

                                # Fetch existing URNs for postcodes starting with this prefix
                                existing_urns = set()
                                range_start = 0
                                page_size = 1000

                                while True:
                                    range_end = range_start + page_size - 1
                                    url = f"{supabase_url}/rest/v1/{supabase_table}?select=URN,Postcode&Postcode=ilike.{prefix}%"
                                    paginated_headers = {**headers, "Range": f"{range_start}-{range_end}"}

                                    response = req.get(url, headers=paginated_headers, timeout=10)
                                    if response.status_code == 401:
                                        # Authentication failed - cannot check existing records
                                        new_records_count = records_in_csv
                                        existing_records_count = 0
                                        break
                                    elif response.status_code != 200:
                                        break

                                    data = response.json()
                                    if not data:
                                        break

                                    for record in data:
                                        urn = record.get("URN")
                                        if urn:
                                            existing_urns.add(str(urn))

                                    if len(data) < page_size:
                                        break

                                    range_start += page_size

                                    # Safety limit
                                    if len(existing_urns) >= 50000:
                                        break

                                # Calculate new vs existing
                                new_records_count = len(urns_in_csv - existing_urns)
                                existing_records_count = len(urns_in_csv & existing_urns)
                            else:
                                # Skip DB comparison for large datasets to avoid timeout
                                new_records_count = records_in_csv
                                existing_records_count = 0
                        except Exception as e:
                            # If we can't check, assume all are new
                            new_records_count = records_in_csv
                            existing_records_count = 0
            except:
                pass
        
        # Check completed_postcodes.csv for this file's postcodes
        completed_postcodes_path = os.path.join(SIMPLE_SCRAPER_DIR, "logs", "completed_postcodes.csv")
        postcodes_completed = 0
        total_records_scraped = 0
        if os.path.exists(completed_postcodes_path):
            try:
                with open(completed_postcodes_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        postcode = row.get("postcode", "").strip()
                        if postcode.startswith(prefix):
                            postcodes_completed += 1
                            try:
                                total_records_scraped += int(row.get("records_scraped", 0))
                            except:
                                pass
            except:
                pass
        
        # Determine overall status
        if scraper_error or uploader_error:
            overall_status = "error"
        elif scraper_completed and uploader_completed:
            overall_status = "completed"
        elif scraper_completed and not uploader_completed:
            overall_status = "uploading"
        elif not scraper_completed:
            overall_status = "scraping"
        else:
            overall_status = "unknown"
        
        # Calculate new records and new records with new dates from uploader log
        new_records_found_from_log = 0
        new_records_with_new_dates = 0
        existing_records_skipped_from_log = 0
        records_inserted_from_log = 0
        records_updated_from_log = 0
        
        # Parse uploader log for STATUS lines
        for log_file in log_files_to_check:
            if os.path.exists(log_file):
                try:
                    with open(log_file, "r", encoding="utf-8") as f:
                        lines = f.readlines()
                        for line in lines[-200:]:  # Check last 200 lines
                            # Look for pattern: "[STATUS] Postcode X: new_records_found=Y, new_records_found_with_new_dates=Z, existing_records_skipped=W"
                            status_match = re.search(r'\[STATUS\]\s+Postcode\s+\S+:\s+new_records_found=(\d+),\s+new_records_found_with_new_dates=(\d+),\s+existing_records_skipped=(\d+)', line, re.IGNORECASE)
                            if status_match:
                                new_records_found_from_log += int(status_match.group(1))
                                new_records_with_new_dates += int(status_match.group(2))
                                existing_records_skipped_from_log += int(status_match.group(3))
                            
                            # Look for pattern: "[STATUS] Postcode X: records_inserted=Y, records_updated=Z, records_failed=W"
                            insert_match = re.search(r'\[STATUS\]\s+Postcode\s+\S+:\s+records_inserted=(\d+),\s+records_updated=(\d+),\s+records_failed=(\d+)', line, re.IGNORECASE)
                            if insert_match:
                                records_inserted_from_log += int(insert_match.group(1))
                                records_updated_from_log += int(insert_match.group(2))
                            
                            # Also check for pattern: "X NEW records (Y with new dates)"
                            match = re.search(r'(\d+)\s+NEW records\s+\((\d+)\s+with new dates\)', line, re.IGNORECASE)
                            if match:
                                new_records_found_from_log = max(new_records_found_from_log, int(match.group(1)))
                                new_records_with_new_dates = max(new_records_with_new_dates, int(match.group(2)))
                            
                            # Also check for pattern: "Found X new date(s) → Y NEW records (Z with new dates)"
                            date_match = re.search(r'Found\s+(\d+)\s+new date\(s\)\s+→\s+(\d+)\s+NEW records\s+\((\d+)\s+with new dates\)', line, re.IGNORECASE)
                            if date_match:
                                new_records_found_from_log = max(new_records_found_from_log, int(date_match.group(2)))
                                new_records_with_new_dates = max(new_records_with_new_dates, int(date_match.group(3)))
                except:
                    pass
        
        # Parse changes_log.csv for date and count changes
        changes_log_path = os.path.join(SIMPLE_SCRAPER_DIR, "logs", "changes_log.csv")
        date_changed_count = 0
        count_changed_count = 0
        date_changed_records = 0
        count_changed_records = 0
        
        if os.path.exists(changes_log_path):
            try:
                with open(changes_log_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        postcode = row.get('Postcode', '').strip()
                        if postcode.startswith(prefix):
                            change_type = row.get('ChangeType', '').strip()
                            records_count = safe_int(row.get('RecordsCount', 0), 0)
                            
                            if change_type == 'DATE_CHANGED':
                                date_changed_count += 1
                                date_changed_records += records_count
                            elif change_type == 'COUNT_CHANGED':
                                count_changed_count += 1
                                count_changed_records += records_count
            except Exception as e:
                pass
        
        # Also parse scraper log for [DATES CHANGED] messages as fallback
        scraper_log_path_alt = os.path.join(BASE_DIR, "logs", "simple_scraper.log")
        records_with_new_dates_from_scraper = 0
        for log_path in [scraper_log_path, scraper_log_path_alt]:
            if os.path.exists(log_path):
                try:
                    with open(log_path, "r", encoding="utf-8") as f:
                        lines = f.readlines()
                        for i, line in enumerate(lines):
                            # Look for [DATES CHANGED] messages with prefix
                            if "[DATES CHANGED]" in line and prefix in line:
                                # Look for "Writing all X records" in the same or next few lines (up to 6 lines ahead)
                                for j in range(i, min(i + 6, len(lines))):
                                    write_match = re.search(r'Writing all (\d+) records', lines[j], re.IGNORECASE)
                                    if write_match:
                                        count = int(write_match.group(1))
                                        records_with_new_dates_from_scraper += count
                                        break
                except:
                    pass
                break  # Use first found log file
        
        # Use changes_log.csv counts if available, otherwise use scraper log or uploader log
        final_records_with_new_dates = date_changed_records if date_changed_records > 0 else (records_with_new_dates_from_scraper if records_with_new_dates_from_scraper > 0 else new_records_with_new_dates)
        final_count_changed_records = count_changed_records
        
        # Use new_records_found_from_log if available, otherwise use new_records_count from data.csv
        final_new_records_found = new_records_found_from_log if new_records_found_from_log > 0 else new_records_count
        
        # Calculate overall_status:
        # - "scraping" when scraper is not completed yet
        # - "completed" when scraper is done AND records_pending_upload = 0
        # - "uploading" when scraper is done but records are still pending upload
        if not scraper_completed:
            final_overall_status = "scraping"
        elif records_in_csv == 0:
            final_overall_status = "completed"
        else:
            final_overall_status = "uploading"
        
        return {
            "filename": filename,
            "prefix": prefix,
            "overall_status": final_overall_status,
            "scraping": {
                "completed": scraper_completed,
                "error": scraper_error,
                "last_log": scraper_last_log,
                "postcodes_completed": postcodes_completed,
                "total_records_scraped": total_records_scraped,
                "records_pending_upload": records_in_csv,
                "records_successfully_uploaded": records_successfully_uploaded,
                "existing_records_skipped": existing_records_count,
                "date_changed_count": date_changed_count,
                "date_changed_records": date_changed_records,
                "count_changed_count": count_changed_count,
                "count_changed_records": count_changed_records,
                # "message": f"Scraped {total_records_scraped} records from {postcodes_completed} postcodes. Found {final_new_records_found} new records ({final_records_with_new_dates} with new dates). Date changes: {date_changed_count} postcodes ({date_changed_records} records). Count changes: {count_changed_count} postcodes ({count_changed_records} records). {records_in_csv} records pending upload."
            },
           
        }
        
    except Exception as e:
        import traceback
        raise HTTPException(status_code=500, detail=f"Error getting job status: {str(e)}\n{traceback.format_exc()}")


@app.get("/EpcSimpleScraper-completed-postcodes", tags=["EPC Simple Scraper"])
async def get_completed_postcodes_simple():
    """
    Get all completed postcodes from completed_postcodes.csv file.
    Returns postcode, completion date, and records_scraped count for all completed postcodes.
    """
    import csv
    from datetime import datetime
    
    completed_postcodes_path = os.path.join(SIMPLE_SCRAPER_DIR, "logs", "completed_postcodes.csv")
    
    if not os.path.exists(completed_postcodes_path):
        return {
            "total": 0,
            "completed_postcodes": [],
            "message": "No completed postcodes file found yet."
        }
    
    try:
        completed_postcodes = []
        
        with open(completed_postcodes_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            # Track position for sorting (most recent first)
            position = 0
            for row in reader:
                position += 1
                postcode = row.get("postcode", "").strip()
                completed_at = row.get("completed_at", "").strip()
                records_scraped = row.get("records_scraped", "0").strip()
                
                if postcode:
                    # Parse records_scraped as integer
                    try:
                        records_count = int(records_scraped)
                    except (ValueError, TypeError):
                        records_count = 0
                    
                    # Format completed_at if needed (it should already be in ISO format)
                    if completed_at:
                        # If it's already in ISO format, keep it; otherwise try to parse
                        try:
                            # Try parsing ISO format first
                            if 'T' in completed_at:
                                dt = datetime.fromisoformat(completed_at.replace('Z', '+00:00'))
                            else:
                                # Try YYYY-MM-DD format
                                dt = datetime.strptime(completed_at, "%Y-%m-%d")
                            completed_at = dt.isoformat()
                        except:
                            pass  # Keep original format if parsing fails
                    
                    completed_postcodes.append({
                        "postcode": postcode,
                        "completed_at": completed_at,
                        "records_scraped": records_count,
                        "_position": position  # Temporary field for sorting
                    })
        
        # Sort by position descending (most recently scraped first)
        completed_postcodes.sort(key=lambda x: x.get("_position", -1), reverse=True)
        
        # Remove the temporary sorting field
        for item in completed_postcodes:
            item.pop("_position", None)
        
        return {
            "total": len(completed_postcodes),
            "completed_postcodes": completed_postcodes
        }
        
    except Exception as e:
        import traceback
        return JSONResponse(content={
            "error": f"Error reading completed postcodes: {str(e)}",
            "traceback": traceback.format_exc()
        }, status_code=500)
    
@app.post("/EpcSimpleScraper_upload/", tags=["EPC Simple Scraper"])
async def upload_postcodes(file: UploadFile = File(...)):
    """Upload a CSV/TXT file (does not trigger automatically)."""
    try:
        filename = file.filename
        ext = os.path.splitext(filename)[1].lower()
        if ext not in [".csv", ".txt"]:
            raise HTTPException(status_code=400, detail="Only .csv or .txt files are allowed.")

        file_path = os.path.join(UPLOAD_DIR, filename)
        async with aiofiles.open(file_path, 'wb') as out_file:
            await out_file.write(await file.read())

        load_postcodes_dynamic(file_path)
        return {"status": "uploaded", "file": filename, "message": f"{filename} uploaded successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/EpcSimpleScraper-trigger/", tags=["EPC Simple Scraper"])
async def trigger_simple_scraper(prefix: str = Query(..., description="Postcode prefix, e.g. 'SG'")):
    """Trigger EPC Simple Scraper asynchronously."""
    csv_path = os.path.join(UPLOAD_DIR, f"{prefix}.csv")
    script_path = os.path.join(SIMPLE_SCRAPER_DIR, "main.py")

    if not os.path.exists(csv_path):
        raise HTTPException(status_code=404, detail=f"No file found for {prefix}.csv")

    # Pass CSV path as relative path from the script's directory
    relative_csv_path = os.path.relpath(csv_path, SIMPLE_SCRAPER_DIR)
    return trigger_scraper(script_path, "simple", SIMPLE_SCRAPER_DIR, args=relative_csv_path)


# ======================================
# 🧠 IMAGE SCRAPER
# ======================================
@app.post("/ImageScraper", tags=["Image Scraper"])
async def trigger_image_scraper(propertyoutcode: str = Query(..., description="PropertyOutcode, e.g. 'SG1'")):
    """Trigger Image Scraper asynchronously."""
    script_path = os.path.join(IMAGE_SCRAPER_DIR, "scripts", "main.py")

    if not os.path.exists(script_path):
        raise HTTPException(status_code=404, detail="Image scraper script not found.")

    # Pass propertyoutcode as command-line argument AND environment variable (for compatibility)
    os.environ["IMAGE_SCRAPER_OUTCODE"] = propertyoutcode
    return trigger_scraper(script_path, "image", IMAGE_SCRAPER_DIR, args=propertyoutcode)


@app.get("/ImageScraper-status/", tags=["Image Scraper"])
def get_image_scraper_status():
    """
    Get the current status of the EPC Image Scraper, including progress and activity.
    """
    import re
    log_path = os.path.join(LOG_DIR, "image_scraper.log")
    pid_file = os.path.join(PID_DIR, "image_scraper.pid")

    # Check if process is running
    is_running = False
    pid = None
    if os.path.exists(pid_file):
        try:
            with open(pid_file, 'r') as f:
                pid = int(f.read().strip())
            if psutil.pid_exists(pid):
                try:
                    proc = psutil.Process(pid)
                    is_running = proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
                except:
                    is_running = False
        except:
            is_running = False

    # Parse log file for progress
    properties_processed = 0
    properties_skipped = 0
    properties_updated = 0
    current_property_id = None
    current_outcode = None
    last_log = ""
    is_completed = False
    has_error = False
    start_time = None

    if os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
            last_log = lines[-1].strip() if lines else "No log output yet"
            
            # Find the MOST RECENT session start to count from current session only
            # Search from end backwards to find the latest "Image scraper starting" marker
            session_start_idx = 0
            for i in range(len(lines) - 1, -1, -1):  # Search from end backwards
                if "image scraper starting" in lines[i].lower():
                    session_start_idx = i
                    break
            
            # Use lines from most recent session start, or last 200 lines if session start not found
            if session_start_idx > 0:
                session_lines = lines[session_start_idx:]
            else:
                session_lines = lines[-200:]  # Fallback to last 200 lines
            
            recent_lines = lines[-200:]  # Keep recent lines for completion/error detection

            # Check for completion
            completion_keywords = [
                "completed image scraper run",
                "completed successfully",
                "finished"
            ]
            recent_text = " ".join([line.lower() for line in recent_lines])
            is_completed = any(kw in recent_text for kw in completion_keywords)

            # Check for errors
            error_keywords = ["error", "failed", "exception", "traceback"]
            has_error = any(kw in last_log.lower() for kw in error_keywords)

            # Extract total properties found from log
            total_properties_found = None
            for line in session_lines:
                if "found" in line.lower() and "image(s)" in line.lower():
                    # Extract number: "Found 2482 image(s) for SG1"
                    match = re.search(r'found\s+(\d+)\s+image\(s\)', line, re.IGNORECASE)
                    if match:
                        total_properties_found = int(match.group(1))
                        break

            # Extract progress information - count ALL from current session
            for line in session_lines:
                line_lower = line.lower()

                # Find start time
                if ("image scraper starting for propertyoutcode" in line_lower or 
                    "image scraper starting" in line_lower) and start_time is None:
                    start_time = line.strip()
                    outcode_match = re.search(r'propertyoutcode[=:]?\s*([A-Z0-9]+)', line, re.IGNORECASE)
                    if outcode_match:
                        current_outcode = outcode_match.group(1).strip()

                # Count properties processed
                if "processing propertyid" in line_lower:
                    properties_processed += 1
                    prop_id_match = re.search(r'propertyid\s+(\d+)', line_lower)
                    if prop_id_match:
                        current_property_id = prop_id_match.group(1).strip()

                # Count skipped properties
                if "skipping propertyid" in line_lower:
                    properties_skipped += 1

                # Count updated properties
                if "updated" in line_lower and "propertyid" in line_lower:
                    properties_updated += 1
                    prop_id_match = re.search(r'propertyid\s+(\d+)', line_lower)
                    if prop_id_match:
                        current_property_id = prop_id_match.group(1).strip()

    # Determine status
    if is_running:
        status = "running"
    elif is_completed:
        status = "completed"
    elif has_error:
        status = "error"
    else:
        status = "stopped"

    return {
        "status": status,
        "is_running": is_running,
        "pid": pid,
        "start_time": start_time,
        "progress": {
            "properties_processed": properties_processed,
            "properties_updated": properties_updated,
            "properties_skipped": properties_skipped,
            "total_properties_found": total_properties_found  # Total from log: "Found X image(s)"
        },
        "current_activity": {
            "current_outcode": current_outcode,
            "current_property_id": current_property_id
        },
        "last_log": last_log,
        "has_error": has_error,
        "is_completed": is_completed
    }


# ======================================
# 🧩 DEEP SCRAPER
# ======================================
@app.post("/DeepScraper", tags=["EPC Deep Scraper"])
async def trigger_deep_scraper(outcode: str = Query(None, description="Optional postcode outcode filter, e.g. 'SG1', 'SG2'. If not provided, scrapes all properties.")):
    """
    Trigger EPC Deep Scraper asynchronously.
    
    Args:
        outcode: Optional postcode outcode (e.g., 'SG1', 'SG2') to filter properties.
                 If not provided, scrapes all properties.
    """
    try:
        log_path = os.path.join(LOG_DIR, "deep_scraper.log")
        pid_file = os.path.join(PID_DIR, "deep_scraper.pid")

        # Build command with optional outcode argument
        cmd = ["python", os.path.join(BASE_DIR, "epc_deep_scraper", "src", "main.py")]
        if outcode:
            cmd.append(outcode)

        # Ensure log file is opened with UTF-8 encoding
        with open(log_path, "a", encoding="utf-8", errors="replace") as log_file:
            # Set environment variable for UTF-8 encoding
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'
            
            # Run from project root so imports work correctly
            process = subprocess.Popen(
                cmd,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd=BASE_DIR,  # Run from project root, not epc_deep_scraper directory
                shell=True if os.name == "nt" else False,
                env=env  # Pass environment with UTF-8 encoding
            )

        with open(pid_file, "w") as f:
            f.write(str(process.pid))

        message = f"EPC Deep Scraper running 🚀"
        if outcode:
            message += f" (filtered by outcode: {outcode})"
        else:
            message += " (all properties)"

        return {"status": "started", "scraper": "deep", "pid": process.pid, "message": message, "outcode": outcode}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/DeepScraper-status/", tags=["EPC Deep Scraper"])
def get_deep_scraper_status():
    """
    Get comprehensive status of EPC Deep Scraper including:
    - Running status
    - Progress (properties processed, remaining)
    - Current postcode being processed
    - Records scraped/inserted
    - Error status
    """
    import re
    
    try:
        log_path = os.path.join(LOG_DIR, "deep_scraper.log")
        pid_file = os.path.join(PID_DIR, "deep_scraper.pid")
        
        # Check if process is running (more thorough check)
        is_running = False
        pid = None
        if os.path.exists(pid_file):
            try:
                with open(pid_file, 'r') as f:
                    pid = int(f.read().strip())
                if psutil.pid_exists(pid):
                    try:
                        proc = psutil.Process(pid)
                        is_running = proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
                        # Double-check by verifying the process name matches Python
                        if is_running:
                            try:
                                proc_name = proc.name().lower()
                                if 'python' not in proc_name and 'pythonw' not in proc_name:
                                    is_running = False
                            except:
                                pass
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        is_running = False
            except:
                is_running = False
        
        # Parse log file for progress
        properties_processed = 0
        properties_successfully_processed = 0  # Count of successfully completed properties
        current_postcode = None
        current_property_id = None
        certificates_scraped = 0
        certificates_success = 0
        certificates_failed = 0
        certificates_inserted = 0  # New inserts in this session
        certificates_updated = 0  # Updates in this session
        last_log = ""
        is_completed = False
        has_error = False
        start_time = None
        properties_needing_scrape_from_log = None  # Extract from log
        recent_epc_ids = []  # Store recently updated/inserted EPC IDs
        
        if os.path.exists(log_path):
            with open(log_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                last_log = lines[-1].strip() if lines else "No log output yet"
                
                # Find the MOST RECENT session start to count from current session only
                # Search from end backwards to find the latest [START] marker
                session_start_idx = 0
                for i in range(len(lines) - 1, -1, -1):  # Search from end backwards
                    if "[START] Starting EPC Deep Scraper" in lines[i]:
                        session_start_idx = i
                        break
                
                # Use lines from most recent session start, or last 200 lines if session start not found
                if session_start_idx > 0:
                    recent_lines = lines[session_start_idx:]
                else:
                    recent_lines = lines[-200:]  # Fallback to last 200 lines
                
                # Check for completion - must check if process is actually running
                # Only mark as completed if process is NOT running AND we see completion message
                completion_keywords = [
                    "[complete] completed epc deep scraper",
                    "completed epc deep scraper",
                    "completed successfully",
                    "no records to scrape"
                ]
                recent_text = " ".join([line.lower() for line in recent_lines])
                has_completion_message = any(kw in recent_text for kw in completion_keywords)
                
                # Check if process is actually running (more thorough check)
                is_process_running = False
                if pid:
                    try:
                        import psutil
                        if psutil.pid_exists(pid):
                            try:
                                proc = psutil.Process(pid)
                                is_process_running = proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
                            except (psutil.NoSuchProcess, psutil.AccessDenied):
                                is_process_running = False
                    except:
                        pass
                
                # Only mark as completed if process is NOT running AND we have completion message
                is_completed = has_completion_message and not is_process_running
                
                # Update is_running based on actual process state (override earlier check)
                if is_process_running:
                    is_running = True
                
                # Check for errors
                error_keywords = ["error", "failed", "exception", "traceback"]
                has_error = any(kw in last_log.lower() for kw in error_keywords)
                
                # Extract properties needing scrape from log
                for line in recent_lines:
                    line_lower_check = line.lower()
                    if "[supabase success] found" in line_lower_check and "unscripted properties needing deep scrape" in line_lower_check:
                        # Extract number: "Found 105 unscripted properties needing deep scrape."
                        match = re.search(r'found\s+(\d+)\s+unscripted', line, re.IGNORECASE)
                        if match:
                            properties_needing_scrape_from_log = int(match.group(1))
                            break
                
                # Extract progress information
                for i, line in enumerate(recent_lines):
                    line_lower = line.lower()
                    
                    # Find start time
                    if "starting epc deep scraper" in line_lower and start_time is None:
                        # Try to extract timestamp if available
                        start_time = line.strip()
                    
                    # Count properties processed (look for "Scraping EPC Certificates for" or "[SCRAPE]" or "[LIVE PROGRESS]")
                    if ("scraping epc certificates for" in line_lower or 
                        "[scrape]" in line_lower or
                        "[live progress]" in line_lower):
                        # Only count once per property (avoid double counting)
                        if "[live progress]" in line_lower:
                            # Extract from: [LIVE PROGRESS] [X/Y] Processing Postcode: ABC 123
                            progress_match = re.search(r'\[live progress\]\s*\[(\d+)/\d+\]', line_lower)
                            if progress_match:
                                prop_num = int(progress_match.group(1))
                                if prop_num > properties_processed:
                                    properties_processed = prop_num
                            # Extract postcode from LIVE PROGRESS line
                            postcode_match = re.search(r'processing postcode:\s*([A-Z0-9\s]+)', line, re.IGNORECASE)
                            if postcode_match:
                                current_postcode = postcode_match.group(1).strip()
                        else:
                            properties_processed += 1
                            # Extract postcode
                            postcode_match = re.search(r'(?:for|Postcode=)\s*([A-Z0-9\s]+)', line, re.IGNORECASE)
                            if postcode_match:
                                current_postcode = postcode_match.group(1).strip()
                    
                    # Count successfully processed properties
                    # Match patterns like: "[SUCCESS] Completed PropertyId=12345" or "Completed PropertyId=12345 (X/Y successful)"
                    if ("completed propertyid" in line_lower and 
                        ("success" in line_lower or "successful" in line_lower)):
                        properties_successfully_processed += 1
                    
                    # Also check for final stats line: "[STATS] Successful: X"
                    stats_match = re.search(r'\[stats\]\s+successful:\s*(\d+)', line_lower)
                    if stats_match:
                        # Use the final count from stats if it's higher (more accurate)
                        stats_count = int(stats_match.group(1))
                        if stats_count > properties_successfully_processed:
                            properties_successfully_processed = stats_count
                    
                    # Extract property ID when marked
                    # Pattern: Processing PropertyId=12345 or PropertyId=12345
                    property_match = re.search(r'propertyid[=:]\s*(\d+)', line_lower)
                    if property_match:
                        current_property_id = property_match.group(1)
                    
                    # Count certificates
                    if "successfully scraped" in line_lower:
                        # Pattern: "✅ Successfully scraped X/Y certificates."
                        cert_match = re.search(r'scraped\s+(\d+)/(\d+)', line_lower)
                        if cert_match:
                            success = int(cert_match.group(1))
                            total = int(cert_match.group(2))
                            certificates_success += success
                            certificates_scraped += total
                    
                    # Count database inserts (new records) and extract EPC ID
                    # Pattern: [SUCCESS] Inserted EPC ID=12345 or [LIVE] EPC Record Inserted: 12345
                    if (("[success]" in line_lower and "inserted" in line_lower and "epc" in line_lower) or
                        ("[live]" in line_lower and "epc record inserted" in line_lower)):
                        certificates_inserted += 1
                        # Extract EPC ID
                        epc_id_match = re.search(r'epc\s+(?:id[=:]|record inserted:\s*)\s*([A-Z0-9\-]+)', line, re.IGNORECASE)
                        if epc_id_match:
                            recent_epc_ids.append({"id": epc_id_match.group(1), "action": "inserted"})
                    
                    # Also extract total EPC records from LIVE STATS lines
                    # Pattern: [LIVE STATS] EPC Records Inserted: X or [LIVE STATS] EPC Records Inserted So Far: X
                    live_stats_match = re.search(r'\[live stats\]\s+epc records inserted(?:\s+so far)?:\s*(\d+)', line_lower)
                    if live_stats_match:
                        total_from_log = int(live_stats_match.group(1))
                        # Use the higher value (most recent)
                        if total_from_log > certificates_inserted:
                            certificates_inserted = total_from_log
                    
                    # Extract total properties from LIVE STATS
                    # Pattern: [LIVE STATS] Total Properties: X
                    total_props_match = re.search(r'\[live stats\]\s+total properties:\s*(\d+)', line_lower)
                    if total_props_match:
                        total_from_log = int(total_props_match.group(1))
                        # Update total if found
                        if total_from_log > 0:
                            total_properties = total_from_log
                    
                    # Count database updates (existing records) and extract EPC ID
                    # Pattern: [SUCCESS] Updated EPC ID=12345
                    if "[success]" in line_lower and "updated" in line_lower and "epc id=" in line_lower:
                        certificates_updated += 1
                        # Extract EPC ID
                        epc_id_match = re.search(r'epc\s+id[=:]\s*([A-Z0-9\-]+)', line, re.IGNORECASE)
                        if epc_id_match:
                            recent_epc_ids.append({"id": epc_id_match.group(1), "action": "updated"})
                    
                    # Count failed certificates
                    if "failed to fetch" in line_lower or "error processing" in line_lower:
                        certificates_failed += 1
                    
                    # Get current postcode from most recent scraping line
                    if i >= len(recent_lines) - 10:  # Check last 10 lines for current activity
                        if "scraping epc certificates for" in line_lower or "[scrape]" in line_lower:
                            postcode_match = re.search(r'(?:for|Postcode=)\s*([A-Z0-9\s]+)', line, re.IGNORECASE)
                            if postcode_match:
                                current_postcode = postcode_match.group(1).strip()
                        
                        # Also check for processing lines
                        if "[processing]" in line_lower or "processing propertyid" in line_lower:
                            postcode_match = re.search(r'Postcode=([A-Z0-9\s]+)', line, re.IGNORECASE)
                            if postcode_match:
                                current_postcode = postcode_match.group(1).strip()
        
        # Initialize counts - ALWAYS query Supabase for real-time database state
        # Don't rely on log values as they may be outdated
        total_properties_needing_scrape = 0
        total_properties_scraped = 0
        total_certificates_in_db = 0
        
        # ALWAYS query Supabase for current counts (real-time database state)
        # This ensures we get accurate counts from all pages, not outdated log values
        try:
            if supabase_url and supabase_key:
                headers = {
                    "apikey": supabase_key,
                    "Authorization": f"Bearer {supabase_key}",
                    "Content-Type": "application/json"
                }
                
                # Get total properties already scraped (Always query this from Supabase)
                # Use count=exact to get total count from all pages (no 1000 limit restriction)
                url_scraped = f"{supabase_url}/rest/v1/ExtractedProperties?select=PropertyId&AlreadyDeepScrapedEPC=eq.true"
                # Don't use limit - use Range header to fetch 0-0 (just get count, no data)
                count_headers = {
                    **headers,
                    "Prefer": "count=exact",
                    "Range": "0-0"  # Request only first record (0-0 = just count, no data)
                }
                resp_scraped = requests.get(url_scraped, headers=count_headers, timeout=5)
                if resp_scraped.status_code in [200, 206]:  # 206 = Partial Content (for Range requests)
                    content_range = resp_scraped.headers.get('Content-Range', '')
                    if content_range and '/' in content_range:
                        try:
                            count_str = content_range.split('/')[-1]
                            if count_str != '*' and count_str.isdigit():
                                total_properties_scraped = int(count_str)
                        except (ValueError, IndexError):
                            # If parsing fails, fallback to 0
                            pass
                
                # ALWAYS query for properties needing scrape from Supabase (real-time count)
                # This ensures we get the current database state, not outdated log values
                # Get total properties needing scraping (AlreadyDeepScrapedEPC is false OR null)
                # Query both conditions separately since Supabase doesn't support OR in filters
                count_null = 0
                count_false = 0
                
                # Query 1: null records (AlreadyDeepScrapedEPC is null AND PropertyEPC is not null)
                # Use Range header to get count from all pages (no 1000 limit)
                url_needing_null = f"{supabase_url}/rest/v1/ExtractedProperties?select=PropertyId&AlreadyDeepScrapedEPC=is.null&PropertyEPC=not.is.null"
                count_headers_null = {
                    **headers,
                    "Prefer": "count=exact",
                    "Range": "0-0"  # Request only first record (0-0 = just count, no data)
                }
                resp_needing_null = requests.get(url_needing_null, headers=count_headers_null, timeout=5)
                if resp_needing_null.status_code in [200, 206]:
                    content_range = resp_needing_null.headers.get('Content-Range', '')
                    if content_range and '/' in content_range:
                        try:
                            count_str = content_range.split('/')[-1]
                            if count_str != '*' and count_str.isdigit():
                                count_null = int(count_str)
                            else:
                                print(f"DEBUG: Invalid count_str for null records: {count_str}")
                        except (ValueError, IndexError) as e:
                            print(f"DEBUG: Failed to parse count_null from Content-Range: {content_range}, error: {e}")
                            count_null = 0
                    else:
                        print(f"DEBUG: No Content-Range header in null query response")
                else:
                    print(f"DEBUG: Supabase query for count_null failed with status {resp_needing_null.status_code}: {resp_needing_null.text}")
                    count_null = 0
                
                # Query 2: false records (AlreadyDeepScrapedEPC is false AND PropertyEPC is not null)
                url_needing_false = f"{supabase_url}/rest/v1/ExtractedProperties?select=PropertyId&AlreadyDeepScrapedEPC=eq.false&PropertyEPC=not.is.null"
                count_headers_false = {
                    **headers,
                    "Prefer": "count=exact",
                    "Range": "0-0"
                }
                resp_needing_false = requests.get(url_needing_false, headers=count_headers_false, timeout=5)
                if resp_needing_false.status_code in [200, 206]:
                    content_range = resp_needing_false.headers.get('Content-Range', '')
                    if content_range and '/' in content_range:
                        try:
                            count_str = content_range.split('/')[-1]
                            if count_str != '*' and count_str.isdigit():
                                count_false = int(count_str)
                            else:
                                print(f"DEBUG: Invalid count_str for false records: {count_str}")
                        except (ValueError, IndexError) as e:
                            print(f"DEBUG: Failed to parse count_false from Content-Range: {content_range}, error: {e}")
                            count_false = 0
                    else:
                        print(f"DEBUG: No Content-Range header in false query response")
                else:
                    print(f"DEBUG: Supabase query for count_false failed with status {resp_needing_false.status_code}: {resp_needing_false.text}")
                    count_false = 0
                
                # Combine null and false counts to get total properties needing scrape
                total_properties_needing_scrape = count_null + count_false
                print(f"DEBUG: Properties needing scrape - null: {count_null}, false: {count_false}, total: {total_properties_needing_scrape}")
        except Exception as e:
            # If Supabase query fails, continue with log-based stats
            pass
        
        # Get recent EPC IDs (last 20, most recent first)
        recent_epc_ids = recent_epc_ids[-20:] if len(recent_epc_ids) > 20 else recent_epc_ids
        recent_epc_ids.reverse()  # Most recent first
        
        # Calculate progress
        # Total properties = already scraped + remaining + currently being processed
        total_properties = total_properties_needing_scrape + total_properties_scraped
        # Progress = (scraped + processed in this session) / total
        properties_scraped_with_current = total_properties_scraped + properties_processed
        progress_percentage = (properties_scraped_with_current / total_properties * 100) if total_properties > 0 else 0
        
        # Update remaining count to account for currently processed
        properties_remaining_actual = max(0, total_properties_needing_scrape - properties_processed)
        
        # Determine status
        if has_error:
            status = "error"
        elif is_completed:
            status = "completed"
        elif is_running:
            status = "running"
        else:
            status = "stopped"
        
        return {
            "status": status,
            "is_running": is_running,
            "is_completed": is_completed,
            "has_error": has_error,
            "pid": pid,
            "progress": {
                "properties_needing_scrape": properties_needing_scrape_from_log,
                "properties_successfully_processed": properties_successfully_processed,
             
                
              
            },
            
            "certificates": {
               
                
                "inserted_this_session": certificates_inserted,
                "updated_this_session": certificates_updated,
               
                "recent_epc_ids": recent_epc_ids  # Last 20 EPC IDs updated/inserted
            },
              # From log: "Found X unscripted properties"
            "log": {
                "last_log": last_log,
                "start_time": start_time
            }
        }
        
    except Exception as e:
        import traceback
        raise HTTPException(status_code=500, detail=f"Error getting deep scraper status: {str(e)}\n{traceback.format_exc()}")


# ======================================
# 🏡 RIGHTMOVE SCRAPER
# ======================================
@app.post("/RightmoveScraper", tags=["Rightmove Scraper"])
async def trigger_rightmove_scraper(file: UploadFile = File(...)):
    """Upload CSV file and trigger Rightmove Scraper."""
    try:
        # Check if file is CSV
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Only CSV files are allowed")
        
        # Save the uploaded CSV to rightmovescraper/data folder
        data_dir = os.path.join(RIGHTMOVE_DIR, "data")
        os.makedirs(data_dir, exist_ok=True)
        
        csv_path = os.path.join(data_dir, "Postcodes.csv")
        
        # Save the file
        async with aiofiles.open(csv_path, 'wb') as out_file:
            content = await file.read()
            await out_file.write(content)
        
        # Trigger the scraper
        script_path = os.path.join(RIGHTMOVE_DIR, "src", "main.py")
        result = trigger_scraper(script_path, "rightmove", RIGHTMOVE_DIR)
        
        # Add upload info to result
        result["uploaded_file"] = file.filename
        result["saved_as"] = "Postcodes.csv"
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




@app.get("/RightmoveScraper-validation-status/{postcode}", tags=["Rightmove Scraper"])
async def get_validation_status_for_postcode(postcode: str):
    """
    Returns the latest PropertyCreatedDate (last_entry) and count for the given outcode (postcode).
    """
   
    
    if not supabase_url or not supabase_key:
        return JSONResponse(content={
            "postcode": postcode.upper(),
            "last_entry": None,
            "count": 0,
            "error": "Supabase credentials missing. Please configure SUPABASE_URL and SUPABASE_SERVICE_KEY in .env file"
        }, status_code=500)
    
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    
    postcode_upper = postcode.upper()
    
    # Get latest PropertyCreatedDate using MAX aggregate
    # PostgREST doesn't support MAX directly, so we'll order by PropertyCreatedDate desc and get the first one
    date_url = f"{supabase_url}/rest/v1/ExtractedProperties?PropertyOutcode=eq.{postcode_upper}&order=PropertyCreatedDate.desc&limit=1&select=PropertyCreatedDate"
    
    # try:
    date_resp = requests.get(date_url, headers=headers, timeout=30)
    last_entry = None
    if date_resp.status_code == 200:
        date_data = date_resp.json()
        if date_data and len(date_data) > 0:
            last_entry = date_data[0].get('PropertyCreatedDate')
    # except Exception as e:
    #     print(f"Error fetching last_entry: {e}")
    #     last_entry = None
    
    # Get count using PostgREST count feature
    # Use select=id to only fetch IDs (more efficient) and get count from Content-Range header
    count_url = f"{supabase_url}/rest/v1/ExtractedProperties?PropertyOutcode=eq.{postcode_upper}&select=id"
    
    # try:
    count_resp = requests.get(count_url, headers={**headers, "Prefer": "count=exact"}, timeout=30)
    count = 0
    if count_resp.status_code == 200:
        # Get count from Content-Range header
        content_range = count_resp.headers.get('Content-Range', '')
        if content_range:
            # Format: "0-9/100" where 100 is the total count
            if '/' in content_range:
                total = content_range.split('/')[-1]
                if total == '*':
                    # If count is too large, count the returned items
                    count = len(count_resp.json())
                else:
                    try:
                        count = int(total)
                    except ValueError:
                        count = len(count_resp.json())
        else:
            # Fallback: count the returned items
            count = len(count_resp.json())
    # except Exception as e:
    #     print(f"Error fetching count: {e}")
    #     count = 0
    
    return {
        "postcode": postcode_upper,
        "last_entry": last_entry,
        "count": count
    }
    # except Exception as e:
    #     import traceback
    #     error_details = str(e)
    #     # Log full traceback for debugging
    #     print(f"Error in validation-status endpoint: {error_details}")
    #     print(traceback.format_exc())
        
    #     return JSONResponse(content={
    #         "postcode": postcode.upper(),
    #         "last_entry": None,
    #         "count": 0,
    #         "error": error_details
    #     }, status_code=500)


@app.get("/RightmoveScraper-completed-postcodes", tags=["Rightmove Scraper"])
async def get_completed_postcodes():
    """
    Get all completed postcodes from rightmove_scraper_postcode_status table.
    Returns postcode, completion date, and records_scraped for all completed postcodes.
    """
    if not supabase_url or not supabase_key:
        return JSONResponse(content={
            "error": "Supabase credentials missing. Please configure SUPABASE_URL and SUPABASE_SERVICE_KEY in .env file"
        }, status_code=500)
    
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    
    # Query all completed postcodes, ordered by completion date (newest first)
    url = f"{supabase_url}/rest/v1/rightmove_scraper_postcode_status?status=eq.completed&order=completed_at.desc"
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # Format the response
            completed_postcodes = []
            for item in data:
                completed_postcodes.append({
                    "postcode": item.get("postcode"),
                    "completed_at": item.get("completed_at"),
                    "records_scraped": item.get("records_scraped"),
                    "updated_at": item.get("updated_at")
                })
            
            return {
                "total": len(completed_postcodes),
                "completed_postcodes": completed_postcodes
            }
        else:
            return JSONResponse(content={
                "error": f"Failed to fetch completed postcodes: {response.status_code} - {response.text}"
            }, status_code=response.status_code)
            
    except Exception as e:
        return JSONResponse(content={
            "error": f"Error fetching completed postcodes: {str(e)}"
        }, status_code=500)


# ======================================
# 🛑 STOP SCRAPER
# ======================================
@app.post("/StopScraper", tags=["Control"])
async def stop_scraper(scraper: str = Query(..., description="Scraper name: simple, deep, image, rightmove")):
    """Stop a running scraper by name."""
    scraper = scraper.lower()
    pid_file = os.path.join(PID_DIR, f"{scraper}_scraper.pid")

    if not os.path.exists(pid_file):
        raise HTTPException(status_code=404, detail=f"No active PID for {scraper} scraper.")

    try:
        with open(pid_file, "r") as f:
            pid_str = f.read().strip()
            if not pid_str:
                os.remove(pid_file)
                raise HTTPException(status_code=404, detail=f"PID file is empty for {scraper} scraper.")
            pid = int(pid_str)

        # Check if process exists
        if not psutil.pid_exists(pid):
            # Clean up stale PID file
            os.remove(pid_file)
            raise HTTPException(status_code=404, detail=f"No running process for PID {pid}. PID file removed.")

        try:
            proc = psutil.Process(pid)
            
            # On Windows, if using shell=True, we might need to kill child processes
            # Try to find the actual Python process
            if platform.system() == "Windows":
                try:
                    # Check if this is the actual Python process
                    if "python" not in proc.name().lower() and "pythonw" not in proc.name().lower():
                        # This might be a shell process, try to find child Python processes
                        children = proc.children(recursive=True)
                        python_procs = [p for p in children if "python" in p.name().lower() or "pythonw" in p.name().lower()]
                        if python_procs:
                            # Kill the Python child processes
                            for child in python_procs:
                                try:
                                    child.terminate()
                                except:
                                    pass
                            # Also kill the parent
                            proc.terminate()
                        else:
                            proc.terminate()
                    else:
                        proc.terminate()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    # Process already gone or access denied
                    os.remove(pid_file)
                    raise HTTPException(status_code=404, detail=f"Process {pid} no longer exists.")
            else:
                # On Unix-like systems, terminate the process
                proc.terminate()

            # Wait for process to terminate
            try:
                proc.wait(timeout=8)
                status = "stopped"
            except psutil.TimeoutExpired:
                # Force kill if it didn't terminate
                if platform.system() == "Windows":
                    try:
                        os.system(f"taskkill /PID {pid} /F /T")
                    except:
                        pass
                    # Also try to kill children
                    try:
                        for child in proc.children(recursive=True):
                            try:
                                child.kill()
                            except:
                                pass
                    except:
                        pass
                else:
                    try:
                        proc.kill()
                    except:
                        pass
                status = "force_killed"

        except psutil.NoSuchProcess:
            # Process already terminated
            os.remove(pid_file)
            return {"status": "already_stopped", "scraper": scraper, "pid": pid, "message": "Process was already stopped."}
        except psutil.AccessDenied:
            # Try force kill on Windows
            if platform.system() == "Windows":
                try:
                    os.system(f"taskkill /PID {pid} /F /T")
                    status = "force_killed"
                except:
                    os.remove(pid_file)
                    raise HTTPException(status_code=500, detail=f"Access denied when trying to stop process {pid}.")
            else:
                os.remove(pid_file)
                raise HTTPException(status_code=500, detail=f"Access denied when trying to stop process {pid}.")

        # Clean up PID file
        if os.path.exists(pid_file):
            os.remove(pid_file)
            
        return {"status": status, "scraper": scraper, "pid": pid, "message": f"{scraper} scraper stopped successfully."}

    except HTTPException:
        raise
    except ValueError:
        # Invalid PID in file
        if os.path.exists(pid_file):
            os.remove(pid_file)
        raise HTTPException(status_code=400, detail=f"Invalid PID in PID file for {scraper} scraper.")
    except Exception as e:
        # Clean up PID file on any other error
        if os.path.exists(pid_file):
            try:
                os.remove(pid_file)
            except:
                pass
        raise HTTPException(status_code=500, detail=f"Error stopping scraper: {str(e)}")


# ======================================
# 🏁 MAIN ENTRY
# ======================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
