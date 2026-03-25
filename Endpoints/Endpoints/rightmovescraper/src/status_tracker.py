"""
Status tracking module for Rightmove Scraper.
Updates rightmove_scraper_postcode_status table in Supabase.
"""
import os
import requests
import uuid
from datetime import datetime
from dotenv import load_dotenv
import sys
sys.stdout.reconfigure(encoding='utf-8')

# Load environment variables
# Get the directory where this file is located
STATUS_DIR = os.path.dirname(os.path.abspath(__file__))
# Get the project root (rightmovescraper directory)
PROJECT_ROOT = os.path.dirname(STATUS_DIR)
# Get the workspace root (Endpoints directory)
WORKSPACE_ROOT = os.path.dirname(PROJECT_ROOT)

# Try loading .env from multiple locations (in order of preference)
env_loaded = False
for env_path in [
    os.path.join(PROJECT_ROOT, ".env"),  # rightmovescraper/.env
    os.path.join(WORKSPACE_ROOT, ".env"),  # Endpoints/.env
    ".env"  # Current working directory
]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        env_loaded = True
        break

if not env_loaded:
    load_dotenv()  # Fallback to default behavior

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# Cache for job_id to avoid repeated API calls
_cached_job_id = None

def get_headers():
    """Get headers for Supabase API requests."""
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

def get_or_create_job(job_id=None):
    """
    Get an existing job or find a suitable one in scraper_jobs table.
    Note: scraper_jobs uses 'id' as primary key, and rightmove_scraper_postcode_status.job_id references it.
    
    Args:
        job_id: Optional job ID (the 'id' from scraper_jobs table). If provided, will try to use existing job.
                If None, will try to find an existing running job or use the most recent one.
    
    Returns:
        str: The job_id (id from scraper_jobs) to use, or None if failed
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("❌ Supabase credentials missing. Cannot get/create job")
        return None
    
    url = f"{SUPABASE_URL}/rest/v1/scraper_jobs"
    
    # If job_id provided, check if it exists (query by 'id' column)
    if job_id:
        check_url = f"{url}?id=eq.{job_id}"
        try:
            response = requests.get(check_url, headers=get_headers(), timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data:
                    print(f"✅ Using existing job: {job_id}")
                    return job_id
        except Exception as e:
            print(f"⚠️ Error checking job {job_id}: {e}")
    
    # Try to find an existing running job
    try:
        running_jobs_url = f"{url}?status=eq.running&order=started_at.desc&limit=1"
        response = requests.get(running_jobs_url, headers=get_headers(), timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data:
                existing_id = data[0].get('id')
                print(f"✅ Using existing running job: {existing_id}")
                return existing_id
    except Exception as e:
        print(f"⚠️ Error finding running job: {e}")
    
    # Try to find the most recent job (any status)
    try:
        recent_jobs_url = f"{url}?order=created_at.desc&limit=1"
        response = requests.get(recent_jobs_url, headers=get_headers(), timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data:
                existing_id = data[0].get('id')
                print(f"✅ Using most recent job: {existing_id}")
                return existing_id
    except Exception as e:
        print(f"⚠️ Error finding recent job: {e}")
    
    # If we can't find any job, we can't proceed
    print("❌ No existing job found and cannot create new one. Please create a job manually or check database.")
    return None

def save_completed_postcode(postcode, records_scraped=None, job_id=None):
    """
    Save completed postcode to rightmove_scraper_postcode_status table.
    Uses UPSERT to update if postcode already exists, or insert if new.
    
    Args:
        postcode: The postcode that was scraped (e.g., 'BN2')
        records_scraped: Number of records scraped (optional)
        job_id: Job ID for this scraping session (optional, will generate if not provided)
    
    Returns:
        bool: True if save was successful, False otherwise
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print(f"❌ Supabase credentials missing. Cannot save completed postcode {postcode}")
        return False
    
    url = f"{SUPABASE_URL}/rest/v1/rightmove_scraper_postcode_status"
    
    # Prepare data
    completed_date = datetime.utcnow().isoformat() + "Z"
    
    # Get or create job_id (use cache to avoid repeated API calls)
    global _cached_job_id
    if job_id:
        # Use provided job_id
        _cached_job_id = job_id
    elif _cached_job_id:
        # Use cached job_id
        job_id = _cached_job_id
    else:
        # Get job_id and cache it
        job_id = get_or_create_job(job_id)
        if job_id:
            _cached_job_id = job_id
    
    if not job_id:
        print(f"❌ Failed to get/create job. Cannot save completed postcode {postcode}")
        return False
    
    data = {
        "job_id": job_id,   
        "postcode": postcode.upper(),
        "status": "completed",
        "completed_at": completed_date,
        "updated_at": completed_date
    }
    
    # Add records_scraped if provided
    if records_scraped is not None:
        data["records_scraped"] = records_scraped
    
    # Check if record already exists first, then use appropriate method
    try:
        # Check if record exists (unique constraint on job_id + postcode)
        check_url = f"{url}?postcode=eq.{postcode.upper()}&job_id=eq.{job_id}"
        check_response = requests.get(check_url, headers=get_headers(), timeout=10)
        
        record_exists = False
        if check_response.status_code == 200:
            existing_data = check_response.json()
            if existing_data and len(existing_data) > 0:
                record_exists = True
        
        if record_exists:
            # Update existing record using PATCH
            patch_response = requests.patch(
                check_url,
                headers=get_headers(),
                json=data,
                timeout=10
            )
            
            if patch_response.status_code in [200, 204]:
                print(f"✅ Updated completed postcode: {postcode} (records: {records_scraped}, completed at: {completed_date})")
                return True
            else:
                print(f"❌ Failed to update completed postcode {postcode}: {patch_response.status_code}")
                print(f"   Error: {patch_response.text[:300]}")
                return False
        else:
            # Insert new record using POST
            post_response = requests.post(
                url,
                headers=get_headers(),
                json=data,
                timeout=10
            )
            
            if post_response.status_code in [200, 201]:
                print(f"✅ Saved completed postcode: {postcode} (records: {records_scraped}, completed at: {completed_date})")
                return True
            else:
                # If POST failed, try PATCH as fallback (in case record was created between check and insert)
                patch_response = requests.patch(
                    check_url,
                    headers=get_headers(),
                    json=data,
                    timeout=10
                )
                
                if patch_response.status_code in [200, 204]:
                    print(f"✅ Saved completed postcode (via PATCH fallback): {postcode} (records: {records_scraped}, completed at: {completed_date})")
                    return True
                else:
                    print(f"❌ Failed to save completed postcode {postcode}: POST={post_response.status_code}, PATCH={patch_response.status_code}")
                    print(f"   POST Error: {post_response.text[:300]}")
                    print(f"   PATCH Error: {patch_response.text[:300]}")
                    return False
            
    except Exception as e:
        print(f"❌ Error saving completed postcode {postcode}: {e}")
        import traceback
        traceback.print_exc()
        return False

