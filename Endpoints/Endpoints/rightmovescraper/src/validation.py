# rightmovescraper/src/validation.py
import sys
import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from db.db import get_connection, run_query, run_insert, log

sys.stdout.reconfigure(encoding='utf-8')

# Load Supabase credentials for direct API calls
# Get the directory where this file is located
VALIDATION_DIR = os.path.dirname(os.path.abspath(__file__))
# Get the project root (rightmovescraper directory)
PROJECT_ROOT = os.path.dirname(VALIDATION_DIR)
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

# If no .env found, try default load_dotenv() behavior
if not env_loaded:
    load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
SUPABASE_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json"
}

def validate_scraped_outcode(expected_postcode, expected_identifier):
    """
    Simple validation: Check if properties with the expected outcode exist in database.
    
    Args:
        expected_postcode: The outcode we expected to scrape (e.g., 'DY3')
        expected_identifier: The Rightmove identifier used from CSV (e.g., '5E37')
    
    Returns:
        dict: Validation results with success status
    """
    log(f"🔍 Validating {expected_postcode} (identifier: {expected_identifier})")
    
    conn = get_connection()
    
    # Simple query: Check if any properties exist with the expected outcode
    matching_properties = run_query(conn, """
        SELECT PropertyReferanceNumber, PropertyOutcode, PropertyPostCode
        FROM ExtractedProperties
        WHERE PropertyOutcode = ?
        LIMIT 10
    """, [expected_postcode.upper()])
    
    matching_count = len(matching_properties)
    
    # Determine status
    if matching_count > 0:
        validation_status = "SUCCESS"
        success = True
        message = f"✅ SUCCESS: Found {matching_count} properties with outcode {expected_postcode}"
    else:
        validation_status = "MISMATCH"
        success = False
        message = f"❌ ERROR: No properties found with outcode {expected_postcode}. The identifier {expected_identifier} may be incorrect!"
    
    log(message)
    
    # Store validation result
    log_validation_result(
        expected_postcode=expected_postcode,
        expected_identifier=expected_identifier,
        status=validation_status,
        properties_found=matching_count,
        matching_properties=matching_count,
        mismatched_properties=0,
        mismatch_details=None
    )
    
    return {
        "success": success,
        "status": validation_status,
        "expected_postcode": expected_postcode,
        "expected_identifier": expected_identifier,
        "properties_found": matching_count,
        "matching_properties": matching_count,
        "mismatched_properties": 0,
        "message": message
    }


def log_validation_result(expected_postcode, expected_identifier, status, 
                         properties_found, matching_properties, mismatched_properties,
                         mismatch_details=None):
    """
    Logs validation results to the database for tracking and reporting.
    Creates a validation log table if it doesn't exist.
    """
    conn = get_connection()
    
    try:
        # Note: Supabase table should be created manually or via migration
        # Table: rightmove_scraper_validation_log (PostgREST expects lowercase)
        # Columns: id, expected_postcode, expected_identifier, validation_status,
        #          properties_found, matching_properties, mismatched_properties,
        #          mismatch_details, validation_timestamp
        validation_timestamp = datetime.utcnow().isoformat()

        run_insert(conn, """
            INSERT INTO rightmove_scraper_validation_log
            (expected_postcode, expected_identifier, validation_status, properties_found,
             matching_properties, mismatched_properties, mismatch_details, validation_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            expected_postcode,
            expected_identifier,
            status,
            properties_found,
            matching_properties,
            mismatched_properties,
            mismatch_details,
            validation_timestamp
        ])
        
        log(f"📊 Validation result logged to database for {expected_postcode}")
        
    except Exception as e:
        log(f"⚠️ Failed to log validation result to database: {e}")
        # Don't fail the scrape if logging fails


def get_recent_validations(limit=20):
    """
    Retrieves recent validation results from the database.
    
    Args:
        limit: Maximum number of validation records to retrieve
    
    Returns:
        list: List of validation records
    """
    conn = get_connection()
    
    try:
        # Query Supabase directly for recent validations
        url = f"{SUPABASE_URL}/rest/v1/rightmove_scraper_validation_log?order=validation_timestamp.desc&limit={limit}&select=expected_postcode,expected_identifier,validation_status,properties_found,matching_properties,mismatched_properties,mismatch_details,validation_timestamp"
        
        response = requests.get(url, headers=SUPABASE_HEADERS, timeout=30)
        
        if response.status_code != 200:
            log(f"❌ Failed to query recent validations: {response.status_code} - {response.text}")
            return []
        
        data = response.json()
        return data
        
    except Exception as e:
        log(f"❌ Error retrieving validation results: {e}")
        return []


def get_failed_validations(limit=10):
    """
    Retrieves only failed validation results (MISMATCH or NO_DATA status).
    
    Args:
        limit: Maximum number of failed validation records to retrieve
    
    Returns:
        list: List of failed validation records
    """
    conn = get_connection()
    
    try:
        # Query Supabase directly for failed validations
        # PostgREST format: validation_status=in.(value1,value2,value3)
        url = f"{SUPABASE_URL}/rest/v1/rightmove_scraper_validation_log?validation_status=in.(MISMATCH,NO_DATA,PARTIAL_MATCH)&order=validation_timestamp.desc&limit={limit}&select=expected_postcode,expected_identifier,validation_status,properties_found,matching_properties,mismatched_properties,mismatch_details,validation_timestamp"
        
        response = requests.get(url, headers=SUPABASE_HEADERS, timeout=30)
        
        if response.status_code != 200:
            log(f"❌ Failed to query failed validations: {response.status_code} - {response.text}")
            return []
        
        data = response.json()
        return data
        
    except Exception as e:
        log(f"❌ Error retrieving failed validations: {e}")
        return []


def get_validation_for_postcode(postcode):
    """
    Retrieves the most recent validation result for a specific postcode.
    
    Args:
        postcode: The postcode/outcode to look up (e.g., 'DY3')
    
    Returns:
        dict: The most recent validation record for the postcode, or None if not found
    """
    try:
        # Query Supabase directly for the specific postcode
        # PostgREST format: expected_postcode=eq.value
        postcode_upper = postcode.upper()
        url = f"{SUPABASE_URL}/rest/v1/rightmove_scraper_validation_log?expected_postcode=eq.{postcode_upper}&order=validation_timestamp.desc&limit=1&select=expected_postcode,expected_identifier,validation_status,properties_found,matching_properties,mismatched_properties,mismatch_details,validation_timestamp"
        
        response = requests.get(url, headers=SUPABASE_HEADERS, timeout=30)
        
        if response.status_code != 200:
            log(f"❌ Failed to query validation for {postcode}: {response.status_code} - {response.text}")
            return None
        
        data = response.json()
        if data and len(data) > 0:
            return data[0]
        return None
        
    except Exception as e:
        log(f"❌ Error retrieving validation for {postcode}: {e}")
        return None


