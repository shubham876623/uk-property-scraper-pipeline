import os
import sys
import json
import re
# import #logging  # <-- add this early
try:
    from openai.error import APIError, AuthenticationError, RateLimitError
except ImportError:
    # For newer OpenAI versions (v1.0+)
    try:
        from openai import APIError, AuthenticationError, RateLimitError
    except ImportError:
        # Fallback - define minimal exceptions
        class APIError(Exception):
            pass
        class AuthenticationError(Exception):
            pass
        class RateLimitError(Exception):
            pass

# ----------------------------------------------------------------------
# ✅ Ensure correct project root: "Endpoints"
# ----------------------------------------------------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ENDPOINTS_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))  # go up two levels

if ENDPOINTS_ROOT not in sys.path:
    sys.path.insert(0, ENDPOINTS_ROOT)

# ----------------------------------------------------------------------
# ✅ Safe print (handles Unicode emojis on Windows)
# ----------------------------------------------------------------------
def safe_print(msg: str):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode("ascii", errors="ignore").decode())

safe_print(f"[PATH FIX] Added {ENDPOINTS_ROOT} to sys.path")

# ----------------------------------------------------------------------
# ✅ Now import from internal modules
# ----------------------------------------------------------------------
from image_scraper.extractor.image_processor import extract_text_from_image
from image_scraper.database.db_connector import fetch_image_urls, update_supabase_record

os.makedirs("logs", exist_ok=True)
#logging.basicConfig(
#     filename="logs/image_scraper_runtime.log",
#     level=#logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     encoding="utf-8"
# )

# ----------------------------------------------------------------------
# 🧹 Utility: Clean model output
# ----------------------------------------------------------------------
def clean_json_output(raw_text):
    """Cleans model response to valid JSON."""
    if not raw_text:
        return "{}"
    clean = re.sub(r"^```(?:json)?", "", raw_text.strip(), flags=re.IGNORECASE | re.MULTILINE)
    clean = re.sub(r"```$", "", clean.strip(), flags=re.MULTILINE)
    json_match = re.search(r"\{.*\}", clean, re.DOTALL)
    return json_match.group(0).strip() if json_match else clean.strip()


# ----------------------------------------------------------------------
# 🧠 Main scraping logic
# ----------------------------------------------------------------------
def process_images(propertyoutcode: str):
    #logging.info(f"Started image scraper for PropertyOutcode={propertyoutcode}")
    safe_print(f"Fetching records for PropertyOutcode={propertyoutcode} ...")

    images = fetch_image_urls(propertyoutcode)
    safe_print(f"Found {len(images)} image(s) for {propertyoutcode}")
    #logging.info(f"Found {len(images)} image(s)")

    for image in images:
        property_id = None  # Initialize property_id outside try block
        try:
            # Handle None PropertyId - skip records without PropertyId
            if image[0] is None:
                safe_print(f"[SKIP] Skipping record with PropertyId=None (cannot process without PropertyId)")
                continue
            
            property_id = int(image[0])
            image_url = image[1]
            existing_rating = image[2]
            existing_current_score = image[3]
            existing_potential_score = image[4]
            postcode = image[5]

            # --- Skip invalid or duplicate records ---
            if not image_url or "None" in image_url or ".pdf" in image_url:
                msg = f"Skipping PropertyId {property_id} (invalid or PDF)"
                safe_print(msg)
                #logging.warning(msg)
                continue

            # --- Check if ALL THREE columns have valid data ---
            # Only skip if Rating, CurrentScore, AND PotentialScore are all present and valid
            rating_valid = existing_rating and existing_rating not in ["0", 0, None, "Not Available", ""]
            current_score_valid = existing_current_score is not None and existing_current_score != "" and existing_current_score != 0
            potential_score_valid = existing_potential_score is not None and existing_potential_score != "" and existing_potential_score != 0
            
            # If all three are valid, skip this property
            if rating_valid and current_score_valid and potential_score_valid:
                msg = f"Skipping PropertyId {property_id} (already has all three: Rating={existing_rating}, CurrentScore={existing_current_score}, PotentialScore={existing_potential_score})"
                safe_print(msg)
                #logging.info(msg)
                continue
            
            # If any column is missing, log what's missing and proceed to scrape
            missing_fields = []
            if not rating_valid:
                missing_fields.append("Rating")
            if not current_score_valid:
                missing_fields.append("CurrentScore")
            if not potential_score_valid:
                missing_fields.append("PotentialScore")
            
            if missing_fields:
                msg = f"PropertyId {property_id}: Missing fields detected: {', '.join(missing_fields)}. Will scrape to update."
                safe_print(msg)
                #logging.info(msg)

            safe_print(f"Processing PropertyId {property_id} → {image_url}")
            #logging.info(f"Processing PropertyId {property_id}")

            # --- Extract data from EPC image with retry logic ---
            if "media.rightmove.co.uk" in image_url:
                max_retries = 3
                retry_count = 0
                all_data_valid = False
                
                while retry_count < max_retries and not all_data_valid:
                    if retry_count > 0:
                        safe_print(f"[RETRY {retry_count}/{max_retries-1}] Re-scraping PropertyId {property_id} (missing data detected)")
                    
                    text_output = extract_text_from_image(image_url)
                    clean_output = clean_json_output(text_output)

                    try:
                        data = json.loads(clean_output)
                    except json.JSONDecodeError:
                        msg = f"JSON decode failed for PropertyId {property_id}"
                        safe_print(msg)
                        retry_count += 1
                        if retry_count < max_retries:
                            continue
                        else:
                            #logging.error(msg)
                            break

                    rating = data.get("rating") or "Not Available"
                    current_score = data.get("current_score")
                    potential_score = data.get("potential_score")
                    
                    # Validate Rating: must be a valid letter (A-G) and not "Not Available"
                    if rating == "Not Available" or rating is None or not isinstance(rating, str):
                        msg = f"PropertyId {property_id}: Rating missing or invalid: {rating}"
                        safe_print(msg)
                        retry_count += 1
                        if retry_count < max_retries:
                            continue
                        else:
                            safe_print(f"[SKIP] PropertyId {property_id}: Rating still missing after {max_retries} attempts")
                            break
                    
                    # Validate Rating format (should be A-G)
                    rating_upper = rating.upper().strip()
                    if rating_upper not in ["A", "B", "C", "D", "E", "F", "G"]:
                        msg = f"PropertyId {property_id}: Invalid rating format: {rating} (expected A-G)"
                        safe_print(msg)
                        retry_count += 1
                        if retry_count < max_retries:
                            continue
                        else:
                            safe_print(f"[SKIP] PropertyId {property_id}: Invalid rating after {max_retries} attempts")
                            break
                    
                    # Convert scores to integers, must be valid numbers
                    try:
                        current_score = int(current_score) if current_score is not None and current_score != "" else None
                    except (ValueError, TypeError):
                        current_score = None
                    
                    try:
                        potential_score = int(potential_score) if potential_score is not None and potential_score != "" else None
                    except (ValueError, TypeError):
                        potential_score = None
                    
                    # Check if CurrentScore is missing or invalid
                    if current_score is None:
                        msg = f"PropertyId {property_id}: CurrentScore missing or invalid"
                        safe_print(msg)
                        retry_count += 1
                        if retry_count < max_retries:
                            continue
                        else:
                            safe_print(f"[SKIP] PropertyId {property_id}: CurrentScore still missing after {max_retries} attempts")
                            break
                    
                    # Check if PotentialScore is missing or invalid
                    if potential_score is None:
                        msg = f"PropertyId {property_id}: PotentialScore missing or invalid"
                        safe_print(msg)
                        retry_count += 1
                        if retry_count < max_retries:
                            continue
                        else:
                            safe_print(f"[SKIP] PropertyId {property_id}: PotentialScore still missing after {max_retries} attempts")
                            break
                    
                    # All three values are valid - mark as complete
                    all_data_valid = True
                    
                    # Update database only when all 3 values are valid
                    update_supabase_record(property_id, rating_upper, current_score, potential_score)
                    msg = f"✅ Updated {property_id}: Rating={rating_upper}, Current={current_score}, Potential={potential_score}"
                    safe_print(msg)
                    #logging.info(msg)
                
                # If we exhausted retries and still don't have all data, skip
                if not all_data_valid:
                    msg = f"[SKIP] PropertyId {property_id}: Missing required data after {max_retries} attempts. Required: Rating, CurrentScore, PotentialScore"
                    safe_print(msg)
                    continue

        except (APIError, AuthenticationError, RateLimitError) as e:
            property_id_str = str(property_id) if property_id is not None else "unknown"
            msg = f"OpenAI API error for PropertyId {property_id_str}: {str(e)}"
            safe_print(msg)
            #logging.error(msg)
            continue

        except ValueError as e:
            # Handle ValueError from image processor (may contain Unicode)
            property_id_str = str(property_id) if property_id is not None else "unknown"
            error_msg = str(e).encode("ascii", errors="ignore").decode("ascii")
            msg = f"Error processing PropertyId {property_id_str}: {error_msg}"
            safe_print(msg)
            #logging.error(msg)
            continue

        except Exception as e:
            # Handle any other exceptions safely
            property_id_str = str(property_id) if property_id is not None else "unknown"
            error_msg = str(e).encode("ascii", errors="ignore").decode("ascii")
            msg = f"Error processing PropertyId {property_id_str}: {error_msg}"
            safe_print(msg)
            #logging.error(msg)
            continue

    safe_print("✅ Completed image scraper run.")
    #logging.info("Completed image scraper run.")


# ----------------------------------------------------------------------
# 🚀 Entry point
# ----------------------------------------------------------------------
if __name__ == "__main__":
    try:
        # Try command-line argument first, then environment variable
        propertyoutcode = sys.argv[1] if len(sys.argv) > 1 else os.getenv("IMAGE_SCRAPER_OUTCODE")
        if not propertyoutcode:
            safe_print("Error: No PropertyOutcode provided. Example: python main.py SG1")
            safe_print("Or set IMAGE_SCRAPER_OUTCODE environment variable.")
            sys.exit(1)

        safe_print(f"Image scraper starting for PropertyOutcode={propertyoutcode}")
        process_images(propertyoutcode)

    except Exception as e:
        #logging.error(f"Critical error in main.py: {e}")
        safe_print(f"Error: {e}")
        import traceback
        traceback.print_exc()
