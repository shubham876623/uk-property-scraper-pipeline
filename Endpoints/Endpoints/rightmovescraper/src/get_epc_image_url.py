import requests
# Add once at the top of main.py
import sys
sys.stdout.reconfigure(encoding='utf-8')

def get_valid_epc_url(original_url):
    # Build the base of the URL
    base_url, filename = original_url.rsplit('/', 1)
    parts = filename.split('_')

    if len(parts) >= 3:
        parts[-3] = "EPCGRAPH"
        parts[-2] = "00"
        base_filename = "_".join(parts[:-1]) + "_0000"

        # Try both extensions
        for ext in ['.png', '.gif']:
            epc_url = f"{base_url}/{base_filename}{ext}"
            try:
                response = requests.head(epc_url, timeout=5)
                if response.status_code == 200:
                    return epc_url  # Valid EPC found
            except requests.RequestException:
                continue  # Skip if request fails

    return None  # No valid EPC found



