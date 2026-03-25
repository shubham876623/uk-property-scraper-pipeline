import re

def extract_address_fields(address: str) -> dict:
    """
    Extract HouseNumber, Street, City, Country from Address String
    """

    parts = [x.strip() for x in address.split(",")]

    result = {
        "HouseNumber": None,
        "Street": None,
        "City": None,
        "Country": "United Kingdom"
    }

    if len(parts) >= 3:
        first_part = parts[0]

        # Check for Flat or Apartment
        flat_match = re.search(r'\bFlat\s*(\d+)', first_part, re.IGNORECASE)
        if flat_match:
            result["HouseNumber"] = flat_match.group(1)
            result["Street"] = parts[1]  # Take next part as street name
        else:
            # Check if starts with number
            if re.match(r'^\d+[A-Za-z]?', first_part):
                tokens = first_part.split(" ", 1)
                result["HouseNumber"] = tokens[0]
                result["Street"] = tokens[1] if len(tokens) > 1 else None
            else:
                result["Street"] = first_part

        # City always second last part
        result["City"] = parts[-2].title()

    else:
        result["Street"] = parts[0]

    return result
