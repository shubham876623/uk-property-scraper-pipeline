import re

def parse_unstructured_address(text):
    text = text.strip()

    # Step 1: Extract UK postcode (assumes at end)
    postcode_match = re.search(r"([A-Z]{1,2}\d{1,2}\s?\d[A-Z]{2})$", text)
    postcode = postcode_match.group(1) if postcode_match else None
    text = text.replace(postcode, "") if postcode else text

    # Step 2: Extract uppercase city before postcode
    city_match = re.search(r"([A-Z\s]+)$", text.strip())
    city = city_match.group(1).strip().title() if city_match else None
    text = text.replace(city.upper(), "") if city else text

    # Step 3: Extract house number and street
    match = re.match(r"^(\d+)[,\s]*(.*)$", text.strip())
    if match:
        house_number = match.group(1)
        street = match.group(2).strip()
    else:
        house_number = None
        street = text.strip()

    return {
        "HouseNumber": house_number,
        "Street": street,
        "City": city,
        "Postcode": postcode
    }
