import re
from decimal import Decimal
from datetime import datetime
import sys
sys.stdout.reconfigure(encoding='utf-8')

from db.db import get_connection, run_query, run_insert
from datetime import date
# --------------------------------------------------------------------
# --- HELPERS --------------------------------------------------------
# --------------------------------------------------------------------
scrape_date = date.today().isoformat() 
def _clean_price(v):
    if v is None:
        return None
    if isinstance(v, (int, float, Decimal)):
        return Decimal(str(v))
    s = str(v)
    s = re.sub(r"[^\d.]", "", s)
    return Decimal(s) if s else None

def _str_or_none(v):
    return None if v is None else str(v).strip()


def _sanitize_history_price(hist_price, current_listing_price):
    """If hist_price looks like day-of-month (1-31) or too small (< 100), use current listing price."""
    if current_listing_price is None:
        return _str_or_none(hist_price)
    try:
        p = int(str(hist_price).replace(",", "").strip())
        if 1 <= p <= 31 or p < 100:
            return _str_or_none(current_listing_price)
    except (ValueError, TypeError):
        pass
    return _str_or_none(hist_price)


def _strip_about_agent_name(v):
    """Remove Rightmove's leading 'About' from agent name (e.g. 'AboutHunters, Sedgley' -> 'Hunters, Sedgley')."""
    if v is None:
        return None
    s = str(v).strip()
    if s and s.lower().startswith("about"):
        s = s[5:].lstrip()
    return s if s else None

def _split_postcode(postcode):
    """
    Safely split a UK postcode into Outcode and Incode.
    Examples:
        'AL3 4EE' → ('AL3', '4EE')
        'SW1A1AA' → ('SW1A', '1AA')
        None or invalid → ('UNKNOWN', 'UNKNOWN')
    """
    if not postcode or not isinstance(postcode, str):
        return "UNKNOWN", "UNKNOWN"

    postcode = postcode.strip().upper()
    match = re.match(r"^([A-Z]{1,2}\d[A-Z\d]?)\s*(\d[A-Z]{2})$", postcode)
    if match:
        return match.group(1), match.group(2)

    # Try simple space split fallback
    parts = postcode.split()
    if len(parts) == 2:
        return parts[0], parts[1]

    return "UNKNOWN", "UNKNOWN"

# --------------------------------------------------------------------
# --- MAIN UPSERT FUNCTION -------------------------------------------
# --------------------------------------------------------------------

def appendingintodb(postcode, listing):
    """Upsert property into ExtractedProperties table."""
    property_ref_no = listing['PropertyReference']
    print(f"📥 Appending to DB for {postcode} and property {property_ref_no}")

    # Skip commercial listings
    if 'status' in listing and listing['status'] and 'commercial' in listing['status'].lower():
        print("➡️ Skipped commercial listing")
        return property_ref_no

    conn = get_connection()

    # Safely extract postcode components
    full_postcode = listing.get('PropertyPostCode')
    outcode, incode = _split_postcode(full_postcode)
    if outcode == "UNKNOWN":
        print(f"⚠️ Warning: Could not extract Outcode from '{full_postcode}' for {property_ref_no}")
    listing['PropertyOutcode'] = outcode
    listing['PropertyIncode'] = incode

    # Fetch any existing record for this property
    prop = run_query(
        conn,
        """
        SELECT PropertyPrice, PropertyType, PropertyEPC, PropertyEPC2
        FROM ExtractedProperties
        WHERE PropertyReferanceNumber = ?
        """,
        [property_ref_no],
    )
    
    # Also get the latest status from marketing history to ensure we have the most recent status
    # This is more accurate than just using PropertyType, which might be outdated
    latest_status_history = run_query(conn, """
        SELECT TOP 1 PropertyStatus
        FROM ExtractedPropertyMarketingHistory
        WHERE PropertyReferenceNumber = ?
        ORDER BY DateStatusChanged DESC
    """, [property_ref_no]) if prop else None

    price_in = listing.get('Price')
    price_in_clean = _clean_price(price_in)
    created_date = listing.get('PropertyCreatedDate')
    # Sold price history (Land Registry / transaction history API) for JSONB column
    sold_price_history = listing.get('soldPropertyTransactions')
    if not isinstance(sold_price_history, list):
        sold_price_history = []

    # Normalize created_date
    if isinstance(created_date, datetime):
        created_date = created_date.isoformat()
    elif not created_date:
        created_date = datetime.utcnow().isoformat()

    # --------------------------------------------------------------
    # INSERT NEW PROPERTY
    # --------------------------------------------------------------
    if not prop:
        print(f"🆕 Inserting new property {property_ref_no}")
        run_insert(conn, """
            INSERT INTO ExtractedProperties (
                AgentUnsubscribedFromEPCEmail, IsVerifyAddressProcessed, PropertyPrice, PropertyNumberOfBedRooms,
                PropertyIsNewBuild, PropertyTitle, PropertyLandType, PropertyAddress, PropertyDescription,
                PropertyImage, PropertyKeyFeatures, PropertyReferanceNumber, PropertyId,PropertyAgentName, PropertyAgentAddress,
                PropertyAgentPhoneNumber, PropertyType, PropertyLongitude, PropertyLatitude, PropertyPostCode,
                PropertyCreatedDate, RightmoveURL, PropertyBathrooms, PropertyCouncilTaxBand, PropertyTenure,
                PropertyOutcode, PropertyIncode, PropertyAgentProfileUrl, PropertyEPC, PropertyEPC2, AgentImageURL, RightmoveCheckedByScraper,
                SoldPriceHistory
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, [
            0, 0, _str_or_none(price_in), listing.get('Bedrooms'),
            listing.get('propertyisnewbuild'), listing.get('propertytitle'),
            listing.get('propertylandtype'), listing.get('PropertyAddress'),
            listing.get('PropertyDescription'), listing.get('PropertyImage'),
            listing.get('PropertyFeatures'), property_ref_no,property_ref_no,
            _strip_about_agent_name(listing.get('AgentName')), listing.get('AgentAddress'),
            listing.get('AgentPhoneNumber'), listing.get('status'),
            listing.get('Longitude'), listing.get('Latitude'),
            full_postcode, created_date,
            listing.get('PropertyURL'), listing.get('Bathrooms'),
            listing.get('CouncilTaxBand'), listing.get('Tenure'),
            outcode, incode,
            listing.get('AgentProfileUrl'), listing.get('PropertyEpc1'),
            listing.get('PropertyEpc2'), listing.get('AgentImageURL'), scrape_date,
            sold_price_history
        ])

        # Insert historical price records if available FIRST
        # This ensures we have the correct dates from Rightmove before inserting current price
        price_history = listing.get('PriceHistory', [])
        current_price_in_history = False
        if price_history and isinstance(price_history, list):
            print(f"📊 Found {len(price_history)} price history records for {property_ref_no}")
            for hist_item in price_history:
                if isinstance(hist_item, dict):
                    hist_price = hist_item.get('price')
                    hist_date = hist_item.get('date')
                    if hist_price and hist_date:
                        # Normalize date - ensure consistent format
                        if isinstance(hist_date, datetime):
                            hist_date_str = hist_date.isoformat()
                        elif isinstance(hist_date, str):
                            # Try to parse and normalize the date string
                            try:
                                # Try common formats
                                date_part = hist_date.split('T')[0].split()[0] if hist_date else hist_date
                                for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y", "%d-%m-%Y"]:
                                    try:
                                        parsed_date = datetime.strptime(date_part, fmt)
                                        hist_date_str = parsed_date.strftime("%Y-%m-%d")
                                        break
                                    except Exception:
                                        continue
                                else:
                                    hist_date_str = str(hist_date)[:10]
                            except Exception:
                                hist_date_str = str(hist_date)[:10] if hist_date else str(hist_date)
                        else:
                            hist_date_str = str(hist_date)
                        
                        # Check if current price matches this history entry
                        if _clean_price(hist_price) == price_in_clean:
                            current_price_in_history = True
                        
                        hist_price_use = _sanitize_history_price(hist_price, price_in)
                        # run_query only supports single WHERE param; fetch all rows for this property and check in Python
                        existing_rows = run_query(conn, """
                            SELECT PropertyReferenceNumber, PropertyPrice, PropertyCreateDate
                            FROM ExtractedPropertiesPricingHistory
                            WHERE PropertyReferenceNumber = ?
                        """, [property_ref_no])
                        already_exists = any(
                            str(r.get("PropertyPrice")) == str(hist_price_use)
                            and (str(r.get("PropertyCreateDate") or "")[:10] == hist_date_str[:10])
                            for r in (existing_rows or [])
                        )
                        if not already_exists:
                            run_insert(conn, """
                                INSERT INTO ExtractedPropertiesPricingHistory
                                (PropertyReferenceNumber, PropertyPrice, PropertyCreateDate)
                                VALUES (?, ?, ?)
                            """, [property_ref_no, hist_price_use, hist_date_str])
        
        # Insert current price into pricing history ONLY if not already in history
        # Use current date (when scraper runs) for the current price entry
        # NOT PropertyCreatedDate, which is when property was added to Rightmove
        if price_in_clean is not None and not current_price_in_history:
            current_price_date = datetime.utcnow().isoformat()
            current_price_date_short = current_price_date[:10]
            existing_rows = run_query(conn, """
                SELECT PropertyPrice, PropertyCreateDate
                FROM ExtractedPropertiesPricingHistory
                WHERE PropertyReferenceNumber = ?
            """, [property_ref_no])
            already_has_current = any(
                str(r.get("PropertyPrice")) == str(price_in)
                and (str(r.get("PropertyCreateDate") or "")[:10] == current_price_date_short)
                for r in (existing_rows or [])
            )
            if not already_has_current:
                run_insert(conn, """
                    INSERT INTO ExtractedPropertiesPricingHistory
                    (PropertyReferenceNumber, PropertyPrice, PropertyCreateDate)
                    VALUES (?, ?, ?)
                """, [property_ref_no, _str_or_none(price_in), current_price_date])

        if listing.get('status'):
            run_insert(conn, """
                INSERT INTO ExtractedPropertyMarketingHistory
                (PropertyReferenceNumber, PropertyStatus, DateStatusChanged)
                VALUES (?, ?, ?)
            """, [property_ref_no, listing['status'], created_date])

        # Insert historical price records if available (for existing properties too)
        price_history = listing.get('PriceHistory', [])
        if price_history and isinstance(price_history, list):
            print(f"📊 Found {len(price_history)} price history records for {property_ref_no}")
            for hist_item in price_history:
                if isinstance(hist_item, dict):
                    hist_price = hist_item.get('price')
                    hist_date = hist_item.get('date')
                    if hist_price and hist_date:
                        # Normalize date - ensure consistent format
                        if isinstance(hist_date, datetime):
                            hist_date_str = hist_date.isoformat()
                        elif isinstance(hist_date, str):
                            # Try to parse and normalize the date string
                            try:
                                # Try common formats
                                date_part = hist_date.split('T')[0].split()[0] if hist_date else hist_date
                                for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y", "%d-%m-%Y"]:
                                    try:
                                        parsed_date = datetime.strptime(date_part, fmt)
                                        hist_date_str = parsed_date.strftime("%Y-%m-%d")
                                        break
                                    except Exception:
                                        continue
                                else:
                                    hist_date_str = str(hist_date)[:10]
                            except Exception:
                                hist_date_str = str(hist_date)[:10] if hist_date else str(hist_date)
                        else:
                            hist_date_str = str(hist_date)
                        
                        hist_price_use = _sanitize_history_price(hist_price, price_in)
                        existing_rows = run_query(conn, """
                            SELECT PropertyReferenceNumber, PropertyPrice, PropertyCreateDate
                            FROM ExtractedPropertiesPricingHistory
                            WHERE PropertyReferenceNumber = ?
                        """, [property_ref_no])
                        already_exists = any(
                            str(r.get("PropertyPrice")) == str(hist_price_use)
                            and (str(r.get("PropertyCreateDate") or "")[:10] == hist_date_str[:10])
                            for r in (existing_rows or [])
                        )
                        if not already_exists:
                            run_insert(conn, """
                                INSERT INTO ExtractedPropertiesPricingHistory
                                (PropertyReferenceNumber, PropertyPrice, PropertyCreateDate)
                                VALUES (?, ?, ?)
                            """, [property_ref_no, hist_price_use, hist_date_str])

        # Insert historical marketing records if available
        marketing_history = listing.get('MarketingHistory', [])
        if marketing_history and isinstance(marketing_history, list):
            print(f"📊 Found {len(marketing_history)} marketing history records for {property_ref_no}")
            for hist_item in marketing_history:
                if isinstance(hist_item, dict):
                    hist_status = hist_item.get('status')
                    hist_date = hist_item.get('date')
                    if hist_status and hist_date:
                        # Normalize date
                        if isinstance(hist_date, datetime):
                            hist_date_str = hist_date.isoformat()
                        else:
                            hist_date_str = str(hist_date)
                        mkt_rows = run_query(conn, """
                            SELECT PropertyStatus, DateStatusChanged
                            FROM ExtractedPropertyMarketingHistory
                            WHERE PropertyReferenceNumber = ?
                        """, [property_ref_no])
                        mkt_exists = any(
                            str(r.get("PropertyStatus")) == str(hist_status)
                            and (str(r.get("DateStatusChanged") or "")[:10] == hist_date_str[:10])
                            for r in (mkt_rows or [])
                        )
                        if not mkt_exists:
                            run_insert(conn, """
                                INSERT INTO ExtractedPropertyMarketingHistory
                                (PropertyReferenceNumber, PropertyStatus, DateStatusChanged)
                                VALUES (?, ?, ?)
                            """, [property_ref_no, _str_or_none(hist_status), hist_date_str])

    # --------------------------------------------------------------
    # UPDATE EXISTING PROPERTY
    # --------------------------------------------------------------
    else:
        print(f"\n{'='*80}")
        print(f"🔄 UPDATING EXISTING PROPERTY: {property_ref_no}")
        print(f"{'='*80}")
        
        row = prop[0]
        # Handle dict or tuple return types
        if isinstance(row, dict):
            prev_price_raw = row.get("PropertyPrice")
            prev_type_raw = row.get("PropertyType")
            prev_epc = row.get("PropertyEPC")
            prev_epc2 = row.get("PropertyEPC2")
        else:
            # Take only first four values, ignore extra columns
            prev_price_raw, prev_type_raw, prev_epc, prev_epc2 = (list(row) + [None, None, None, None])[:4]

        # Get the most recent status from history (more accurate than PropertyType)
        # PropertyType might be outdated, so check history first
        prev_status_from_history = None
        if latest_status_history:
            if isinstance(latest_status_history[0], dict):
                prev_status_from_history = latest_status_history[0].get("PropertyStatus")
            else:
                prev_status_from_history = latest_status_history[0][0] if latest_status_history[0] else None
        
        # Use status from history if available, otherwise fall back to PropertyType
        prev_status = prev_status_from_history if prev_status_from_history else prev_type_raw
        prev_status_lower = str(prev_status or "").lower().strip()

        prev_price_clean = _clean_price(prev_price_raw)
        
        print(f"📊 Previous Status (from DB): '{prev_status or 'None'}' (PropertyType: '{prev_type_raw or 'None'}')")
        print(f"📊 Previous Price: {prev_price_raw}")
        
        # --- UPDATE ALL FIELDS with latest data from Rightmove ---
        # This ensures existing properties are fully synchronized with current Rightmove data
        # Updates all fields including: price, status, bedrooms, title, description, images, agent details, location, etc.
        # Note: Status (PropertyType) will be handled separately below for special cases (Removed status)
        # But we update it here too to ensure it's always in sync for normal cases
        new_status_for_update = _str_or_none(listing.get('status'))
        new_price_from_rightmove = _str_or_none(price_in)
        
        print(f"📥 New Status (from Rightmove): '{new_status_for_update or 'None'}'")
        print(f"📥 New Price (from Rightmove): {new_price_from_rightmove}")
        
        # Ensure PropertyType is always updated, even if status is None (use previous status as fallback)
        if not new_status_for_update:
            new_status_for_update = prev_status if prev_status else "For Sale"
            print(f"⚠️ Warning: Status is None for {property_ref_no}, using previous status: {new_status_for_update}")
        
        print(f"🔄 Executing comprehensive UPDATE for {property_ref_no}...")
        print(f"   - PropertyType will be set to: '{new_status_for_update}'")
        print(f"   - PropertyPrice will be set to: {new_price_from_rightmove}")
        
        run_insert(conn, """
            UPDATE ExtractedProperties SET
                PropertyPrice = ?,
                PropertyNumberOfBedRooms = ?,
                PropertyIsNewBuild = ?,
                PropertyTitle = ?,
                PropertyLandType = ?,
                PropertyAddress = ?,
                PropertyDescription = ?,
                PropertyImage = ?,
                PropertyKeyFeatures = ?,
                PropertyAgentName = ?,
                PropertyAgentAddress = ?,
                PropertyAgentPhoneNumber = ?,
                PropertyType = ?,
                PropertyLongitude = ?,
                PropertyLatitude = ?,
                PropertyPostCode = ?,
                RightmoveURL = ?,
                PropertyBathrooms = ?,
                PropertyCouncilTaxBand = ?,
                PropertyTenure = ?,
                PropertyOutcode = ?,
                PropertyIncode = ?,
                PropertyAgentProfileUrl = ?,
                PropertyEPC = ?,
                PropertyEPC2 = ?,
                AgentImageURL = ?,
                RightmoveCheckedByScraper = ?,
                PropertyId = ?,
                SoldPriceHistory = ?
            WHERE PropertyReferanceNumber = ?
        """, [
            _str_or_none(price_in),
            listing.get('Bedrooms'),
            listing.get('propertyisnewbuild'),
            listing.get('propertytitle'),
            listing.get('propertylandtype'),
            listing.get('PropertyAddress'),
            listing.get('PropertyDescription'),
            listing.get('PropertyImage'),
            listing.get('PropertyFeatures'),
            _strip_about_agent_name(listing.get('AgentName')),
            listing.get('AgentAddress'),
            listing.get('AgentPhoneNumber'),
            new_status_for_update,  # PropertyType - will be overridden by status logic if needed
            listing.get('Longitude'),
            listing.get('Latitude'),
            full_postcode,
            listing.get('PropertyURL'),
            listing.get('Bathrooms'),
            listing.get('CouncilTaxBand'),
            listing.get('Tenure'),
            outcode,
            incode,
            listing.get('AgentProfileUrl'),
            listing.get('PropertyEpc1'),
            listing.get('PropertyEpc2'),
            listing.get('AgentImageURL'),
            scrape_date,
            property_ref_no,
            sold_price_history,
            property_ref_no
        ])
        
        # print(f"✅ Comprehensive UPDATE completed for listing{listing}")
        # print(f"   ✓ PropertyType updated to: '{new_status_for_update}'")
        # print(f"   ✓ PropertyPrice updated to: {new_price_from_rightmove}")
        # print(f"   ✓ All other fields synchronized with Rightmove data")

        # Insert/append price history from Rightmove (never overwrite existing rows)
        # Only add new (price, date) combinations so reductions are preserved
        price_history = listing.get('PriceHistory', [])
        if price_history and isinstance(price_history, list):
            print(f"📊 Found {len(price_history)} price history records for {property_ref_no} (existing property)")
            for hist_item in price_history:
                if isinstance(hist_item, dict):
                    hist_price = hist_item.get('price')
                    hist_date = hist_item.get('date')
                    if hist_price and hist_date:
                        if isinstance(hist_date, datetime):
                            hist_date_str = hist_date.isoformat()
                        elif isinstance(hist_date, str):
                            try:
                                date_part = hist_date.split('T')[0].split()[0] if hist_date else hist_date
                                for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y", "%d-%m-%Y"]:
                                    try:
                                        parsed_date = datetime.strptime(date_part, fmt)
                                        hist_date_str = parsed_date.strftime("%Y-%m-%d")
                                        break
                                    except Exception:
                                        continue
                                else:
                                    hist_date_str = str(hist_date)[:10]
                            except Exception:
                                hist_date_str = str(hist_date)[:10] if hist_date else str(hist_date)
                        else:
                            hist_date_str = str(hist_date)
                        hist_price_use = _sanitize_history_price(hist_price, price_in)
                        existing_rows = run_query(conn, """
                            SELECT PropertyReferenceNumber, PropertyPrice, PropertyCreateDate
                            FROM ExtractedPropertiesPricingHistory
                            WHERE PropertyReferenceNumber = ?
                        """, [property_ref_no])
                        already_exists = any(
                            str(r.get("PropertyPrice")) == str(hist_price_use)
                            and (str(r.get("PropertyCreateDate") or "")[:10] == hist_date_str[:10])
                            for r in (existing_rows or [])
                        )
                        if not already_exists:
                            run_insert(conn, """
                                INSERT INTO ExtractedPropertiesPricingHistory
                                (PropertyReferenceNumber, PropertyPrice, PropertyCreateDate)
                                VALUES (?, ?, ?)
                            """, [property_ref_no, hist_price_use, hist_date_str])

    # Insert historical marketing records if available (for existing properties too)
    marketing_history = listing.get('MarketingHistory', [])
    if marketing_history and isinstance(marketing_history, list):
        print(f"📊 Found {len(marketing_history)} marketing history records for {property_ref_no}")
        for hist_item in marketing_history:
            if isinstance(hist_item, dict):
                hist_status = hist_item.get('status')
                hist_date = hist_item.get('date')
                if hist_status and hist_date:
                    # Normalize date
                    if isinstance(hist_date, datetime):
                        hist_date_str = hist_date.isoformat()
                    else:
                        hist_date_str = str(hist_date)
                    mkt_rows = run_query(conn, """
                        SELECT PropertyStatus, DateStatusChanged
                        FROM ExtractedPropertyMarketingHistory
                        WHERE PropertyReferenceNumber = ?
                    """, [property_ref_no])
                    mkt_exists = any(
                        str(r.get("PropertyStatus")) == str(hist_status)
                        and (str(r.get("DateStatusChanged") or "")[:10] == hist_date_str[:10])
                        for r in (mkt_rows or [])
                    )
                    if not mkt_exists:
                        run_insert(conn, """
                            INSERT INTO ExtractedPropertyMarketingHistory
                            (PropertyReferenceNumber, PropertyStatus, DateStatusChanged)
                            VALUES (?, ?, ?)
                        """, [property_ref_no, _str_or_none(hist_status), hist_date_str])

    print(f"✅ Done processing property {property_ref_no} ({postcode})")

    
        # Step 4: ✅ Always update RightmoveCheckedByScraper timestamp
    run_insert(
    conn,
    "UPDATE ExtractedProperties SET RightmoveCheckedByScraper = ? WHERE PropertyReferanceNumber = ?",
    [scrape_date, property_ref_no],
    )
    return property_ref_no


