import os
import requests
from dotenv import load_dotenv
from datetime import datetime, date
import decimal

# ============================================================
# LOAD ENV
# ============================================================

DB_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DB_DIR)
WORKSPACE_ROOT = os.path.dirname(PROJECT_ROOT)

env_loaded = False
for env_path in [
    os.path.join(PROJECT_ROOT, ".env"),
    os.path.join(WORKSPACE_ROOT, ".env"),
    ".env",
]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        env_loaded = True
        break

if not env_loaded:
    load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

# ============================================================
# LOGGING
# ============================================================

BASE_DIR = os.getcwd()
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "supabase_db.log")


def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except:
        pass


# ============================================================
# CONNECTION PLACEHOLDER
# ============================================================

def get_connection():
    """Dummy connection for compatibility with scraper."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        log("❌ Supabase credentials missing")
    return True


# ============================================================
# JSON SAFE SERIALIZER
# ============================================================

def _json_safe(value):
    if value is None:
        return None

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, decimal.Decimal):
        return float(value)

    # Convert DD/MM/YYYY → ISO
    if isinstance(value, str) and value.count("/") == 2:
        try:
            d, m, y = value.split("/")
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        except:
            return value

    return value


# ============================================================
# SQL PARSER (TABLE + WHERE FIELD)
# ============================================================

def _parse_sql(query: str):
    q = query.replace("\n", " ").strip()
    tokens_upper = q.upper().split()
    tokens_orig = q.split()

    table = None
    where_field = None

    if "INSERT" in tokens_upper and "INTO" in tokens_upper:
        table = tokens_orig[tokens_upper.index("INTO") + 1].split("(")[0]

    elif "UPDATE" in tokens_upper:
        table = tokens_orig[tokens_upper.index("UPDATE") + 1]

    elif "FROM" in tokens_upper:
        table = tokens_orig[tokens_upper.index("FROM") + 1]

    if "WHERE" in tokens_upper:
        where_field = tokens_orig[tokens_upper.index("WHERE") + 1]
        where_field = where_field.replace("=", "").replace("?", "").strip()

    return table, where_field


# ============================================================
# SELECT
# ============================================================

def run_query(conn, query, params=None):
    try:
        table, where_field = _parse_sql(query)

        if not table:
            log(f"❌ Could not detect table: {query}")
            return []

        if where_field and params:
            value = params[0]
            url = f"{SUPABASE_URL}/rest/v1/{table}?{where_field}=eq.{value}"
        else:
            url = f"{SUPABASE_URL}/rest/v1/{table}?select=*"

        resp = requests.get(url, headers=HEADERS, timeout=30)

        if resp.status_code != 200:
            log(f"❌ SELECT error {resp.status_code}: {resp.text}")
            return []

        return resp.json()

    except Exception as e:
        log(f"❌ run_query error: {e}")
        return []


# ============================================================
# INSERT + UPDATE (FIXED)
# ============================================================

def run_insert(conn, query, params=None):
    try:
        q = query.strip()
        q_upper = q.upper()

        table, where_field = _parse_sql(q)

        # ---------------------------
        # INSERT
        # ---------------------------
        if q_upper.startswith("INSERT INTO"):
            cols_raw = q.split("(", 1)[1].split(")", 1)[0]
            cols = [c.strip() for c in cols_raw.split(",")]

            record = {col: _json_safe(val) for col, val in zip(cols, params)}

            url = f"{SUPABASE_URL}/rest/v1/{table}"
            resp = requests.post(url, headers=HEADERS, json=[record])

            if resp.status_code not in (200, 201, 204):
                log(f"❌ INSERT error {resp.status_code} [{table}]: {resp.text}")
            else:
                log(f"✅ INSERTED into {table}")
            return

        # ---------------------------
        # UPDATE (FIXED ✅)
        # ---------------------------
        elif q_upper.startswith("UPDATE"):
            set_part = q.split("SET")[1].split("WHERE")[0].strip()

            set_fields = [
                part.split("=")[0].strip()
                for part in set_part.split(",")
                if "=" in part
            ]

            where_val = _json_safe(params[-1])
            set_values = params[:-1]

            payload = {
                field: _json_safe(val)
                for field, val in zip(set_fields, set_values)
            }

            url = f"{SUPABASE_URL}/rest/v1/{table}?{where_field}=eq.{where_val}"

            resp = requests.patch(url, headers=HEADERS, json=payload)

            if resp.status_code not in (200, 204):
                log(f"❌ UPDATE error {resp.status_code}: {resp.text}")
            else:
                log(
                    f"🔄 UPDATED {table} ({len(payload)} fields) "
                    f"WHERE {where_field}={where_val}"
                )
            return

        else:
            log(f"⚠️ Unsupported SQL: {query}")

    except Exception as e:
        log(f"❌ run_insert error: {e}")
