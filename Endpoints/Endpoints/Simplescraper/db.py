import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()

# Global connection variable
# conn = None

def get_db_connection():
    """Establish a single persistent connection to the MS SQL database."""
    # if conn is None or conn.closed:
    global conn
    server = os.getenv("SERVER")
    database = os.getenv("DATABASE")
    username = os.getenv("USER_NAME")
    password = os.getenv("PASSWORD") 
    driver = "ODBC Driver 17 for SQL Server"  

    conn_str = f"DRIVER={{{driver}}};SERVER={server},1433;DATABASE={database};UID={username};PWD={password}"
    
    try:
        conn = pyodbc.connect(conn_str, timeout=10)
        print("✅ Database connection established.")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        conn = None
    return conn