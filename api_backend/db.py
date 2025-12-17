import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "tourism_db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "tourism_dw")
DB_USER = os.getenv("DB_USER", "tourism")
DB_PASS = os.getenv("DB_PASS", "tourism123")

def get_dw_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursor_factory=RealDictCursor
    )

    with conn.cursor() as cur:
        cur.execute("SET search_path TO dw, public")

    conn.commit()
    return conn
