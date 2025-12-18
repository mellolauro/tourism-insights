import os
import psycopg2
from psycopg2.extras import RealDictCursor

def get_dw_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        cursor_factory=RealDictCursor
    )

    cur = conn.cursor()
    cur.execute("SET search_path TO raw, staging, dw, public;")
    conn.commit()

    return conn
