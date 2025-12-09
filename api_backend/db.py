import psycopg2
from psycopg2.extras import RealDictCursor

def get_dw_connection():
    return psycopg2.connect(
        host="tourism_db",
        port=5432,
        user="postgres",
        password="postgres",
        database="tourism_dw"
    )
