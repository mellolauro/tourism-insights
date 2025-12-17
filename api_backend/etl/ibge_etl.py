import requests
from datetime import datetime
from db import get_dw_connection
import json

IBGE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"

def run_ibge_etl():
    conn = get_dw_connection()
    cur = conn.cursor()

    # 1. Extrair dados
    response = requests.get(IBGE_URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    # 2. Salvar RAW
    cur.execute("""
        INSERT INTO raw.tourism_data (source, payload)
        VALUES (%s, %s)
    """, ("IBGE_SIDRA", json.dumps(data)))

    # Ignorar cabeçalho
    rows = data[1:]

    for row in rows:
        ref_date = datetime.strptime(row["D3C"], "%Y%m").date()
        visitors = int(float(row["V"]))

        # STAGING
        cur.execute("""
            INSERT INTO staging.cleaned_tourism (reference_date, visitors, origin)
            VALUES (%s, %s, %s)
        """, (ref_date, visitors, "BR"))

        # DIM DATE
        cur.execute("""
            INSERT INTO dw.dim_date (date_value, year, month, day)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (date_value) DO NOTHING
        """, (ref_date, ref_date.year, ref_date.month, 1))

        # DIM ORIGIN
        cur.execute("""
            INSERT INTO dw.dim_origin (origin)
            VALUES (%s)
            ON CONFLICT (origin) DO NOTHING
        """, ("BR",))

        # FACT
        cur.execute("""
            INSERT INTO dw.fact_tourism (date_id, origin_id, visitors)
            SELECT d.date_id, o.origin_id, %s
            FROM dw.dim_date d, dw.dim_origin o
            WHERE d.date_value = %s AND o.origin = %s
        """, (visitors, ref_date, "BR"))

    conn.commit()
    cur.close()
    conn.close()

    print("ETL IBGE executado com sucesso.")
