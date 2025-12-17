import requests
import csv
import io
import json
from datetime import datetime
from db import get_dw_connection

# Dataset oficial (exemplo estável)
EMBRATUR_CSV_URL = (
    "https://dados.gov.br/dataset/"
    "chegadas-de-turistas-internacionais-no-brasil"
    "/resource/4d6a1e5b-9f9b-4a4a-9f5a-0c1f5f8a9b3e/download/"
    "chegadas-turistas-internacionais.csv"
)

SOURCE_NAME = "EMBRATUR"

def run_embratur_etl():
    conn = get_dw_connection()
    cur = conn.cursor()

    print("⬇️ Baixando dados Embratur...")
    response = requests.get(EMBRATUR_CSV_URL, timeout=60)
    response.raise_for_status()

    # =====================================================
    # 1. RAW
    # =====================================================
    cur.execute("""
        INSERT INTO raw.tourism_data (source, payload)
        VALUES (%s, %s)
    """, (
        SOURCE_NAME,
        json.dumps({"csv": response.text[:5000]})  # amostra
    ))

    # =====================================================
    # 2. LEITURA CSV
    # =====================================================
    csv_file = io.StringIO(response.text)
    reader = csv.DictReader(csv_file)

    for row in reader:
        """
        Estrutura típica:
        - ano
        - mes
        - pais_origem
        - chegadas
        """

        try:
            year = int(row["ano"])
            month = int(row["mes"])
            visitors = int(row["chegadas"])
            origin = row.get("pais_origem", "INTERNACIONAL")
        except Exception:
            continue

        ref_date = datetime(year, month, 1).date()

        # =================================================
        # 3. STAGING
        # =================================================
        cur.execute("""
            INSERT INTO staging.cleaned_tourism
            (reference_date, visitors, origin)
            VALUES (%s, %s, %s)
        """, (
            ref_date,
            visitors,
            origin[:50]
        ))

        # =================================================
        # 4. DIM DATE
        # =================================================
        cur.execute("""
            INSERT INTO dw.dim_date (date_value, year, month, day)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (date_value) DO NOTHING
        """, (
            ref_date,
            year,
            month,
            1
        ))

        # =================================================
        # 5. DIM ORIGIN
        # =================================================
        cur.execute("""
            INSERT INTO dw.dim_origin (origin)
            VALUES (%s)
            ON CONFLICT (origin) DO NOTHING
        """, (origin[:50],))

        # =================================================
        # 6. FACT
        # =================================================
        cur.execute("""
            INSERT INTO dw.fact_tourism (date_id, origin_id, visitors)
            SELECT d.date_id, o.origin_id, %s
            FROM dw.dim_date d, dw.dim_origin o
            WHERE d.date_value = %s AND o.origin = %s
        """, (
            visitors,
            ref_date,
            origin[:50]
        ))

    conn.commit()
    cur.close()
    conn.close()

    print("✅ ETL Embratur executado com sucesso.")
