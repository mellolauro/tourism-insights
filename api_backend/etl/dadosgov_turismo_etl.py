import requests
import json
from datetime import datetime
from db import get_dw_connection
from psycopg2.extras import Json

CATALOG_URL = "https://api.dados.gov.br/v1/datasets"
QUERY = "turismo"

def run_dadosgov_turismo_etl():
    conn = get_dw_connection()
    cur = conn.cursor()

    # =====================================================
    # 1. EXTRAÇÃO – catálogo dados.gov.br
    # =====================================================
    response = requests.get(
        CATALOG_URL,
        params={"q": QUERY},
        timeout=30
    )
    response.raise_for_status()
    datasets = response.json()["data"]

    print(f"Datasets encontrados: {len(datasets)}")

    for dataset in datasets:
        dataset_id = dataset["id"]
        title = dataset.get("title", "")
        organization = dataset.get("organization", {}).get("title", "UNKNOWN")

        # =================================================
        # 2. SALVAR RAW
        # =================================================
        cur.execute("""
            INSERT INTO raw.tourism_data (source, payload)
            VALUES (%s, %s)
        """, (
            f"DADOS_GOV:{dataset_id}",
            json.dumps(dataset)
        ))

        # =================================================
        # 3. PROCESSAR RECURSOS
        # =================================================
        resources = dataset.get("resources", [])

        for res in resources:
            url = res.get("url", "")
            fmt = res.get("format", "").lower()

            # só formatos tratáveis automaticamente
            if fmt not in ("csv", "json"):
                continue

            print(f"→ Recurso {fmt.upper()} | {title}")

            try:
                r = requests.get(url, timeout=30)
                r.raise_for_status()
            except Exception:
                continue

            # =================================================
            # 4. STAGING (genérico para turismo)
            # =================================================
            extracted_at = datetime.utcnow().date()

            cur.execute("""
                INSERT INTO staging.cleaned_tourism
                (reference_date, visitors, origin)
                VALUES (%s, %s, %s)
            """, (
                extracted_at,
                None,              # ainda sem métrica numérica
                organization[:50]
            ))

            # =================================================
            # 5. DIM DATE
            # =================================================
            cur.execute("""
                INSERT INTO dw.dim_date (date_value, year, month, day)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (date_value) DO NOTHING
            """, (
                extracted_at,
                extracted_at.year,
                extracted_at.month,
                extracted_at.day
            ))

            # =================================================
            # 6. DIM ORIGIN
            # =================================================
            cur.execute("""
                INSERT INTO dw.dim_origin (origin)
                VALUES (%s)
                ON CONFLICT (origin) DO NOTHING
            """, (organization[:50],))

            # =================================================
            # 7. FACT (placeholder até métrica numérica)
            # =================================================
            cur.execute("""
                INSERT INTO dw.fact_tourism (date_id, origin_id, visitors)
                SELECT d.date_id, o.origin_id, %s
                FROM dw.dim_date d, dw.dim_origin o
                WHERE d.date_value = %s AND o.origin = %s
            """, (
                0,  # placeholder (dataset metadata)
                extracted_at,
                organization[:50]
            ))

    conn.commit()
    cur.close()
    conn.close()

    print("ETL dados.gov.br (turismo) executado com sucesso.")
