import pandas as pd
from db import get_dw_connection

def transform():
    conn = get_dw_connection()

    df = pd.read_sql("""
        SELECT payload
        FROM raw.tourism_data
        WHERE source = 'IBGE'
        ORDER BY extracted_at DESC
        LIMIT 1
    """, conn)

    if df.empty:
        print("⚠️ Nenhum dado IBGE encontrado no RAW")
        return

    raw_payload = df.iloc[0, 0]  # <- acesso direto, sem ambiguidades

    # ===============================
    # VALIDAÇÃO DO PAYLOAD
    # ===============================
    if not isinstance(raw_payload, list):
        print("❌ Payload IBGE não é LISTA")
        print(type(raw_payload), raw_payload)
        return

    try:
        series = raw_payload[0]["resultados"][0]["series"]
    except (KeyError, IndexError, TypeError) as e:
        print("❌ Estrutura inesperada do payload IBGE")
        print(raw_payload)
        print(e)
        return

    rows = []

    for serie in series:
        for ref, value in serie["serie"].items():
            rows.append({
                "reference_date": f"{ref[:4]}-{ref[4:]}-01",
                "visitors": int(value),
                "origin": "Brasil"
            })

    if not rows:
        print("⚠️ Nenhum dado processado")
        return

    staging_df = pd.DataFrame(rows)

    staging_df.to_sql(
        "cleaned_tourism",
        conn,
        schema="staging",
        if_exists="append",
        index=False
    )

    conn.close()
    print(f"✔ {len(rows)} registros inseridos em STAGING")

if __name__ == "__main__":
    transform()
