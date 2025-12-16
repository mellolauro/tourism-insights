import pandas as pd
from datetime import datetime
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

    raw = df.iloc[0]["payload"]

    rows = []
    for serie in raw[0]["resultados"][0]["series"]:
        for period, value in serie["serie"].items():
            rows.append({
                "reference_date": datetime.strptime(period, "%Y%m").date(),
                "visitors": int(float(value) * 1000),
                "origin": "IBGE"
            })

    clean_df = pd.DataFrame(rows)

    clean_df.to_sql(
        "cleaned_tourism",
        conn,
        schema="staging",
        if_exists="append",
        index=False
    )

    conn.close()
    print("✔ STAGING IBGE pronto")

if __name__ == "__main__":
    transform()
