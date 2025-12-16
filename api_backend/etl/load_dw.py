import pandas as pd
from db import get_dw_connection

def load_dw():
    conn = get_dw_connection()
    cur = conn.cursor()

    df = pd.read_sql("""
        SELECT DISTINCT reference_date, visitors, origin
        FROM staging.cleaned_tourism
        WHERE origin = 'IBGE'
    """, conn)

    for _, row in df.iterrows():
        # dim_date
        cur.execute("""
            INSERT INTO dw.dim_date (date_value, year, month, day)
            VALUES (%s, EXTRACT(YEAR FROM %s), EXTRACT(MONTH FROM %s), EXTRACT(DAY FROM %s))
            ON CONFLICT (date_value) DO NOTHING
        """, (row.reference_date, row.reference_date, row.reference_date, row.reference_date))

        # dim_origin
        cur.execute("""
            INSERT INTO dw.dim_origin (origin)
            VALUES (%s)
            ON CONFLICT (origin) DO NOTHING
        """, (row.origin,))

        # fact
        cur.execute("""
            INSERT INTO dw.fact_tourism (date_id, origin_id, visitors)
            SELECT d.date_id, o.origin_id, %s
            FROM dw.dim_date d, dw.dim_origin o
            WHERE d.date_value = %s AND o.origin = %s
        """, (row.visitors, row.reference_date, row.origin))

    conn.commit()
    conn.close()
    print("✔ DW carregado")

if __name__ == "__main__":
    load_dw()
