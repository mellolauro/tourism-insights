from fastapi import APIRouter
import pandas as pd
from db import get_dw_connection

router = APIRouter()

@router.get("/kpis/timeseries")
def get_kpis_timeseries():
    conn = get_dw_connection()

    df = pd.read_sql("""
        SELECT
            make_date(year, month, 1) AS date,
            total_visitors AS visitors
        FROM analytics.monthly_visitors
        ORDER BY year, month
    """, conn)

    conn.close()
    return df.to_dict(orient="records")
