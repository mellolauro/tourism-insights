from fastapi import APIRouter, Query
from db import get_dw_connection
from datetime import date
from dateutil.relativedelta import relativedelta

router = APIRouter(prefix="/api/v1/kpis", tags=["KPIs"])

@router.get("/timeseries")
def tourism_timeseries(months: int = Query(12, ge=1, le=60)):
    conn = get_dw_connection()
    cur = conn.cursor()

    cur.execute("SELECT MAX(date_value) AS max_date FROM dw.dim_date")
    max_date = cur.fetchone()["max_date"]

    if not max_date:
        return []

    start_date = max_date - relativedelta(months=months)

    cur.execute("""
        SELECT
            d.date_value AS date,
            SUM(f.visitors) AS visitors
        FROM dw.fact_tourism f
        JOIN dw.dim_date d ON d.date_id = f.date_id
        WHERE d.date_value >= %s
        GROUP BY d.date_value
        ORDER BY d.date_value
    """, (start_date,))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "date": r["date"].isoformat(),
            "visitors": r["visitors"]
        }
        for r in rows
    ]
