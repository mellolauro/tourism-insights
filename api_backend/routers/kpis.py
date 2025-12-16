from fastapi import APIRouter
from datetime import date, timedelta

router = APIRouter(prefix="/api/v1/kpis", tags=["KPIs"])

@router.get("/timeseries")
def kpis_timeseries():
    base_date = date.today()

    data = []
    for i in range(10):
        data.append({
            "date": str(base_date - timedelta(days=i)),
            "visitors": 100000 + i * 1200,
            "revenue": 5000000 + i * 250000,
        })

    return data
