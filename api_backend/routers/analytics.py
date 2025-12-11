from fastapi import APIRouter
from datetime import datetime, timedelta

router = APIRouter()

# === KPI ATUAIS (exemplo mock) ===
@router.get("/kpis/latest")
def get_kpis_latest():
    return {
        "visitors": 12850,
        "occupancy_rate": 0.82,
        "revenue": 915000,
        "conversion_rate": 0.17,
    }

# === SÉRIE TEMPORAL DE VISITANTES ===
@router.get("/kpis/timeseries")
def get_kpis_timeseries():
    base = datetime.now()
    data = []

    for i in range(30):
        day = base - timedelta(days=i)
        data.append({
            "date": day.strftime("%Y-%m-%d"),
            "visitors": 900 + i * 15  # exemplo mock
        })

    return list(reversed(data))
