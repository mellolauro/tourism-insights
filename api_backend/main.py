from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers.kpis import router as kpis_router

app = FastAPI(title="Tourism Insights API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # depois restringimos
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(kpis_router)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/forecast")
def forecast(payload: dict):
    return {
        "history": [
            {"date": "2024-01-01", "value": 100},
            {"date": "2024-01-02", "value": 120},
        ],
        "forecast": [
            {"date": "2024-01-03", "value": 130, "lower": 120, "upper": 150},
            {"date": "2024-01-04", "value": 140, "lower": 125, "upper": 160},
        ]
    }
