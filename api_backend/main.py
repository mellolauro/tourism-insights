from fastapi import FastAPI
from api_backend.routers import forecast

app = FastAPI(
    title="Tourism Insights API",
    version="0.1"
)

app.include_router(forecast.router, prefix="/api", tags=["Forecast"])


@app.get("/")
def root():
    return {"status": "API online"}
