from fastapi import FastAPI
from routers import forecast
from routers import analytics

app = FastAPI(
    title="Tourism Insights API",
    version="0.1"
)

app.include_router(forecast.router, prefix="/api", tags=["Forecast"])
app.include_router(analytics.router, prefix="/api", tags=["Analytics"])

@app.get("/")
def root():
    return {"status": "API online"}
