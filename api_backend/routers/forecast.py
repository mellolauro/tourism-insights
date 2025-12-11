from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import pandas as pd
from prophet import Prophet
from datetime import datetime
from db import get_dw_connection 

router = APIRouter()

# Modelo de entrada
class ForecastRequest(BaseModel):
    metric: str  # "visitors", "revenue", etc.
    periods: int = 30  # dias de previsão

# Endpoint principal
@router.post("/forecast")
def generate_forecast(request: ForecastRequest):

    metric = request.metric
    periods = request.periods

    # ----------------------------
    # 1. Carregar dados do DW
    # ----------------------------
    try:
        conn = get_dw_connection()
        query = f"""
            SELECT date, {metric}
            FROM fact_tourism_daily
            WHERE {metric} IS NOT NULL
            ORDER BY date;
        """
        df = pd.read_sql(query, conn)
        conn.close()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar DW: {str(e)}")

    if df.empty:
        raise HTTPException(status_code=404, detail="Nenhum dado encontrado para previsão")

    # ----------------------------
    # 2. Preparar dados p/ Prophet
    # ----------------------------
    df_prophet = df.rename(columns={"date": "ds", metric: "y"})

    # Prophet exige datetime
    df_prophet["ds"] = pd.to_datetime(df_prophet["ds"])

    # ----------------------------
    # 3. Treinar Prophet
    # ----------------------------
    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False
    )
    model.fit(df_prophet)

    # ----------------------------
    # 4. Gerar previsão
    # ----------------------------
    future = model.make_future_dataframe(periods=periods)
    forecast = model.predict(future)

    # ----------------------------
    # 5. Formatar retorno p/ Dashboard
    # ----------------------------
    result = {
        "history": [
            {"date": row["ds"].strftime("%Y-%m-%d"), "value": float(row["y"])}
            for _, row in df_prophet.iterrows()
        ],
        "forecast": [
            {
                "date": row["ds"].strftime("%Y-%m-%d"),
                "value": float(row["yhat"]),
                "lower": float(row["yhat_lower"]),
                "upper": float(row["yhat_upper"]),
            }
            for _, row in forecast.tail(periods).iterrows()
        ],
    }

    return result
