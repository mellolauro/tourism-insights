import os
import pandas as pd
from prophet import Prophet

SAMPLE_CSV = os.getenv("SAMPLE_CSV", "sample_data/tourism.csv")  

def make_forecast(country: str, periods: int = 90):
    """
    Lê um CSV de exemplo (date,country,visitors),
    filtra pelo país, treina Prophet e retorna previsões.
    """
    if not os.path.exists(SAMPLE_CSV):
        raise FileNotFoundError(f"Sample CSV not found: {SAMPLE_CSV}")

    df = pd.read_csv(SAMPLE_CSV, parse_dates=["date"])
    df = df[df["country"].str.lower() == country.lower()]
    if df.empty:
        raise ValueError("Nenhum dado de histórico para o country informado.")

    df_prop = df.rename(columns={"date": "ds", "visitors": "y"})[["ds", "y"]]
    m = Prophet(yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False)
    m.fit(df_prop)

    future = m.make_future_dataframe(periods=periods, freq="D")
    forecast = m.predict(future)

    out = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(periods).copy()
    out["ds"] = out["ds"].dt.strftime("%Y-%m-%d")
    return out.to_dict(orient="records")
