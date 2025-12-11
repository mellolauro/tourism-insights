# main.py (AJUSTADO PARA CORS)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware 
from routers import forecast
from routers import analytics

app = FastAPI(
            title="Tourism Insights API",
            version="0.1"
)

# --- Configuração CORS ---
# Lista de origens permitidas
# Adicione a URL do seu frontend em desenvolvimento/produção
origins = [
    "http://localhost",
    "http://localhost:3000", # A origem do seu frontend Next.js
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,          # Permite as origens da lista
    allow_credentials=True,         # Permite cookies/credenciais se necessário
    allow_methods=["*"],            # Permite todos os métodos (GET, POST, etc.)
    allow_headers=["*"],            # Permite todos os headers
)
# -------------------------

app.include_router(forecast.router, prefix="/api/v1", tags=["Forecast"])
app.include_router(analytics.router, prefix="/api/v1", tags=["Analytics"])

@app.get("/")
def root():
    return {"status": "API online"}