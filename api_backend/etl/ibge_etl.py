import requests
import json
from datetime import datetime
from db import get_dw_connection

IBGE_URL = (
    "https://servicodados.ibge.gov.br/api/v3/agregados/1737/"
    "periodos/2010-2023/variaveis/2265?localidades=N1[all]"
)

def extract_ibge():
    print("🔎 Buscando dados do IBGE...")

    resp = requests.get(IBGE_URL, timeout=30)

    if resp.status_code != 200:
        print(f"❌ IBGE retornou {resp.status_code}")
        print(resp.text[:500])
        return

    data = resp.json()

    if not data:
        print("⚠️ Resposta vazia do IBGE")
        return

    conn = get_dw_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO raw.tourism_data (source, payload)
        VALUES (%s, %s)
    """, (
        "IBGE",
        json.dumps(data)
    ))

    conn.commit()
    conn.close()

    print("✔ Dados IBGE inseridos em raw.tourism_data")

if __name__ == "__main__":
    extract_ibge()
