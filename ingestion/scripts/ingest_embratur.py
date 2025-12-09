import requests
from utils import save_to_minio

def ingest_embratur():
    url = "https://dados.turismo.gov.br/api/v1/visitantes"
    response = requests.get(url)
    data = response.json()

    save_to_minio(bucket="tourism_raw", filename="embratur.json", content=data)

if __name__ == "__main__":
    ingest_embratur()
