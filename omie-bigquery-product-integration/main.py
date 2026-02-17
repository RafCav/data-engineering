import pandas as pd
import json
import pandas_gbq
import requests
from config import load_config


def fetch_endpoint(product_endpoint: str, body: dict):
    response = requests.post(
        product_endpoint,
        json=body,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )

    if response.status_code != 200:
        raise RuntimeError(f"Omie request failed: HTTP {response.status_code}")

    data = response.json()

    if "faultstring" in data:
        raise RuntimeError(f"Omie API error: {data['faultstring']}")

    return data


def integration():

    gbq_cred, gbq_table, product_endpoint, omie_secret = load_config()

    payload = {"param": [{"pagina": 1, "registros_por_pagina": 100, "apenas_importado_api": "N", "filtrar_apenas_omiepdv": "N"}]}
    body = {"call": "ListarProdutos", **json.loads(omie_secret), **payload}

    ##### Fetch Page - runs for the first time to get the total_pages. Also, gets the data from the page 1
    data = fetch_endpoint(product_endpoint, body)
    items = data.get("produto_servico_cadastro", []) or []
    total_pages = int(data.get("total_de_paginas", 1))

    ##### Pagination - runs all pages and store the data into data_per_page
    for page in range(2, total_pages + 1):
        body['param'][0]['pagina'] = page
        data = fetch_endpoint(product_endpoint, body)
        items.extend(data.get("produto_servico_cadastro", []) or [])

    ##### Save data into BigQuery
    if items:
        df = pd.DataFrame(items)
        pandas_gbq.to_gbq(df, gbq_table, project_id=gbq_table.split(".")[0], credentials=gbq_cred, if_exists="replace")
    else:
        print('No data to save')

    return '200'


if __name__ == "__main__":
    integration()


