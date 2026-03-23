import os
from dotenv import load_dotenv
import pandas_gbq
from google.oauth2 import service_account
import json
from google.cloud import bigquery
from config import load_config
from types import SimpleNamespace


def build_mock_request(method="GET", body=None, headers=None):
    headers = headers or {}
    body = body or {}

    raw = json.dumps(body)

    def _get_json():
        return body

    def _get_data(as_text=False):
        return raw if as_text else raw.encode("utf-8")

    return SimpleNamespace(
        method=method,
        headers=headers,
        get_json=_get_json,
        get_data=_get_data,
    )


def integration(request):

    ###################################### Health check / ping

    if request.method == 'GET':
        return {"status": "ok"}, 200

    ###################################### Credentials

    gbq_cred, gbq_table, api_key = load_config()

    ###################################### Validation

    if request.method != 'POST':
        print(f"Error: method_not_allowed \nMethod used: {request.method}")
        return {"error": "method_not_allowed", "message": "Only POST method is supported"}, 405
    if request.headers.get('postkey') != api_key:
        print(f"Error: unauthorized \nCredential used: {request.headers.get('postkey')}")
        return {"error": "unauthorized", "message": "Invalid authentication credentials"}, 401
    print('method allowed | authorized')

    ###################################### Receive Data

    try:
        response = request.get_json()
    except Exception as e:
        print(f"Error: invalid_payload \nException: {e} \nBody received: {request.get_data(as_text=True)}")
        return {"error": "invalid_payload", "message": "Request body must be a valid JSON"}, 400

    ###################################### Validating Orderid

    if not 'client_id' in response:
        print(f"Error: unprocessable_entity \nPayload received: {str(response)}")
        return {"error": "unprocessable_entity",
                "message": "Required field 'client_id' is missing or invalid"}, 422

    client_id = response.get('client_id')

    ###################################### Fetch Data Warehouse

    sql = f' SELECT * FROM {gbq_table} WHERE client_id = @client_id LIMIT 1 '

    try:
        bq_client = bigquery.Client(credentials=gbq_cred, project=gbq_table.split(".")[0])
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("client_id", "INT64", client_id)]
        )
        job = bq_client.query(sql, job_config=job_config)
        rows = list(job.result())

    except Exception as e:
        print(f"Error: upstream_service_error \nException: {e} \nSQL: {sql}")
        return {"error": "upstream_service_error", "message": "Failed to retrieve data"}, 502

    ###################################### Prepare json to return

    data = [{"name": r["name"], "state": r["state"], "city": r["city"]} for r in rows]

    return {"data": data}, 200


# Exemplo
req = build_mock_request(
    method="POST",
    headers={"postkey": "deRhf5FdzyU86#zrR4"},
    body={"client_id": 5}
)
print(integration(req))