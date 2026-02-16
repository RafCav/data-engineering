import json
import os
from google.oauth2 import service_account
from dotenv import load_dotenv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def load_gcp_credentials():
    gcp_env = os.getenv("GCP_CREDENTIALS")
    gcp_file = os.getenv("GCP_JSON_KEY_PATH")

    if gcp_env:
        return service_account.Credentials.from_service_account_info(info=json.loads(gcp_env))

    if gcp_file and os.path.exists(gcp_file):
        return service_account.Credentials.from_service_account_file(gcp_file)

    raise RuntimeError("GCP credentials not configured")


def load_config():
    gbq_cred = load_gcp_credentials()
    gbq_table = os.getenv('GBQ_PRODUCT_TABLE')
    product_endpoint = os.getenv('OMIE_PRODUCT_ENDPOINT')
    omie_secret = os.getenv('OMIE_SECRET')

    if not all([gbq_table, product_endpoint, omie_secret]):
        raise RuntimeError("Missing required environment variables")

    return gbq_cred, gbq_table, product_endpoint, omie_secret

