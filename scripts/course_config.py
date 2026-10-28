"""Configuração local compartilhada pelos scripts da disciplina."""
import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
os.environ.setdefault("DLT_DATA_DIR", str(ROOT / ".local" / "dlt"))
os.environ.setdefault("DLT_TELEMETRY", "false")


def s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
        region_name="us-east-1",
        config=Config(connect_timeout=5, read_timeout=30, retries={"max_attempts": 2}),
    )


def list_objects(bucket, prefix=""):
    return [
        obj
        for page in s3_client().get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix
        )
        for obj in page.get("Contents", [])
    ]


def read_dlt_table(dataset, entity):
    """Lê todos os arquivos principais e seleciona a carga dlt mais recente."""
    import io
    from decimal import Decimal
    import pandas as pd

    client = s3_client()
    prefix = f"{dataset}/{entity}/"
    files = [o["Key"] for o in list_objects("bronze", prefix)
             if o["Key"].endswith(".parquet") and "/ano=" not in o["Key"]]
    if not files:
        raise FileNotFoundError(f"Nenhum Parquet principal em bronze/{prefix}")
    df = pd.concat([
        pd.read_parquet(io.BytesIO(client.get_object(Bucket="bronze", Key=k)["Body"].read()))
        for k in files
    ], ignore_index=True)
    if "_dlt_load_id" in df:
        latest = max(df["_dlt_load_id"].unique(), key=Decimal)
        df = df[df["_dlt_load_id"] == latest].reset_index(drop=True)
        print(f"Carga selecionada para {dataset}/{entity}: {latest}")
    return df
