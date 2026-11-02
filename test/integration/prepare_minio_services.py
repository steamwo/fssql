"""复用用户指定的服务，为公共 API 集成测试准备独立数据。"""

import io
import json
import os
from pathlib import Path
import subprocess
import uuid

import boto3
from botocore.config import Config
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq


def container_environment(name):
    """读取指定服务的连接配置，凭据不输出到终端或报告。"""
    result = subprocess.run(
        ["wsl", "-d", "Ubuntu-22.04", "--", "/usr/bin/docker", "inspect", name,
         "--format", "{{json .Config.Env}}"],
        check=True, capture_output=True, text=True, timeout=20,
    )
    return dict(item.split("=", 1) for item in json.loads(result.stdout) if "=" in item)


def main():
    """验证服务并在专用测试 bucket 中上传固定的小规模查询样本。"""
    root = Path(__file__).resolve().parents[2] / ".cache/agent-verification"
    root.mkdir(parents=True, exist_ok=True)
    evidence = root.parents[1] / "artifacts/local-validation/2026-11-02/services"
    evidence.mkdir(parents=True, exist_ok=True)
    minio = container_environment("docker-minio-1")
    postgres = container_environment("docker-postgres-1")
    access = minio.get("MINIO_ROOT_USER", minio.get("MINIO_ACCESS_KEY"))
    secret = minio.get("MINIO_ROOT_PASSWORD", minio.get("MINIO_SECRET_KEY"))
    if not access or not secret:
        raise RuntimeError("MinIO credentials are not available in the configured environment")

    with psycopg.connect(
        host="127.0.0.1", port=5432,
        dbname=postgres.get("POSTGRES_DB", postgres.get("POSTGRES_USER", "postgres")),
        user=postgres.get("POSTGRES_USER", "postgres"),
        password=postgres.get("POSTGRES_PASSWORD", ""),
        connect_timeout=10, options="-c default_transaction_read_only=on",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT version(), current_database(), 1 + 1")
            version, database, value = cur.fetchone()
            if value != 2:
                raise AssertionError("PostgreSQL query returned an unexpected result")
    result = {"postgres": {"endpoint": "127.0.0.1:5432", "version": version,
                           "database": database, "read_only_query": value}}

    client = boto3.client(
        "s3", endpoint_url="http://127.0.0.1:9000", region_name="us-east-1",
        aws_access_key_id=access, aws_secret_access_key=secret,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                      connect_timeout=10, read_timeout=10, retries={"max_attempts": 2}),
    )
    state_file = root / "minio-test-env.json"
    if state_file.exists():
        state = json.loads(state_file.read_text(encoding="utf-8"))
        bucket = state["FSSQL_MINIO_BUCKET"]
        if not bucket.startswith("fssql-agent-test-"):
            raise RuntimeError("Refusing to use a non-test bucket")
    else:
        bucket = "fssql-agent-test-20261102-" + uuid.uuid4().hex[:8]
        client.create_bucket(Bucket=bucket)
    table = pa.table({"id": pa.array([1, 2, 3], type=pa.int64()),
                      "label": ["alpha", "beta", "gamma"],
                      "amount": pa.array([10, 20, 30], type=pa.int64())})
    payload = io.BytesIO()
    pq.write_table(table, payload, compression="NONE", use_dictionary=False, data_page_version="1.0")
    client.put_object(Bucket=bucket, Key="input.parquet", Body=payload.getvalue())
    # 公共 CSV 装配目前默认不启用 Header，列名使用 column0/column1/column2。
    client.put_object(Bucket=bucket, Key="input.csv",
                      Body=b"1,alpha,10\n2,beta,20\n3,gamma,30\n")
    state = {"FSSQL_MINIO_ENDPOINT": "http://127.0.0.1:9000", "FSSQL_MINIO_BUCKET": bucket,
             "FSSQL_MINIO_ACCESS_KEY": access, "FSSQL_MINIO_SECRET_KEY": secret}
    state_file.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8", newline="\n")
    result["minio"] = {"endpoint": "http://127.0.0.1:9000", "bucket": bucket,
                        "fixture_rows": 3, "objects": ["input.csv", "input.parquet"]}
    (evidence / "services.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
