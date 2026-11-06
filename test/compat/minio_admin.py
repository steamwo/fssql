#!/usr/bin/env python3
"""MinIO 测试对象清理工具。

用法：
  python minio_admin.py delete-prefix <bucket> <prefix>

凭据只从环境变量读取，不写入命令行参数或输出：
  FSSQL_MINIO_ENDPOINT / FSSQL_MINIO_ACCESS_KEY / FSSQL_MINIO_SECRET_KEY
"""

import json
import os
import sys

try:
    import boto3
except Exception as exc:  # pragma: no cover - dependency failure is reported to Go
    print(json.dumps({"error": f"boto3 import failed: {exc}"}))
    raise SystemExit(2)


def client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["FSSQL_MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["FSSQL_MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["FSSQL_MINIO_SECRET_KEY"],
        region_name="us-east-1",
    )


def delete_prefix(bucket, prefix):
    s3 = client()
    deleted = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if not objects:
            continue
        s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
        deleted += len(objects)
    return deleted


def main():
    if len(sys.argv) != 4 or sys.argv[1] != "delete-prefix":
        raise ValueError("usage: minio_admin.py delete-prefix <bucket> <prefix>")
    deleted = delete_prefix(sys.argv[2], sys.argv[3])
    print(json.dumps({"deleted": deleted}))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        raise SystemExit(1)
