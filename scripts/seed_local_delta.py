"""Manually create or replace the local MinIO demo dataset."""

from datetime import date, timedelta
import os

import pyarrow as pa
from deltalake import write_deltalake


def main():
    rows = [
        {
            "tenant_id": tenant_id,
            "event_id": 1,
            "event_date": date(2026, 10, 12) + timedelta(days=tenant_id % 7),
            "event_type": "purchase" if tenant_id % 2 else "page_view",
            "user_id": tenant_id,
        }
        for tenant_id in range(1, 801)
    ]
    write_deltalake(
        "s3://poc/delta/events",
        pa.Table.from_pylist(rows),
        mode="overwrite",
        partition_by=["event_date"],
        storage_options={
            "AWS_ENDPOINT_URL": "http://minio:9000",
            "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
            "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
            "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ADDRESSING_STYLE": "path",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        },
    )
    print("Wrote 800 demo events to s3://poc/delta/events (October 12–18, 2026).")


if __name__ == "__main__":
    main()
