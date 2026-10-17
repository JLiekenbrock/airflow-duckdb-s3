"""Project one official NYC yellow-taxi month into the POC event schema in Delta."""

import argparse
from datetime import date
import json
import os
from pathlib import Path
import tempfile
import time
import urllib.request

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from deltalake import write_deltalake


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", default="2025-01", help="Source month, YYYY-MM")
    args = parser.parse_args()
    start = date.fromisoformat(args.month + "-01")
    end = date(start.year + (start.month == 12), start.month % 12 + 1, 1)
    month_id = start.year * 100 + start.month
    target = f"s3://poc/delta/nyc_taxi/{args.month}"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{args.month}.parquet"
    schema = pa.schema([
        ("tenant_id", pa.uint64()), ("event_id", pa.uint64()),
        ("event_date", pa.date32()), ("event_type", pa.string()), ("user_id", pa.uint64()),
    ])
    stats = {"month": args.month, "source_rows": 0, "written_rows": 0, "delta_path": target}
    started = time.monotonic()
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "trips.parquet"
        print(f"Downloading {url}", flush=True)
        urllib.request.urlretrieve(url, path)
        parquet = pq.ParquetFile(path)
        print(f"Downloaded {path.stat().st_size:,} bytes; {parquet.metadata.num_rows:,} trips", flush=True)

        def batches():
            offset = 0
            for source in parquet.iter_batches(
                batch_size=250_000, columns=["PULocationID", "tpep_pickup_datetime", "VendorID"]
            ):
                dates = pc.cast(source.column("tpep_pickup_datetime"), pa.date32())
                zones = pc.cast(pc.fill_null(source.column("PULocationID"), 0), pa.uint64())
                vendors = pc.cast(pc.fill_null(source.column("VendorID"), 0), pa.uint64())
                # IDs use source month and original row position, including filtered rows.
                events = pa.RecordBatch.from_arrays([
                    zones,
                    pa.array(range(month_id * 10_000_000_000 + offset, month_id * 10_000_000_000 + offset + len(source)), type=pa.uint64()),
                    dates, pa.repeat("purchase", len(source)), vendors,
                ], schema=schema)
                mask = pc.and_(pc.and_(pc.greater_equal(dates, start), pc.less(dates, end)), pc.and_(pc.greater(zones, 0), pc.less_equal(zones, 800)))
                events = events.filter(mask)
                offset += len(source)
                stats["source_rows"] += len(source)
                stats["written_rows"] += len(events)
                if len(events):
                    yield events

        write_deltalake(
            target, pa.RecordBatchReader.from_batches(schema, batches()),
            mode="overwrite", partition_by=["event_date"],
            storage_options={
                "AWS_ENDPOINT_URL": "http://minio:9000",
                "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
                "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
                "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
                "AWS_ALLOW_HTTP": "true", "AWS_S3_ADDRESSING_STYLE": "path",
                "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            },
        )
    stats["excluded_rows"] = stats["source_rows"] - stats["written_rows"]
    stats["elapsed_seconds"] = round(time.monotonic() - started, 2)
    print(json.dumps(stats, indent=2), flush=True)


if __name__ == "__main__":
    main()
