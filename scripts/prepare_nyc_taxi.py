"""Download one NYC yellow-taxi month and write its real trip schema to local Delta."""

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


FIELDS = {
    "VendorID": ("vendor_id", pa.uint16()),
    "tpep_pickup_datetime": ("pickup_at", pa.timestamp("us", tz="America/New_York")),
    "tpep_dropoff_datetime": ("dropoff_at", pa.timestamp("us", tz="America/New_York")),
    "passenger_count": ("passenger_count", pa.uint16()),
    "trip_distance": ("trip_distance", pa.float64()),
    "RatecodeID": ("rate_code_id", pa.uint16()),
    "store_and_fwd_flag": ("store_and_fwd_flag", pa.string()),
    "PULocationID": ("pickup_location_id", pa.uint16()),
    "DOLocationID": ("dropoff_location_id", pa.uint16()),
    "payment_type": ("payment_type", pa.uint16()),
    **{name: (name.lower(), pa.decimal128(18, 2)) for name in (
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "total_amount", "congestion_surcharge",
        "Airport_fee", "cbd_congestion_fee",
    )},
}
SCHEMA = pa.schema([
    ("trip_id", pa.uint64()), ("source_month", pa.date32()),
    ("source_row_number", pa.uint64()), ("pickup_date", pa.date32()),
    *FIELDS.values(),
])


def convert_batch(batch, month, offset):
    """Keep source nulls and corrections; normalize names, money, and timestamps."""
    count = len(batch)
    dates = pc.cast(batch.column("tpep_pickup_datetime"), pa.date32())
    base = (month.year * 100 + month.month) * 10_000_000_000 + offset
    arrays = [
        pa.array(range(base, base + count), type=pa.uint64()),
        pa.repeat(pa.scalar(month, type=pa.date32()), count),
        pa.array(range(offset, offset + count), type=pa.uint64()), dates,
    ]
    for source, (_, target_type) in FIELDS.items():
        values = batch.column(source) if source in batch.schema.names else pa.nulls(count, type=target_type)
        if pa.types.is_timestamp(target_type) and values.type.tz is None:
            values = pc.assume_timezone(values, "America/New_York", ambiguous="earliest", nonexistent="earliest")
        arrays.append(pc.cast(values, target_type))
    return pa.RecordBatch.from_arrays(arrays, schema=SCHEMA)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", default="2025-01", help="Source month, YYYY-MM")
    args = parser.parse_args()
    start = date.fromisoformat(args.month + "-01")
    end = date(start.year + (start.month == 12), start.month % 12 + 1, 1)
    target = f"s3://poc/delta/nyc_taxi_trips/{args.month}"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{args.month}.parquet"
    stats = {"month": args.month, "source_rows": 0, "written_rows": 0, "delta_path": target}
    started = time.monotonic()
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "trips.parquet"
        print(f"Downloading {url}", flush=True)
        urllib.request.urlretrieve(url, path)
        parquet = pq.ParquetFile(path)
        if parquet.metadata.num_rows >= 10_000_000_000:
            raise ValueError("Source exceeds the supported per-file trip-ID range")

        def batches():
            offset = 0
            for raw in parquet.iter_batches(batch_size=250_000):
                batch = convert_batch(raw, start, offset)
                dates = batch.column("pickup_date")
                batch = batch.filter(pc.and_(pc.greater_equal(dates, start), pc.less(dates, end)))
                offset += len(raw)
                stats["source_rows"] += len(raw)
                stats["written_rows"] += len(batch)
                if len(batch):
                    yield batch

        write_deltalake(
            target, pa.RecordBatchReader.from_batches(SCHEMA, batches()),
            mode="overwrite", partition_by=["pickup_date"],
            storage_options={
                "AWS_ENDPOINT_URL": "http://minio:9000",
                "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
                "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
                "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
                "AWS_ALLOW_HTTP": "true", "AWS_S3_ADDRESSING_STYLE": "path",
                "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            },
        )
    stats["excluded_pickup_dates"] = stats["source_rows"] - stats["written_rows"]
    stats["elapsed_seconds"] = round(time.monotonic() - started, 2)
    print(json.dumps(stats, indent=2), flush=True)


if __name__ == "__main__":
    main()
