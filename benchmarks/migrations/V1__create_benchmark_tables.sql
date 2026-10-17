CREATE DATABASE IF NOT EXISTS poc_insert_benchmark;

CREATE TABLE IF NOT EXISTS poc_insert_benchmark.append
(
    tenant_id UInt64,
    event_id UInt64,
    event_date Date,
    event_type LowCardinality(String),
    user_id UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (tenant_id, event_date, event_id);

CREATE TABLE IF NOT EXISTS poc_insert_benchmark.delete_insert AS poc_insert_benchmark.append;
CREATE TABLE IF NOT EXISTS poc_insert_benchmark.legacy AS poc_insert_benchmark.append;
CREATE TABLE IF NOT EXISTS poc_insert_benchmark.insert_overwrite AS poc_insert_benchmark.append;
