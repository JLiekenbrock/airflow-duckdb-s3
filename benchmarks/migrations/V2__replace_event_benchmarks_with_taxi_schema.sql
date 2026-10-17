DROP TABLE IF EXISTS poc_insert_benchmark.append;
DROP TABLE IF EXISTS poc_insert_benchmark.delete_insert;
DROP TABLE IF EXISTS poc_insert_benchmark.legacy;
DROP TABLE IF EXISTS poc_insert_benchmark.insert_overwrite;

CREATE TABLE poc_insert_benchmark.append AS analytics.nyc_taxi_trips;
CREATE TABLE poc_insert_benchmark.delete_insert AS analytics.nyc_taxi_trips;
CREATE TABLE poc_insert_benchmark.legacy AS analytics.nyc_taxi_trips;
CREATE TABLE poc_insert_benchmark.insert_overwrite AS analytics.nyc_taxi_trips;
