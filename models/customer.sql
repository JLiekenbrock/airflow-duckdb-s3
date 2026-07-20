{{ config(location=output_uri('customer')) }}

{{ scd2_from_parquet('customer', 'customer_id') }}
