{{ config(location=output_uri('account')) }}

{{ scd2_from_parquet('account', 'account_id') }}
