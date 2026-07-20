{{ config(location=output_uri('customer')) }}

{% set selected_stichtage = require_stichtage() %}
{% set selected_mandant = require_mandant() %}

select *
from read_parquet('{{ input_uri('customer') }}', hive_partitioning = true)
where cast(mandant as varchar) = '{{ selected_mandant }}'
  and cast(stichtag as date) in ({{ sql_date_list(selected_stichtage) }})

