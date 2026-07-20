{{ config(location=output_uri('account')) }}

{% set selected_stichtage = require_stichtage() %}
{% set selected_mandant = require_mandant() %}

select *
from read_parquet('{{ input_uri('account') }}', hive_partitioning = true)
where cast(mandant as varchar) = '{{ selected_mandant }}'
  and cast(stichtag as date) in ({{ sql_date_list(selected_stichtage) }})

