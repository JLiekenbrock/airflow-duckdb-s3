{{ config(location=output_uri()) }}

{% set selected_stichtage = require_stichtage() %}
{% set selected_mandant = require_mandant() %}

select
    cast(mandant as varchar) as mandant,
    cast(stichtag as date) as stichtag,
    count(*) as row_count,
    sum(cast(betrag as decimal(18, 2))) as betrag_summe
from read_parquet('{{ input_uri() }}', hive_partitioning = true)
where cast(mandant as varchar) = '{{ selected_mandant }}'
  and cast(stichtag as date) in ({{ sql_date_list(selected_stichtage) }})
group by 1, 2

