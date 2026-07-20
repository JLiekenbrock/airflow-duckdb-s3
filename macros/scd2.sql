{% macro scd2_from_parquet(table, business_key) -%}
with source_rows as (
    select *
    from read_parquet('{{ input_uri(table) }}', hive_partitioning = true)
    where cast(mandant as varchar) = '{{ require_mandant() }}'
      and cast(stichtag as date) in ({{ sql_date_list(require_stichtage()) }})
),

hashed_rows as (
    select
        *,
        hash(
            json_merge_patch(
                to_json(source_rows),
                json_object('mandant', null, 'stichtag', null)
            )
        ) as scd_change_hash
    from source_rows
),

change_candidates as (
    select
        *,
        lag(scd_change_hash) over (
            partition by mandant, {{ adapter.quote(business_key) }}
            order by cast(stichtag as date)
        ) as previous_change_hash
    from hashed_rows
),

versions as (
    select * exclude (previous_change_hash)
    from change_candidates
    where previous_change_hash is null
       or previous_change_hash != scd_change_hash
),

dated_versions as (
    select
        *,
        lead(cast(stichtag as timestamp)) over (
            partition by mandant, {{ adapter.quote(business_key) }}
            order by cast(stichtag as date)
        ) as next_valid_from
    from versions
)

select
    * exclude (next_valid_from),
    cast(stichtag as timestamp) as valid_from,
    case
        when next_valid_from is null then null
        else next_valid_from - interval 1 microsecond
    end as valid_to,
    next_valid_from is null as is_current
from dated_versions
{%- endmacro %}
