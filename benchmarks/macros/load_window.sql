{% macro load_window(start_date, end_date) %}
  {# Disjoint date windows make concurrent deletes/inserts independent.
     No shared adapter temporary tables are used. This is not atomic. #}
  {% set source = ref('stg_nyc_taxi_trips') %}
  {% set predicate %}
    pickup_date >= toDate('{{ start_date }}')
    and pickup_date < toDate('{{ end_date }}')
  {% endset %}
  {% do run_query('DELETE FROM poc_insert_benchmark.batching WHERE ' ~ predicate) %}
  {% set insert %}
    INSERT INTO poc_insert_benchmark.batching
    SELECT * REPLACE (
      assumeNotNull(trip_id) AS trip_id,
      assumeNotNull(source_month) AS source_month,
      assumeNotNull(source_row_number) AS source_row_number,
      assumeNotNull(pickup_date) AS pickup_date,
      assumeNotNull(pickup_at) AS pickup_at
    ) FROM {{ source }} WHERE {{ predicate }}
  {% endset %}
  {% do run_query(insert) %}
{% endmacro %}
