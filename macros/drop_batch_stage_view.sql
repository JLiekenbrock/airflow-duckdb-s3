{% macro drop_batch_stage_view() %}
    {% set batch_id = var('batch_id') %}
    {% set relation = adapter.get_relation(
        database=target.database,
        schema=target.schema,
        identifier='stg_delta_events_' ~ batch_id
    ) %}
    {% if relation %}
        {% do adapter.drop_relation(relation) %}
    {% endif %}
{% endmacro %}
