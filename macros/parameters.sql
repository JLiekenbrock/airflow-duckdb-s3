{% macro require_mandant() -%}
  {%- set value = var('mandant') -%}
  {%- if not modules.re.match('^[A-Za-z0-9_-]{1,64}$', value) -%}
    {{ exceptions.raise_compiler_error('mandant must match [A-Za-z0-9_-]{1,64}') }}
  {%- endif -%}
  {{ return(value) }}
{%- endmacro %}

{% macro require_stichtage() -%}
  {%- set values = var('stichtage') -%}
  {%- if values is string or values | length == 0 -%}
    {{ exceptions.raise_compiler_error('stichtage must be a non-empty list') }}
  {%- endif -%}
  {%- for value in values -%}
    {%- if not modules.re.match('^\\d{4}-\\d{2}-\\d{2}$', value) -%}
      {{ exceptions.raise_compiler_error('invalid stichtag: ' ~ value) }}
    {%- endif -%}
  {%- endfor -%}
  {{ return(values) }}
{%- endmacro %}

{% macro sql_date_list(values) -%}
  {%- for value in values -%}
    DATE '{{ value }}'{% if not loop.last %}, {% endif %}
  {%- endfor -%}
{%- endmacro %}

{% macro input_uri() -%}
  {%- set template = env_var('DBT_INPUT_URI_TEMPLATE') -%}
  {{ return(template.replace('{mandant}', require_mandant())) }}
{%- endmacro %}

{% macro output_uri() -%}
  {%- set template = env_var('DBT_OUTPUT_URI_TEMPLATE') -%}
  {%- set result = template.replace('{mandant}', require_mandant()) -%}
  {{ return(result.replace('{run_id}', var('run_id'))) }}
{%- endmacro %}

