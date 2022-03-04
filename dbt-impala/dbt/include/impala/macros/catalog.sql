
{% macro dbt-impala__get_catalog(information_schema, schemas) -%}

  {% set msg -%}
    get_catalog not implemented for dbt-impala
  {%- endset %}

  {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}
