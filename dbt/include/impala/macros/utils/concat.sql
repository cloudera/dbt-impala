{% macro impala__concat(fields) -%}
    concat({{ fields|join(', ') }})
{%- endmacro %}
