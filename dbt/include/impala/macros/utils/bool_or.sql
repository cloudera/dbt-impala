{% macro impala__bool_or(expression) -%}
    max({{ expression }})
{%- endmacro %}
