{% macro impala__bool_or(expression) -%}
    bitor({{ expression }})
{%- endmacro %}
