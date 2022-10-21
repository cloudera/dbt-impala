{% macro impala__any_value(expression) -%}
    {#-- return any value (non-deterministic)  --#}
    max({{ expression }})
{%- endmacro %}
