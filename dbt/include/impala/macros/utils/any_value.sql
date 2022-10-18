{% macro impala__any_value(expression) -%}
    {#-- return any value (non-deterministic)  --#}
    first_value({{ expression }}) over(partition by {{expression}} order by {{expression}})
{%- endmacro %}
