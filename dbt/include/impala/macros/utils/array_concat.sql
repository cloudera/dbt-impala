{% macro impala__array_concat(array_1, array_2) -%}
  {{ exceptions.raise_compiler_error("Array functions are not supported in Impala") }}
{%- endmacro %}
