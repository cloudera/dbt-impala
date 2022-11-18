{% macro impala__array_append(array, new_element) -%}
  {{ exceptions.raise_compiler_error("Array functions are not supported in Impala") }}
{%- endmacro %}
