{% macro impala__array_construct(inputs, data_type) -%}
  {{ exceptions.raise_compiler_error("Array functions are not supported in Impala") }}
{%- endmacro %}
