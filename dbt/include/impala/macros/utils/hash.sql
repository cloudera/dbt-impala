{% macro impala__hash(field) -%}
    hex(fnv_hash(cast({{ field }} as {{ api.Column.translate_type('string') }})))
{%- endmacro %}