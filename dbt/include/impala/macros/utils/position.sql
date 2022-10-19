{% macro position(substring_text, string_text) -%}
    {{ return(adapter.dispatch('position', 'dbt') (substring_text, string_text)) }}
{% endmacro %}

{% macro impala__position(substring_text, string_text) %}

    locate(
        {{ substring_text }}, {{ string_text }}
    )

{%- endmacro -%}
