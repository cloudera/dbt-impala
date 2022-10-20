{% macro impala__split_part(string_text, delimiter_text, part_number) %}

    {% set split_part_expr %}

    split_part(
        {{ string_text }},
        {{ delimiter_text }},
        {{ part_number }}
        )

    {% endset %}

    {{ return(split_part_expr) }}

{% endmacro %}
