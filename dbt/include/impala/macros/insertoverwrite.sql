{% macro get_quoted_csv_exclude(column_names, exclude_name) %}

    {% set quoted = [] %}
    {% for col in column_names -%}
        {% if exclude_name|string() != col|string() %}
           {%- do quoted.append(adapter.quote(col)) -%}
        {% endif %}
    {%- endfor %}

    {%- set dest_cols_csv = quoted | join(', ') -%}
    {{ return(dest_cols_csv) }}

{% endmacro %}

{% macro get_insert_overwrite_sql(target, source, dest_columns) -%}

    {%- set partition_cols = config.get('partition_by', validator=validation.any[list]) -%}

    {% if partition_cols is not none %}
        {% if partition_cols is string %}
           {%- set partition_col = partition_cols -%}
        {% else %}
           {%- set partition_col = partition_cols[0] -%}
        {% endif %}

        {%- set dest_cols_csv = get_quoted_csv_exclude(dest_columns | map(attribute="name"), "") -%}
        {%- set dest_cols_csv_exclude = get_quoted_csv_exclude(dest_columns | map(attribute="name"), partition_col) -%}

        insert overwrite {{ target }} ({{ dest_cols_csv_exclude }}) partition({{ partition_col }})
            select {{ dest_cols_csv }}
            from {{ source }}
    {% else %}
        {%- set dest_cols_csv = get_quoted_csv_exclude(dest_columns | map(attribute="name"), "") -%}

        insert into {{ target }} ({{ dest_cols_csv }})
        (
            select {{ dest_cols_csv }}
            from {{ source }}
        )
    {% endif %}

{%- endmacro %}

