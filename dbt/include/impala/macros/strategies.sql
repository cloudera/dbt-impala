{#
# Copyright 2022 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#}

{% macro get_qualified_columnnames_csv(columns, qualifier='') %}
    {% set quoted = [] %}
    {% for col in columns -%}
        {% if qualifier != '' %}
          {%- do quoted.append(qualifier + '.' + col.name) -%}
        {% else %}
          {%- do quoted.append(col.name) -%}
        {% endif %}
    {%- endfor %}

    {%- set dest_cols_csv = quoted | join(', ') -%}
    {{ return(dest_cols_csv) }}

{% endmacro %}

{% macro impala__get_merge_sql(source, target, unique_key, dest_columns, predicates=none) %}
  {%- set predicates = [] if predicates is none else [] + predicates -%}
  {%- set merge_update_columns = config.get('merge_update_columns') -%}
  {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
  {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}

  {% if unique_key %}
      {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
          {% for key in unique_key %}
              {% set this_key_match %}
                  DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
              {% endset %}
              {% do predicates.append(this_key_match) %}
          {% endfor %}
      {% else %}
          {% set unique_key_match %}
              DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
          {% endset %}
          {% do predicates.append(unique_key_match) %}
      {% endif %}
  {% else %}
      {% do predicates.append('FALSE') %}
  {% endif %}

  merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on {{"(" ~ predicates | join(") and (") ~ ")"}}

  {% if unique_key %}
    when matched then update set
      {% for column_name in update_columns -%}
          {{ column_name | replace('"', "`") }} = DBT_INTERNAL_SOURCE.{{ column_name | replace('"', "`") }}
          {%- if not loop.last %}, {%- endif %}
      {%- endfor %}
  {% endif %}

  when not matched then insert
    ({{ get_qualified_columnnames_csv(dest_columns) }})
  values
    ({{ get_qualified_columnnames_csv(dest_columns, 'DBT_INTERNAL_SOURCE') }})

{% endmacro %}

{% macro impala__get_insert_overwrite_sql(source, target) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute="name") | join(", ") -%}
    {%- set partition_cols = config.get('partition_by', validator=validation.any[list]) -%}

    {% if partition_cols is not none %}
        {% if partition_cols is string %}
            {%- set partition_cols_csv = partition_cols -%}
        {% else %}
            {%- set partition_cols_csv = partition_cols | join(", ") -%}
        {% endif %}

        insert overwrite {{ target }} partition({{ partition_cols_csv }})
        (
            select {{ dest_cols_csv }}
            from {{ source }}
        )
    {% else %}
        {% do exceptions.raise_compiler_error("Impala adapter does not support 'insert_overwrite' if 'partition_cols' are not present") %}
    {% endif %}

{% endmacro %}

{% macro impala__get_incremental_sql(source, target) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute="name") | join(", ") -%}
    {%- set partition_cols = config.get('partition_by', validator=validation.any[list]) -%}

    {% if partition_cols is not none %}
        {% if partition_cols is string %}
            {%- set partition_cols_csv = partition_cols -%}
        {% else %}
            {%- set partition_cols_csv = partition_cols | join(", ") -%}
        {% endif %}

        insert into {{ target }} partition({{ partition_cols_csv }})
        (
            select {{ dest_cols_csv }}
            from {{ source }}
        )
    {% else %}
        insert into {{ target }} ({{ dest_cols_csv }})
        (
           select {{ dest_cols_csv }}
           from {{ source }}
        )
    {% endif %}

{% endmacro %}

{% macro get_transformation_sql(target, source, unique_key, dest_columns, predicates) -%}

    {% set raw_strategy = config.get('incremental_strategy') or 'append' %}

    {% if raw_strategy == 'insert_overwrite' %}
        {{ impala__get_insert_overwrite_sql(source, target) }}
    {% elif raw_strategy == 'merge' %}
        {{ impala__get_merge_sql(source, target, unique_key, dest_columns) }}
    {% else %}
        {{ impala__get_incremental_sql(source, target) }}
    {% endif %}

{%- endmacro %}
