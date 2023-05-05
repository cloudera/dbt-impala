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

{% macro get_insert_overwrite_sql(target, source, dest_columns) -%}

    {% set raw_strategy = config.get('incremental_strategy') or 'append' %}
    {%- set partition_cols = config.get('partition_by', validator=validation.any[list]) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute="name") | join(", ") -%}

    {% if partition_cols is not none %}
        {% if partition_cols is string %}
            {%- set partition_cols_csv = partition_cols -%}
        {% else %}
            {%- set partition_cols_csv = partition_cols | join(", ") -%}
        {% endif %}
        {{ print("partition_cols_csv = " + partition_cols_csv) }}

        {% if raw_strategy == 'insert_overwrite' %}

            insert overwrite {{ target }} partition({{ partition_cols_csv }})
            (
                select {{ dest_cols_csv }}
                from {{ source }}
            )

        {% elif raw_strategy == 'append' %}

            insert into {{ target }} partition({{ partition_cols_csv }})
            (
                select {{ dest_cols_csv }}
                from {{ source }}
            )

        {% endif %}
    {% else %}

        insert into {{ target }} ({{ dest_cols_csv }})
        (
            select {{ dest_cols_csv }}
            from {{ source }}
        )
    {% endif %}

{%- endmacro %}
