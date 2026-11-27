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

{% macro impala__get_incremental_merge_sql(arg_dict) %}
  {% do return(get_merge_sql(
      arg_dict["target_relation"],
      arg_dict["temp_relation"],
      arg_dict["unique_key"],
      arg_dict["dest_columns"],
      arg_dict["incremental_predicates"]
  )) %}
{% endmacro %}

{% macro impala__merge_unique_keys(unique_key) -%}
  {%- if unique_key is sequence and unique_key is not mapping and unique_key is not string -%}
    {{ return(unique_key) }}
  {%- else -%}
    {{ return([unique_key]) }}
  {%- endif -%}
{%- endmacro %}

{% macro impala__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
  {%- if config.get('stored_as') == 'kudu' -%}
    {{ impala__get_kudu_upsert_sql(target, source, unique_key, dest_columns, incremental_predicates) }}
  {%- else -%}
    {{ impala__get_iceberg_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) }}
  {%- endif -%}
{%- endmacro %}

{% macro impala__get_iceberg_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
  {#- Impala MERGE INTO works only for Iceberg tables. -#}
  {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
  {%- set dest_cols_csv = dest_columns | map(attribute="name") | join(", ") -%}
  {%- set merge_update_columns = config.get("merge_update_columns") -%}
  {%- set merge_exclude_columns = config.get("merge_exclude_columns") -%}
  {%- set update_columns = get_merge_update_columns(
        merge_update_columns, merge_exclude_columns, dest_columns
      ) -%}
  {%- set sql_header = config.get("sql_header", none) -%}
  {%- set unique_keys = impala__merge_unique_keys(unique_key) -%}

  {% for key in unique_keys %}
    {% set this_key_match %}
      DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
    {% endset %}
    {% do predicates.append(this_key_match) %}
  {% endfor %}

  {{ sql_header if sql_header is not none }}

  merge into {{ target }} as DBT_INTERNAL_DEST
      using {{ source }} as DBT_INTERNAL_SOURCE
      on {{ "(" ~ predicates | join(") and (") ~ ")" }}

  when matched then update set
      {% for column_name in update_columns -%}
          {%- set col = column_name | replace('"', '') | replace('`', '') -%}
          {{ col }} = DBT_INTERNAL_SOURCE.{{ col }}
          {%- if not loop.last %}, {% endif %}
      {%- endfor %}

  when not matched then insert
      ({{ dest_cols_csv }})
  values
      (
        {%- for col in dest_columns -%}
          DBT_INTERNAL_SOURCE.{{ col.name }}
          {%- if not loop.last %}, {% endif %}
        {%- endfor -%}
      )
{%- endmacro %}

{% macro impala__get_kudu_upsert_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
  {#-
    Impala rejects MERGE INTO for Kudu targets. UPSERT provides merge semantics
    on the Kudu primary key. Excluded/update column lists are applied by joining
    the existing target row when present.
  -#}
  {%- set merge_update_columns = config.get("merge_update_columns") -%}
  {%- set merge_exclude_columns = config.get("merge_exclude_columns") -%}
  {%- set update_columns = get_merge_update_columns(
        merge_update_columns, merge_exclude_columns, dest_columns
      ) -%}
  {%- set update_colnames = [] -%}
  {% for column_name in update_columns %}
    {% do update_colnames.append(column_name | replace('"', '') | replace('`', '') | lower) %}
  {% endfor %}
  {%- set unique_keys = impala__merge_unique_keys(unique_key) -%}
  {%- set unique_key_lower = unique_keys | map('lower') | list -%}
  {%- set dest_cols_csv = dest_columns | map(attribute="name") | join(", ") -%}
  {%- set sql_header = config.get("sql_header", none) -%}
  {%- set join_predicates = [] -%}
  {% for key in unique_keys %}
    {% do join_predicates.append("DBT_INTERNAL_SOURCE." ~ key ~ " = DBT_INTERNAL_DEST." ~ key) %}
  {% endfor %}
  {%- if incremental_predicates -%}
    {% for predicate in incremental_predicates %}
      {% do join_predicates.append(predicate) %}
    {% endfor %}
  {%- endif -%}

  {{ sql_header if sql_header is not none }}

  upsert into {{ target }} ({{ dest_cols_csv }})
  select
    {% for col in dest_columns -%}
      {%- set col_name = col.name -%}
      {%- if col_name | lower in unique_key_lower or col_name | lower in update_colnames -%}
        DBT_INTERNAL_SOURCE.{{ col_name }}
      {%- else -%}
        case
          when DBT_INTERNAL_DEST.{{ unique_keys[0] }} is not null
            then DBT_INTERNAL_DEST.{{ col_name }}
          else DBT_INTERNAL_SOURCE.{{ col_name }}
        end
      {%- endif -%}
      {%- if not loop.last %}, {% endif %}
    {%- endfor %}
  from {{ source }} as DBT_INTERNAL_SOURCE
  left join {{ target }} as DBT_INTERNAL_DEST
    on {{ "(" ~ join_predicates | join(") and (") ~ ")" }}
{%- endmacro %}
