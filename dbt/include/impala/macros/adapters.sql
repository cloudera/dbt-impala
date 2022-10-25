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

{% macro ct_option_partition_cols(label, required=false) %}
  {%- set cols = config.get('partition_by', validator=validation.any[list, basestring]) -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {{ label }} (
    {%- for item in cols -%}
      {{ item }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{%- endmacro -%}

{% macro ct_option_sort_cols(label, required=false) %}
  {%- set cols = config.get('sort_by', validator=validation.any[list, basestring]) -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {{ label }} (
    {%- for item in cols -%}
      {{ item }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{%- endmacro -%}

{% macro ct_option_comment_relation(label, required=false) %}
  {%- set comment = config.get('comment', validator=validation[basestring]) -%}

  {%- if comment is not none %}
    {{label}} '{{ comment | replace("'", "\\'") }}'
  {%- endif %}
{%- endmacro -%}

{% macro ct_option_row_format(label, required=false) %}
  {%- set rowFormat = config.get('row_format', validator=validation[basestring]) -%}

  {%- if rowFormat is not none %}
    {{label}} {{ rowFormat }}
  {%- endif %}
{%- endmacro -%}

{% macro ct_option_with_serdeproperties(label, required=false) %}
  {%- set serdeproperties = config.get('serde_properties') -%}

  {%- if serdeproperties is not none %}
    {{label}} {{serdeproperties}}
  {%- endif %}
{%- endmacro -%}

{% macro ct_option_stored_as(label, required=false) %}
  {%- set storedAs = config.get('stored_as', validator=validation[basestring]) -%}

  {%- if storedAs is not none %}
    {{label}} {{storedAs}}
  {%- endif %}
{%- endmacro -%}

{% macro ct_option_location_clause(label, required=false) %}
  {%- set location = config.get('location', validator=validation[basestring]) -%}

  {%- if location is not none %}
    {{label}} '{{ location }}'
  {%- endif %}
{%- endmacro -%}

{% macro ct_option_cached_in(label, required=false) %}
  {%- set isCached = config.get('is_cached', validator=validation[basestring]) -%}
  {%- set cachePool = config.get('cache_pool', validator=validation[basestring]) -%}
  {%- set withReplication = config.get('replication_factor', validator=validation[basestring]) -%}
  {%- set unCached = 'uncached' -%}

  {%- if isCached is not none %}
    {% if isCached == true %}
      {% if withReplication is not none %}
        {{label}} '{{ cachePool }}' with replication={{ withReplication }}
      {% else %}
        {{label}} '{{ cachePool }}'
      {% endif %}
    {% else %}
      {{unCached}}
    {% endif %}
  {%- endif %}
{%- endmacro -%}

{% macro ct_option_tbl_properties(label, required=false) %}
  {%- set tblProperties = config.get('tbl_properties') -%}

  {%- if tblProperties is not none %}
    {{label}} {{tblProperties}}
  {%- endif %}
{%- endmacro -%}

/* to_timestamp function in impala takes 2 arguements, second being format and mandatory */
{% macro impala__snapshot_string_as_time(timestamp) -%}
    {%- set result = "to_timestamp('" ~ timestamp ~ "', 'yyyy-MM-dd HH:mm:ss.SSSSSS')" -%}
    {{ return(result) }}
{%- endmacro %}

{% macro impala__list_schemas(database) %}
    {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
        show databases
    {%- endcall %}

    {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{# Note: This function currently needs to query each object to determine its type. Depending on the schema, this function could be expensive. #}
{% macro impala__list_relations_without_caching(relation) %}
  {% set result_set = run_query('show tables in ' ~ relation) %}
  {% set objects_with_type = [] %}

  {%- for rs in result_set -%}
    {% set obj_type = [] %}

    {% do obj_type.append(rs[0]) %}

    {% set obj_rel = relation.new_copy(relation.schema, rs[0]) %}
    {% set rel_type = get_relation_type(obj_rel) %}
    {% do obj_type.append(rel_type) %}

    {% do objects_with_type.append(obj_type) %}
  {%- endfor -%}

  {{ return(objects_with_type) }}
{% endmacro %}

{% macro impala__create_table_as(temporary, relation, sql) -%}

  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set is_external = config.get('external') -%}

  {{ sql_header if sql_header is not none }}

  create {% if is_external == true -%}external{%- endif %} table
    {{ relation.include(schema=true) }}
    {{ ct_option_partition_cols(label="partitioned by") }}
    {{ ct_option_sort_cols(label="sort by") }}
    {{ ct_option_comment_relation(label="comment") }}
    {{ ct_option_row_format(label="row format") }}
    {{ ct_option_with_serdeproperties(label="with serdeproperties") }}
    {{ ct_option_stored_as(label="stored as") }}
    {{ ct_option_location_clause(label="location") }} 
    {{ ct_option_cached_in(label="cached in") }}
    {{ ct_option_tbl_properties(label="tblproperties") }}
  as 
    {{ sql }}
  ;
{%- endmacro %}

{% macro impala__create_view_as(relation, sql) -%}

  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set backup = config.get('backup') -%}

  {{ sql_header if sql_header is not none }}

  create view
    {{ relation.include(schema=True) }}
    {{ ct_option_comment_relation(label="comment") }}
  as
    {{ sql }}
  ;
{%- endmacro %}

{% macro impala__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation }}
  {% endcall %}
{% endmacro %}

{% macro impala__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation }} cascade
  {%- endcall -%}
{% endmacro %}

{% macro impala__drop_relation(relation) -%}
  {% call statement('drop_relation_if_exists_table') %}
    drop table if exists {{ relation }}
  {% endcall %}
  {% call statement('drop_relation_if_exists_view') %}
    drop view if exists {{ relation }}
  {% endcall %}
{% endmacro %}

{% macro is_relation_present(relation) -%}
  {% set result_set = run_query('show tables in ' ~ relation.schema ~ ' like "' ~ relation.identifier.lower() ~ '"') %}

  {% if execute %}
    {%- for rs in result_set -%}
      {% do return(true) %}
    {%- endfor -%}
  {%- endif -%}
  
  {% do return(false) %}
{% endmacro %}

{% macro get_relation_type(relation) -%}
  {% set rel_type = 'table' %}
  {% set relation_exists = is_relation_present(relation) %}

  {%- if relation_exists -%}
    {% set result_set = run_query('describe extended ' ~ relation) %}
  
    {% if execute %}
      {%- for rs in result_set -%}
        {%- if rs[0].startswith('Table Type') -%}
          {%- if rs[1].startswith('VIRTUAL_VIEW') -%}
            {% set rel_type = 'view' %}
            {% do return(rel_type) %}
          {%- elif rs[1].startswith('MANAGED_TABLE') -%}
            {% set rel_type = 'table' %}
            {% do return(rel_type) %}
          {%- elif rs[1].startswith('EXTERNAL_TABLE') -%}
            {% set rel_type = 'table' %}
            {% do return(rel_type) %}
          {%- endif -%}
        {%- endif -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endif -%}
  
  {% do return(rel_type) %}
{% endmacro %}

{% macro impala__rename_relation(from_relation, to_relation) -%}
  {% set from_rel_type = get_relation_type(from_relation) %}
  
  {% call statement('drop_relation_if_exists_table') %}
    drop table if exists {{ to_relation }}
  {% endcall %}
  {% call statement('drop_relation_if_exists_view') %}
    drop view if exists {{ to_relation }};
  {% endcall %}
  {% call statement('rename_relation') -%}
    {% if not from_rel_type %}
      {% do exceptions.raise_database_error("Cannot rename a relation with a blank type: " ~ from_relation.identifier) %}
    {% elif from_rel_type == 'table' %}
        alter table {{ from_relation }} rename to {{ to_relation }}
    {% elif from_rel_type == 'view' %}
        alter view {{ from_relation }} rename to {{ to_relation }}
    {% else %}
      {% do exceptions.raise_database_error("Unknown type '" ~ from_rel_type ~ "' for relation: " ~ from_relation.identifier) %}
    {% endif %}
  {%- endcall %}
{% endmacro %}

{% macro impala__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      describe extended {{ relation.include(schema=(schema is not none)) }}
  {% endcall %}
  {% do return(load_result('get_columns_in_relation').table) %}
{% endmacro %}

{% macro impala__alter_column_type(relation, column_name, new_column_type) -%}
  {% call statement('alter_column_type') %}
    alter table {{ relation }} change {{ column_name }} {{ column_name }} {{ new_column_type }}
  {% endcall %}
{% endmacro %}

{% macro impala__truncate_relation(relation) -%}
    {% call statement('truncate_relation') -%}
      truncate table if exists {{ relation }}
    {%- endcall %}
{% endmacro %}

/* impala has two hash functions, both are not perfect hash function: fnv_hash and murmur_hash, 
   the earlier one seems be available from older version and hence is being used here */
{% macro impala__snapshot_hash_arguments(args) -%}
    hex(fnv_hash(concat({%- for arg in args -%}
        coalesce(cast({{ arg }} as varchar ), '')
        {% if not loop.last %} , '|',  {% endif %}
    {%- endfor -%})))
{%- endmacro %}

{% macro impala__build_snapshot_table(strategy, sql) %}
    select *,
        {{ strategy.scd_id }} as dbt_scd_id,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from,
        cast(nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as timestamp) as dbt_valid_to
    from (
        {{ sql }}
    ) sbq
{% endmacro %}

{% macro get_row_count(relation_name) %}
    {% call statement('row_count', fetch_result=True) %}
        select count(*) as count from {{relation_name}}
    {% endcall %}
    {%- set row_count = load_result('row_count') -%}

    {% do return(row_count['data'][0][0]) %}    
{% endmacro %}

{% macro get_new_inserts_count(relation_name) %}
    {% call statement('inserts_count', fetch_result=True) %}
        select count(*) as count from {{relation_name}} where {{relation_name}}.dbt_change_type='insert'
    {% endcall %}
    {%- set inserts_count = load_result('inserts_count') -%}

    {% do return(inserts_count['data'][0][0]) %}    
{% endmacro %}

{% macro fetch_rows_to_insert(target_relation, staging_table, insert_cols) %}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    insert into {{target_relation}} ({{insert_cols_csv}}) select {{insert_cols_csv}} from {{staging_table}}
{% endmacro %}

{% macro impala__generate_database_name(custom_database_name=none, node=none) -%}
  {% do return(None) %}
{%- endmacro %}

/* snapshots flow for impala */
{% materialization snapshot, adapter='impala' %}
  {%- set config = model['config'] -%}

  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}

  {% if not adapter.check_schema_exists(model.database, model.schema) %}
    {% do create_schema(model.database, model.schema) %}
  {% endif %}

  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=model.database,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

  {% if not target_relation_exists %}

      {% set build_sql = build_snapshot_table(strategy, model['compiled_sql']) %}
      {% set final_sql = create_table_as(False, target_relation, build_sql) %}

  {% else %}

      {{ adapter.valid_snapshot_target(target_relation) }}

      {% set staging_table = build_snapshot_staging_table(strategy, sql, target_relation) %}

      /*
      1. if the staging table has no entries, then simply do nothing
      2. if the staging table has entries, drop the existing snapshot table, build a new one 
       */

      {% set row_count = get_row_count(staging_table) %}
      {% set insert_count = get_new_inserts_count(staging_table) %}

      /* if the change includes anything apart from insert, rebuild the snapshot */
      {% if row_count > 0 and row_count != insert_count %}
        {% call statement('drop_snapshot') %}
            drop table {{target_relation}}
        {% endcall %}

        {% set build_sql = build_snapshot_table(strategy, model['compiled_sql']) %}
        {% set final_sql = create_table_as(False, target_relation, build_sql) %}
      {% elif row_count > 0 and row_count == insert_count %} /* insert, if all changes are of that type */
        
        {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

        {% set quoted_source_columns = [] %}
        {% for column in source_columns %}
          {% do quoted_source_columns.append(adapter.quote(column.name)) %}
        {% endfor %}
        
        {% set final_sql = fetch_rows_to_insert(target_relation, staging_table, quoted_source_columns) %}
      {% else %}
        {% set final_sql = 'select 1' %} /* dummy sql */
      {% endif %}

  {% endif %}

  {% call statement('main') %}
      {{ final_sql }}
  {% endcall %}

  {% do persist_docs(target_relation, model) %}

  {% if not target_relation_exists %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {% if staging_table is defined %}
    {% call statement('purge_staging') %}
      drop table {{staging_table}}
    {% endcall %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

