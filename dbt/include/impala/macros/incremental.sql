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

{% macro validate_get_incremental_strategy(raw_strategy) %}
  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    Expected one of: 'append', 'insert_overwrite'
  {%- endset %}

  {% if raw_strategy not in ['append', 'insert_overwrite'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(raw_strategy) %}
{% endmacro %}

{% macro incremental_validate_on_schema_change(on_schema_change, default='ignore') %}
   {% if on_schema_change not in ['fail', 'ignore'] %}
     {% set log_message = 'Invalid value for on_schema_change (%s) specified. Setting default value of %s.' % (on_schema_change, default) %}
     {% do log(log_message) %}
     
     {% do exceptions.raise_compiler_error(log_message) %}

     {{ return(default) }}
   {% else %}
     {{ return(on_schema_change) }}
   {% endif %}
{% endmacro %}

{% materialization incremental, adapter='impala' -%}

  {% set unique_key = config.get('unique_key') %}
  {% set overwrite_msg -%}
    impala adapter does not support 'unique_key'
  {%- endset %}
  {% if unique_key is not none %}
    {% do exceptions.raise_compiler_error(overwrite_msg) %}
  {% endif %}

  {% set raw_strategy = config.get('incremental_strategy', default='append') %}
  {% set strategy = validate_get_incremental_strategy(raw_strategy) %}
  
  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

  {%- set time_stamp = modules.datetime.datetime.now().isoformat().replace("-","").replace(":","").replace(".","") -%}

  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(target_relation, '__' + time_stamp + '__dbt_tmp') %}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {% set tmp_identifier = model['name'] + '__' + time_stamp + '__dbt_tmp' %}
  {% set backup_identifier = model['name'] + '__' + time_stamp + "__dbt_backup" %}

  -- the intermediate_ and backup_ relations should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation. This has to happen before
  -- BEGIN, in a separate transaction
  {% set preexisting_intermediate_relation = adapter.get_relation(identifier=tmp_identifier, 
                                                                  schema=schema,
                                                                  database=database) %}                                               
  {% set preexisting_backup_relation = adapter.get_relation(identifier=backup_identifier,
                                                            schema=schema,
                                                            database=database) %}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}

  {% do to_drop.append(tmp_relation) %}

  {# -- first check whether we want to full refresh for source view or config reasons #}
  {% set trigger_full_refresh = (full_refresh_mode or existing_relation.is_view) %}

  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif trigger_full_refresh %}
      {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
      {% set tmp_identifier = model['name'] + '__' + time_stamp + '__dbt_tmp' %}
      {% set backup_identifier = model['name'] + '__' + time_stamp  + '__dbt_backup' %}
      {% set intermediate_relation = existing_relation.incorporate(path={"identifier": tmp_identifier}) %}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}

      {% set build_sql = create_table_as(False, intermediate_relation, sql) %}
      {% set need_swap = true %}
      {% do to_drop.append(backup_relation) %}
      {% do to_drop.append(intermediate_relation) %}
  {% else %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% do adapter.expand_target_column_types(
             from_relation=tmp_relation,
             to_relation=target_relation) %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {#-- since unique key is not supported, the following macro (default impl), will only return insert stm, and hence is directly used here --#}
    {#-- set build_sql = get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns) --#}

    {% set build_sql = get_insert_overwrite_sql(target_relation, tmp_relation, dest_columns) %}
  
  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {% if need_swap %} 
      {% do adapter.rename_relation(target_relation, backup_relation) %} 
      {% do adapter.rename_relation(intermediate_relation, target_relation) %} 
  {% endif %}

  {% do persist_docs(target_relation, model) %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

