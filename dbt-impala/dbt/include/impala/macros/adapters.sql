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

{% macro impala__current_timestamp() -%}
  current_timestamp()
{%- endmacro %}

{% macro impala__snapshot_string_as_time(timestamp) -%}
    {%- set result = "to_timestamp('" ~ timestamp ~ "')" -%}
    {{ return(result) }}
{%- endmacro %}

{% macro impala__list_schemas(database) %}
    {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
        show databases
    {%- endcall %}

    {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro impala__list_relations_without_caching(relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    show tables in {{ relation }}
  {% endcall %}

  {% do return(load_result('list_relations_without_caching').table) %}
{% endmacro %}

{% macro impala__create_table_as(temporary, relation, sql) -%}

  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set backup = config.get('backup') -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary -%}temporary{%- endif %} table
    {{ relation.include(schema=(not temporary)) }}
    {% if backup == false -%}backup no{%- endif %}
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
    drop schema if exists {{ relation }}
  {%- endcall -%}
{% endmacro %}

{% macro impala__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro impala__rename_relation(from_relation, to_relation) -%}
  {% call statement('drop_relation') %}
    drop {{ to_relation.type }} if exists {{ to_relation }}
  {% endcall %}
  {% call statement('rename_relation') -%}
    {% if not from_relation.type %}
      {% do exceptions.raise_database_error("Cannot rename a relation with a blank type: " ~ from_relation.identifier) %}
    {% elif from_relation.type in ('table') %}
        alter table {{ from_relation }} rename to {{ to_relation }}
    {% elif from_relation.type == 'view' %}
        alter view {{ from_relation }} rename to {{ to_relation }}
    {% else %}
      {% do exceptions.raise_database_error("Unknown type '" ~ from_relation.type ~ "' for relation: " ~ from_relation.identifier) %}
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
