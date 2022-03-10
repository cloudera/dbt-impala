
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
