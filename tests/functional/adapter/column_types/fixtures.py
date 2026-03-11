macro_test_is_type_sql = """
{% macro simple_type_check_column(column, check) %}
    {% if check == 'string' %}
        {{ return(column.is_string()) }}
    {% elif check == 'float' %}
        {{ return(column.is_float()) }}
    {% elif check == 'number' %}
        {{ return(column.is_number()) }}
    {% elif check == 'numeric' %}
        {{ return(column.is_numeric()) }}
    {% elif check == 'integer' %}
        {{ return(column.is_integer()) }}
    {% else %}
        {% do exceptions.raise_compiler_error('invalid type check value: ' ~ check) %}
    {% endif %}
{% endmacro %}

{% macro type_check_column(column, type_checks) %}
    {% set failures = [] %}
    {% for type_check in type_checks %}
        {% if type_check.startswith('not ') %}
            {% if simple_type_check_column(column, type_check[4:]) %}
                {% do log('simple_type_check_column got ', True) %}
                {% do failures.append(type_check) %}
            {% endif %}
        {% else %}
            {% if not simple_type_check_column(column, type_check) %}
                {% do failures.append(type_check) %}
            {% endif %}
        {% endif %}
    {% endfor %}
    {% if (failures | length) > 0 %}
        {% do log('column ' ~ column.name ~ ' had failures: ' ~ failures, info=True) %}
    {% endif %}
    {% do return((failures | length) == 0) %}
{% endmacro %}

{% test test_is_type(model, column_map) %}
    {% if not execute %}
        {{ return(None) }}
    {% endif %}

    {% if not column_map %}
        {% do exceptions.raise_compiler_error('test_is_type must have a column name') %}
    {% endif %}

    {% set columns = adapter.get_columns_in_relation(model) %}

    {% if (column_map | length) != (columns | length) %}
        {% set column_map_keys = (column_map | list | string) %}
        {% set column_names = (columns | map(attribute='name') | list | string) %}
        {% do exceptions.raise_compiler_error(
            'did not get all the columns/all columns not specified:\n'
            ~ column_map_keys ~ '\nvs\n' ~ column_names
        ) %}
    {% endif %}

    {% set bad_columns = [] %}

    {% for column in columns %}
        {% set column_key = column.name | lower %}
        {% if column_key in column_map %}
            {% set type_checks = column_map[column_key] %}
            {% if not type_checks %}
                {% do exceptions.raise_compiler_error('no type checks?') %}
            {% endif %}
            {% if not type_check_column(column, type_checks) %}
                {% do bad_columns.append(column.name) %}
            {% endif %}
        {% else %}
            {% do exceptions.raise_compiler_error(
                'column key ' ~ column_key ~ ' not found in '
                ~ (column_map | list | string)
            ) %}
        {% endif %}
    {% endfor %}

    {% do log('bad columns: ' ~ bad_columns, info=True) %}

    {% for bad_column in bad_columns %}
        select '{{ bad_column }}' as bad_column
        {{ 'union all' if not loop.last }}
    {% endfor %}

    select * from (select 1 bad_columnas ) as nothing
    where false
{% endtest %}
"""

model_sql = """
select
    cast(0 as tinyint) as tinyint_col,
    cast(1 as smallint) as smallint_col,
    cast(2 as integer) as integer_col,
    cast(2 as int) as int_col,
    cast(3 as bigint) as bigint_col,
    cast(4.0 as float) as float_col,
    cast(5.0 as double) as double_col,
    cast(6.0 as decimal) as decimal_col,
    cast('a' as string) as string_col,
    cast('a' as char(1)) as char_col,
    cast('b' as varchar(20)) as varchar_col
"""
schema_yml = """
version: 2
models:
  - name: model
    tests:
      - test_is_type:
          column_map:
            tinyint_col: ['integer', 'number']
            smallint_col: ['integer', 'number']
            integer_col: ['integer', 'number']
            int_col: ['integer', 'number']
            bigint_col: ['integer', 'number']
            float_col: ['float', 'number']
            double_col: ['float', 'number']
            decimal_col: ['numeric', 'number']
            string_col: ['string', 'not number']
            char_col: ['string', 'not number']
            varchar_col: ['string', 'not number']
"""
