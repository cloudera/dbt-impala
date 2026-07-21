impala_model_contract_sql_header_sql = """
{{ config(
    materialized = "table"
) }}

select
  'blue' as color,
  1 as id,
  '2019-01-01' as date_day
"""

impala_model_incremental_contract_sql_header_sql = """
{{ config(
    materialized = "incremental",
    on_schema_change='fail'
) }}

select
  'blue' as color,
  1 as id,
  '2019-01-01' as date_day
"""

impala_my_model_incremental_wrong_order_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='fail'
  )
}}

select
  'blue' as color,
  1 as id,
  '2019-01-01' as date_day
"""

impala_my_model_incremental_wrong_name_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='fail'
  )
}}

select
  'blue' as color,
  1 as error,
  '2019-01-01' as date_day
"""

impala_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: tinyint
      - name: color
        data_type: string
      - name: date_day
        data_type: string

  - name: my_model_error
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: tinyint
      - name: color
        data_type: string
      - name: date_day
        data_type: string

  - name: my_model_wrong_order
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: tinyint
        description: hello
        tests:
          - unique
      - name: color
        data_type: string
      - name: date_day
        data_type: string

  - name: my_model_wrong_name
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: tinyint
        description: hello
        tests:
          - unique
      - name: color
        data_type: string
      - name: date_day
        data_type: string
"""


impala_model_quoted_column_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
      materialized: table
    columns:
      - name: id
        data_type: tinyint
        description: hello
      - name: from  # reserved word
        quote: true
        data_type: string
      - name: date_day
        data_type: string
"""

impala_model_contract_header_schema_yml = """
version: 2
models:
  - name: my_model_contract_sql_header
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: tinyint
        description: hello
        tests:
          - unique
      - name: color
        data_type: string
      - name: date_day
        data_type: string
"""

impala_foreign_key_model_sql = """
{{
  config(
    materialized = "table"
  )
}}

select cast(1 as int) as id
"""

impala_my_model_wrong_order_depends_on_fk_sql = """
{{
  config(
    materialized = "table"
  )
}}

-- depends_on: {{ ref('foreign_key_model') }}

select
  'blue' as color,
  cast(1 as int) as id,
  '2019-01-01' as date_day
"""

impala_constrained_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    constraints:
      - type: check
        expression: (id > 0)
      - type: check
        expression: id >= 1
      - type: primary_key
        columns: [ id ]
      - type: foreign_key
        columns: [ id ]
        expression: {schema}.foreign_key_model (id)
    columns:
      - name: id
        data_type: int
        description: hello
      - name: color
        data_type: string
      - name: date_day
        data_type: string
  - name: foreign_key_model
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [ id ]
    columns:
      - name: id
        data_type: int
"""
