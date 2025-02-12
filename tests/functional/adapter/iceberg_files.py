merge_iceberg_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy='merge',
    merge_exclude_columns=['msg'],
    table_type='iceberg'
) }}
{% if not is_incremental() %}
-- data for first invocation of model
select CAST(1 AS INT) as id, 'hello' as msg, 'blue' as color
union all
select CAST(2 AS INT) as id, 'goodbye' as msg, 'red' as color
{% else %}
-- data for subsequent incremental update
select CAST(1 AS INT) as id, 'hey' as msg, 'blue' as color
union all
select CAST(2 AS INT) as id, 'yo' as msg, 'green' as color
union all
select CAST(3 AS INT) as id, 'anyway' as msg, 'purple' as color
{% endif %}
"""
