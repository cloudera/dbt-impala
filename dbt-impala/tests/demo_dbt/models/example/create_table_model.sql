/*
    Sample usage of table creation config
 */
{{
    config(
        materialized='table',
        unique_key='id',
        partition_by=['name'],
        sort_by=['bool_val'],
        comment="the model",
        row_format='delimited',
        stored_as='PARQUET',
        tbl_properties="('dbt_test'='1')",
        serde_properties="('quoteChar'='\'', 'escapeChar'='\\')",
    )
}}

with source_data as (
     select 1 as id, true as bool_val, "Name 1" as str_val,  "Name 1" as name
     union all 
     select 2 as id, false as bool_val, "Name 2" as str_val, "Name 2" as name
     union all 
     select 3 as id, false as bool_val, "Name 3'" as str_val, "Name 3" as name
     union all 
     select 4 as id, false as bool_val, "Name 4\\" as str_val, "Name 4" as name
)

select * from source_data

