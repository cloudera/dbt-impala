/*
    Sample usage of view creation config
 */
{{
    config(
        materialized='view',
        unique_key='id',
        comment="the view model",
    )
}}

with source_data as (
     select 1 as id, true as bool_val, "Name 1" as str_val,  "Name 1" as name
     union all 
     select 2 as id, false as bool_val, "Name 2" as str_val, "Name 2" as name
     union all 
     select 3 as id, false as bool_val, "Name 3" as str_val, "Name 3" as name
     union all 
     select 4 as id, false as bool_val, "Name 4" as str_val, "Name 4" as name
)

select * from source_data
