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
        is_cached=false,
        cache_pool='mypool',
        replication_factor=1,
        stored_as='PARQUET',
        tbl_properties="('k1'='v1', 'k2'='v2')",
        serde_properties="('k1'='v1', 'k2'='v2')",
        location="/root/test",
    )
}}

select id, bool_val, some_date, name from {{ ref('seed_sample') }}
