/*
    Sample incremental model
 */
{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='insert_overwrite',
    )
}}

select * from {{ ref('seed_sample') }}

{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
