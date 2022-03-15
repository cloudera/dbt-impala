/*
   Sample snapshot based on timestamp strategy
 */

{% snapshot time_based_snapshot %}

{{
    config(
      target_database='analytics',
      target_schema='snapshots',
      unique_key='id',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ ref('seed_sample') }}

{% endsnapshot %}