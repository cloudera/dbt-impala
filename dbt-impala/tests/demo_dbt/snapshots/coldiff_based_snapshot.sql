/*
   Sample snapshot based on col check strategy
 */

{% snapshot coldiff_based_snapshot %}

{{
    config(
      target_database='analytics',
      target_schema='snapshots',
      unique_key='id',

      strategy='check',
      check_cols=['name'], 
    )
}}

select * from {{ ref('seed_sample') }}

{% endsnapshot %}