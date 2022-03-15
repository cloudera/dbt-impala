
-- Use the `ref` function to select from other models

select *
from {{ ref('simple_model') }}
where id = 1
