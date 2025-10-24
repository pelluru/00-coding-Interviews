
with customers as (select * from {{ source('app','customers_raw') }})
select
  cast(customer_id as string) as customer_id,
  initcap(first_name || ' ' || last_name) as full_name,
  email,
  current_timestamp as dim_loaded_at
from customers
