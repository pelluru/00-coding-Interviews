
with src as (select * from {{ source('app','orders_raw') }})
select
  cast(order_id as string) as order_id,
  cast(customer_id as string) as customer_id,
  cast(order_ts as timestamp) as order_ts,
  total_amount
from src
