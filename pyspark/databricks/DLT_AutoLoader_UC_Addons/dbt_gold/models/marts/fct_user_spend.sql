-- models/marts/fct_user_spend.sql
-- Simple fct based on DLT gold table; useful for exposures & tests
select
  user_id,
  total_spent_30d,
  txn_count_30d,
  last_txn_at
from {{ source('core','gold_user_spend') }}
