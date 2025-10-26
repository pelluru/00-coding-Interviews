
{{ config(materialized='incremental', unique_key='customer_id', incremental_strategy='merge') }}

with src as (select * from {{ ref('stg_customer') }})
select customer_id, name, city, current_timestamp() as load_ts from src
