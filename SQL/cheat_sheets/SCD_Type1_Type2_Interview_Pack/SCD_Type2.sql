
-- SCD Type 2 Example
drop table if exists dim_customer;
create table dim_customer (
  sk_customer serial primary key,
  customer_id int,
  name text,
  city text,
  start_date date,
  end_date date,
  is_current boolean
);
insert into dim_customer (customer_id, name, city, start_date, end_date, is_current)
values (1,'Alex','LA','2024-01-01','9999-12-31',true);

update dim_customer
set end_date = current_date - 1, is_current = false
where customer_id = 1 and is_current = true;

insert into dim_customer (customer_id, name, city, start_date, end_date, is_current)
values (1,'Alex','NY',current_date,'9999-12-31',true);

select * from dim_customer order by sk_customer;
