
-- SCD Type 1 Example
drop table if exists dim_customer;
create table dim_customer(customer_id int primary key, name text, city text);
insert into dim_customer values (1,'Alex','LA'),(2,'Sam','NY');

merge into dim_customer t
using (select 1 as customer_id, 'Alex' as name, 'SF' as city) s
on t.customer_id = s.customer_id
when matched then update set t.name = s.name, t.city = s.city
when not matched then insert (customer_id, name, city) values (s.customer_id, s.name, s.city);

select * from dim_customer;
