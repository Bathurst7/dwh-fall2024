-- check all the table's name
select *
from information_schema.tables
where table_schema = 'public';
-- check the table customers
select *
from information_schema.columns
where table_name = 'customers';
-- check the table customer_customer_demo
select *
from information_schema.columns
where table_name = 'customer_customer_demo';