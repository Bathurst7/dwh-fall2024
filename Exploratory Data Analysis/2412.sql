select product_id, product_name, units_in_stock, units_on_order, reorder_level, discontinued from products;
select * from categories;
select discontinued, count(*)
from products
group by 1;

select * from orders limit 10;
select * from order_details limit 10;
select distinct(ship_via) from orders;
select * from shippers;
select c.company_name, o.ship_name, (c.company_name = o.ship_name)
from orders o 
    left join customers c on o.customer_id = c.customer_id;