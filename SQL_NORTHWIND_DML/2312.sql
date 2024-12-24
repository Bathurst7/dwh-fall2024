select  distinct(p.product_name), c.category_name, c.description
from products p
    left join categories c on p.category_id = c.category_id
order by 3;

select product_id, product_name, units_in_stock, units_on_order, reorder_level, discontinued
from products;

select distinct discontinued from products;

select p.product_id, p.product_name, sum(od.quantity) total_quantity_on_order
from order_details od
    left join products p 
        on od.product_id = p.product_id
    left join orders o
        on od.order_id = o.order_id
where p.product_id = 3 and o.order_date = (select max(order_date) from orders)
group by 1, 2;

select title, title_of_courtesy, count(*)
from employees
group by 1, 2;

select *
from information_schema.columns
where table_name = 'employees';

select count(*)
from employees
where title_of_courtesy is null;

select * from employees;

select e.employee_id, concat(e.first_name, ' ',e.last_name), count(*), e.title, e.title_of_courtesy, e.city
from orders o
    left join employees e 
        on o.employee_id = e.employee_id
group by 1
order by 3 desc;

select e.city, count(*)
from orders o
    left join employees e 
        on o.employee_id = e.employee_id
group by 1
order by 2 desc;

select min(o.order_date), max(o.order_date), e.city
from orders o
    left join employees e
        on o.employee_id = e.employee_id
group by 3
order by 1;

select max(order_date)
from orders;

select distinct(ship_name), count(*), shippers.company_name
from orders
group by 1, 3;

select *
from orders;

select o.ship_city, e.city employee_city, concat(e.first_name, ' ', e.last_name) full_name, c.city customer_city, count(*), (c.city = o.ship_city)
from orders o
    left join employees e
        on 1=1 and o.employee_id = e.employee_id
    left join customers c
        on 1=1 and o.customer_id = c.customer_id
group by 1, 2, 3, 4;

select p.product_id, p.product_name, p.supplier_id
from products p;

select s.company_name, count(product_id)
from suppliers s
    left join products p 
         on p.supplier_id = s.supplier_id
group by 1
order by 2 desc;

select discontinued, count(*)
from products
group by 1;

select ship_country, count(*)
from orders
group by 1
order by 2 desc;

select * from territories t
left join region r on t.region_id = r.region_id;

select e.city, t.territory_description, r.region_description
from employees e 
    left join employee_territories et on e.employee_id = et.employee_id
    left join territories t on et.territory_id = t.territory_id
    left join region r on t.region_id = t.region_id
order by 3;

with cte1 as (select 
    o.order_id,
    c.city customer_city,
    o.ship_city ship_city, 
    o.ship_country ship_country, 
    concat(e.first_name, ' ', e.last_name) full_name, 
    e.city employee_city, e.country employee_country, 
    t.territory_description,
    r.region_description
from orders o 
    left join customers c on o.customer_id = c.customer_id
    left join employees e on o.employee_id = e.employee_id
    left join employee_territories et on e.employee_id = et.employee_id
    left join territories t on et.territory_id = t.territory_id
    left join region r on t.region_id = r.region_id),

cte2 as (select 
    od.order_id,
    s.city supplier_city, 
    s.country supplier_country
    -- s.region supplier_region
from order_details od
    left join products p on od.product_id = p.product_id
    left join suppliers s on p.supplier_id = s.supplier_id)

select *
from cte2
    left join cte1 on cte2.order_id = cte1.order_id
order by full_name;

select
    r.region_description, t.territory_description
from employees e
    left join employee_territories et on e.employee_id = et.employee_id
    left join territories t on et.territory_id = t.territory_id
    left join region r on t.region_id = r.region_id
order by 1;

select * from territories
left join region on territories.region_id = region.region_id
order by 2;