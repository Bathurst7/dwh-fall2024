select * from information_schema.tables
where table_schema = 'public';

-- fact generation:
select 
    (row_number() over())::int fact_id, 
    od.*, -- all the column from the order details
    o.customer_id,
    o.employee_id,
    o.order_date,
    o.required_date,
    o.shipped_date,
    s.company_name,
    -- o.freight,
    o.ship_address,
    o.ship_city,
    o.ship_country
    -- o.ship_region,
    -- o.ship_postal_code
from order_details od 
    left join orders o 
        on od.order_id = o.order_id
    left join shippers s
        on o.ship_via = s.shipper_id;

-- dimension products
select 
    p.*,
    c.category_name,
    s.company_name,
    s.city supplier_city,
    s.country supplier_country
from products p
    left join categories c on p.category_id = c.category_id
    left join suppliers s on p.supplier_id = s.supplier_id;

-- dimension customers
select
    c.customer_id,
    c.company_name customer_name,
    c.city,
    c.country
from customers c;

-- dimnesion employees
with cte1 as (
select 
    e.employee_id,
    concat(e.title_of_courtesy, ' ',e.first_name, ' ', e.last_name) employee_full_name,
    e.title,
    concat(m.title_of_courtesy, ' ',m.first_name, ' ', m.last_name) manager_full_name,
    e.hire_date
from employees e
    full join employees m
        on e.reports_to = m.employee_id
where e.employee_id is not null),

cte2 as (select 
    e.employee_id,
    string_agg(distinct t.territory_description, ',') list_of_territories_managed,
    string_agg(distinct r.region_description, ',') region_managed
from employee_territories et
    left join employees e
        on et.employee_id = e.employee_id
    left join territories t 
        on et.territory_id = t.territory_id
    left join region r
        on t.region_id = r.region_id
group by e.employee_id)

select *
from cte1
    full join cte2
    using (employee_id);