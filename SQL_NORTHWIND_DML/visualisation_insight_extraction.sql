-- criteria 1: employee performances'

-- a look on employee table
select * from employees limit 10;

-- customer's total handled order
-- modify the view of the employee table
with mapping_region_customer as (
    select e.employee_id, r.region_description
    from employees e
        left join employee_territories et on e.employee_id = et.employee_id
        left join territories t on et.territory_id = t.territory_id
        left join region r on t.region_id = r.region_id
),

customed_viewed_employee as (
select 
    e.employee_id,
    e.title,
    concat(e.title_of_courtesy, ' ', e.first_name, ' ', e.last_name) employee_full_name,
    e.hire_date,
    concat(m.title_of_courtesy, ' ', m.first_name, ' ', m.last_name) managed_by,
    coalesce(string_agg(distinct mrc.region_description, ', '), 'No region assigned') in_charged_of_region
from employees e
    full join employees m
        on e.reports_to = m.employee_id
    left join mapping_region_customer mrc
        on e.employee_id = mrc.employee_id
where e.employee_id is not null
group by 1, 2, 3, 4, 5
order by 1)

-- total orders that an employee has handled
select 
    e.*,
    count(order_id) total_order_handled
from orders o
    left join customed_viewed_employee e
        on o.employee_id = e.employee_id
group by 1, 2, 3, 4, 5, 6
order by 7 desc;

-- regions' amount of order processed
-- modify the view of the employee table
with mapping_region_customer as (
    select e.employee_id, r.region_description
    from employees e
        left join employee_territories et on e.employee_id = et.employee_id
        left join territories t on et.territory_id = t.territory_id
        left join region r on t.region_id = r.region_id
),

customed_viewed_employee as (
select 
    e.employee_id,
    e.title,
    concat(e.title_of_courtesy, ' ', e.first_name, ' ', e.last_name) employee_full_name,
    e.hire_date,
    concat(m.title_of_courtesy, ' ', m.first_name, ' ', m.last_name) managed_by,
    coalesce(string_agg(distinct mrc.region_description, ', '), 'No region assigned') in_charged_of_region
from employees e
    full join employees m
        on e.reports_to = m.employee_id
    left join mapping_region_customer mrc
        on e.employee_id = mrc.employee_id
where e.employee_id is not null
group by 1, 2, 3, 4, 5
order by 1),

-- total orders that an employee has handled
customed_joined_region as (select 
    e.*,
    count(order_id) total_order_handled
from orders o
    left join customed_viewed_employee e
        on o.employee_id = e.employee_id
group by 1, 2, 3, 4, 5, 6
order by 7 desc)

select in_charged_of_region region, sum(total_order_handled)
from customed_joined_region
group by 1
order by 2 desc;

-- profit generated per orders
-- modify the view of the employee table
with mapping_region_customer as (
    select e.employee_id, r.region_description
    from employees e
        left join employee_territories et on e.employee_id = et.employee_id
        left join territories t on et.territory_id = t.territory_id
        left join region r on t.region_id = r.region_id),

customed_viewed_employee as (
select 
    e.employee_id,
    e.title,
    concat(e.title_of_courtesy, ' ', e.first_name, ' ', e.last_name) employee_full_name,
    e.hire_date,
    concat(m.title_of_courtesy, ' ', m.first_name, ' ', m.last_name) managed_by,
    coalesce(string_agg(distinct mrc.region_description, ', '), 'No region assigned') in_charged_of_region
from employees e
    full join employees m
        on e.reports_to = m.employee_id
    left join mapping_region_customer mrc
        on e.employee_id = mrc.employee_id
where e.employee_id is not null
group by 1, 2, 3, 4, 5),

-- calculate the pricepaid per orders
total_price_paid_per_order as (
select o.order_id, 
    sum(od.unit_price*od.quantity*(1-od.discount)) price_paid_per_order
from orders o
    left join order_details od
        on o.order_id = od.order_id
group by o.order_id),

-- total orders that an employee has handled
customed_joined_region as (select 
    e.*,
    count(o.order_id) total_order_handled,
    sum(tppp.price_paid_per_order) total_price_paid
from orders o
    left join customed_viewed_employee e
        on o.employee_id = e.employee_id
    left join total_price_paid_per_order tppp
        on o.order_id = tppp.order_id
group by 1, 2, 3, 4, 5, 6)

select * from customed_joined_region;

-- 