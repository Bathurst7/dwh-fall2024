select * from information_schema.tables
where table_schema = 'public';

-- fact generation:
with fact_order_details as (select 
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
        on o.ship_via = s.shipper_id),

-- dimension products

dim_products as (select 
    p.*,
    c.category_name,
    s.company_name,
    s.city supplier_city,
    s.country supplier_country
from products p
    left join categories c on p.category_id = c.category_id
    left join suppliers s on p.supplier_id = s.supplier_id),

-- dimension customers
dim_customers as (select
    c.customer_id,
    c.company_name customer_name,
    c.city,
    c.country
from customers c),

-- dimnesion employees
cte1 as (
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
group by e.employee_id),

dim_employees as (select *
from cte1
    full join cte2
    using (employee_id))


select
    dc.customer_id,
    dc.customer_name,
    date_diff(current_date(), max(fod.order_date)) as recency,
    count(distinct fod.fact_id) as frequency,
    sum(fod.unit_price * fod.quantity * (1 - fod.discount)) AS monetary
from fact_order_details fod
    left join dim_customers dc
        on fod.customer_id = dc.customer_id;


-- Fact generation
WITH fact_order_details AS (
    SELECT 
        (ROW_NUMBER() OVER())::INT AS fact_id, 
        od.*, -- All columns from the order details
        o.customer_id,
        o.employee_id,
        o.order_date,
        o.required_date,
        o.shipped_date,
        s.company_name AS shipper_name,
        o.ship_address,
        o.ship_city,
        o.ship_country
    FROM order_details od 
    LEFT JOIN orders o 
        ON od.order_id = o.order_id
    LEFT JOIN shippers s
        ON o.ship_via = s.shipper_id
),

-- Dimension products
dim_products AS (
    SELECT 
        p.*,
        c.category_name,
        s.company_name AS supplier_name,
        s.city AS supplier_city,
        s.country AS supplier_country
    FROM products p
    LEFT JOIN categories c 
        ON p.category_id = c.category_id
    LEFT JOIN suppliers s 
        ON p.supplier_id = s.supplier_id
),

-- Dimension customers
dim_customers AS (
    SELECT
        c.customer_id,
        c.company_name AS customer_name,
        c.city,
        c.country
    FROM customers c
),

-- Dimension employees
cte1 AS (
    SELECT 
        e.employee_id,
        CONCAT(e.title_of_courtesy, ' ', e.first_name, ' ', e.last_name) AS employee_full_name,
        e.title,
        CONCAT(m.title_of_courtesy, ' ', m.first_name, ' ', m.last_name) AS manager_full_name,
        e.hire_date
    FROM employees e
    FULL JOIN employees m
        ON e.reports_to = m.employee_id
    WHERE e.employee_id IS NOT NULL
),
cte2 AS (
    SELECT 
        e.employee_id,
        STRING_AGG(DISTINCT t.territory_description, ',') AS list_of_territories_managed,
        STRING_AGG(DISTINCT r.region_description, ',') AS region_managed
    FROM employee_territories et
    LEFT JOIN employees e
        ON et.employee_id = e.employee_id
    LEFT JOIN territories t 
        ON et.territory_id = t.territory_id
    LEFT JOIN region r
        ON t.region_id = r.region_id
    GROUP BY e.employee_id
),
dim_employees AS (
    SELECT *
    FROM cte1
    FULL JOIN cte2
    USING (employee_id)
)

-- Final RFM Query
SELECT
    dc.customer_id,
    dc.customer_name,
    (CURRENT_DATE - MAX(fod.order_date))::INTEGER AS recency, -- Recency in days
    COUNT(DISTINCT fod.fact_id) AS frequency, -- Count of unique orders
    SUM(fod.unit_price * fod.quantity * (1 - fod.discount)) AS monetary -- Total revenue
FROM fact_order_details fod
LEFT JOIN dim_customers dc
    ON fod.customer_id = dc.customer_id
GROUP BY dc.customer_id, dc.customer_name
ORDER BY recency, frequency DESC, monetary DESC;

select max(order_date)
from orders

select current_date