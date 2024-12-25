-- overview of the dataset
SELECT 
    * 
FROM INFORMATION_SCHEMA.TABLES
WHERE
    table_schema = 'public';

-- inspect the orders table (higher level of detail compared to the order_details)
-- take a look on the orders table
SELECT
    *
FROM orders
LIMIT 10;

-- Order is processed by (employee being in charged)
SELECT 
    o.order_id,
    o.customer_id,
    o.freight,
    o.order_date,
    o.required_date,
    o.employee_id,
    e.title,
    CONCAT(e.title_of_courtesy, ' ', e.first_name, ' ', e.last_name) employee_in_charge
FROM orders o
    LEFT JOIN employees e
    ON o.employee_id = e.employee_id;

-- List all the territories has been used
SELECT 
    e.employee_id,
    CONCAT(e.title_of_courtesy, ' ', e.first_name, ' ', e.last_name) employee,
    t.territory_id,
    t.territory_description,
    CONCAT(r.region_description, ' of USA')
FROM employee_territories et
    LEFT JOIN employees e 
    ON et.employee_id = e.employee_id
    LEFT JOIN territories t 
    ON et.territory_id = t.territory_id
    LEFT JOIN region r 
    ON t.region_id = r.region_id;

-- Map all the orders with the respective territories
WITH cte_orders AS (
    SELECT 
        o.order_id,
        o.customer_id,
        o.freight,
        o.order_date,
        o.required_date,
        o.employee_id,
        o.ship_city,
        o.ship_country,
        o.ship_region,
        e.title,
        CONCAT(e.title_of_courtesy, ' ', e.first_name, ' ', e.last_name) employee_in_charge
    FROM orders o
        LEFT JOIN employees e
        ON o.employee_id = e.employee_id
),
cte_territories AS (
    SELECT 
        e.employee_id,
        t.territory_id,
        t.territory_description,
        CONCAT(r.region_description, ' of USA') territory_region
    FROM employee_territories et
        LEFT JOIN employees e 
        ON et.employee_id = e.employee_id
        LEFT JOIN territories t 
        ON et.territory_id = t.territory_id
        LEFT JOIN region r 
        ON t.region_id = r.region_id
),

cte_customers AS (
    SELECT 
        customer_id,
        company_name ship_to_company,
        city customer_city,
        country customer_country,
        region customer_region
    FROM customers 
),

cte_order_details AS (
    SELECT 
        od.order_id, 
        od.product_id, 
        p.product_name,
        od.unit_price,
        od.quantity,
        od.discount, 
        c.category_name, 
        s.company_name suppliers_company,
        s.city suppliers_city,
        s.country suppliers_country,
        s.region suppliers_region
    FROM order_details od
        LEFT JOIN products p
        ON od.product_id = p.product_id
        LEFT JOIN categories c
        ON p.category_id = c.category_id
        LEFT JOIN suppliers s 
        ON p.supplier_id = s.supplier_id
)

SELECT *
FROM cte_orders co
    LEFT JOIN cte_territories ct
    ON co.employee_id = ct.employee_id
    LEFT JOIN cte_customers cc
    ON co.customer_id = cc.customer_id
    RIGHT JOIN cte_order_details cod
    ON co.order_id = cod.order_id;