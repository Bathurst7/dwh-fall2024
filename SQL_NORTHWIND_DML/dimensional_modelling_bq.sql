-- CREATE DIM_SUPPLIERS
SELECT
  supplier_id,
  company_name,
  contact_name,
  city,
  country,
  region
FROM 
  `northwind.suppliers`;

-- CREATE DIM_PRODUCTS
SELECT
  p.product_id,
  p.product_name,
  c.category_name,
  p.units_in_stock,
  p.reorder_level,
  p.discontinued
FROM 
  `northwind.products` AS p
  LEFT JOIN 
    `northwind.categories` AS c
    ON p.category_id = c.category_id;

-- CREATE DIM_CUSTOMERS
SELECT
  customer_id,
  company_name,
  contact_name,
  city,
  country,
  region
FROM
  `northwind.customers`;

-- CREATE DIM_EMPLOYEES
SELECT
  CONCAT(e.first_name, ' ', e.last_name) AS employee_full_name,
  e.title,
  e.hire_date,
  r.region_description,
  'USA' AS country,
  COUNT(et.territory_id) AS number_of_territories_managing

FROM
  `northwind.employees` AS e
    LEFT JOIN
      `northwind.employee_territories` AS et
      ON e.employee_id = et.employee_id
    LEFT JOIN
      `northwind.territories` AS t
      ON et.territory_id = t.territory_id
    LEFT JOIN
      `northwind.region` AS r
      ON t.region_id = r.region_id
GROUP BY 1, 2, 3, 4;

-- CREATE DIM_DATE
WITH temp_orders AS (
  SELECT 
    * EXCEPT (order_date, required_date, shipped_date),
    TIMESTAMP_MILLIS(order_date) AS order_date,
    TIMESTAMP_MILLIS(required_date) AS required_date,
    TIMESTAMP_MILLIS(shipped_date) AS shipped_date
  FROM 
    `northwind.orders`
),
date_sequence AS (
  SELECT 
    GENERATE_DATE_ARRAY(
      DATE(MIN(order_date)), -- Start from the oldest order_date
      CURRENT_DATE(),        -- Up to today
      INTERVAL 1 DAY         -- 1-day interval
    ) AS dates
  FROM 
    temp_orders
)
SELECT 
  ROW_NUMBER() OVER(ORDER BY d) AS date_id,
  d AS date
FROM 
  date_sequence, 
  UNNEST(dates) AS d; -- Unnest the date array into individual rows

-- CREATE FACT-TABLE:
WITH temp_orders AS (
  SELECT 
    * EXCEPT (order_date, required_date, shipped_date),
    TIMESTAMP_MILLIS(order_date) AS order_date,
    TIMESTAMP_MILLIS(required_date) AS required_date,
    TIMESTAMP_MILLIS(shipped_date) AS shipped_date
  FROM 
    `northwind.orders`
),
date_sequence AS (
  SELECT 
    GENERATE_DATE_ARRAY(
      DATE(MIN(TIMESTAMP_MILLIS(order_date))), -- Start from the oldest order_date
      CURRENT_DATE(),                         -- Up to today
      INTERVAL 1 DAY                          -- 1-day interval
    ) AS dates
  FROM 
    `northwind.orders`
),
dim_date AS (
  SELECT 
    ROW_NUMBER() OVER(ORDER BY d) AS date_id,
    d AS date
  FROM 
    date_sequence, 
    UNNEST(dates) AS d -- Unnest the date array into individual rows
)
-- Create the fact table with mapped date IDs
SELECT
  ROW_NUMBER() OVER(ORDER BY od.order_id, od.product_id) AS fact_id,
  od.order_id,
  od.product_id,
  od.quantity,
  (od.unit_price * od.quantity) AS extended_price,
  od.discount,
  (od.unit_price * od.quantity) * (1 - od.discount) AS total_price,
  CASE 
    WHEN o.shipped_date IS NULL THEN 'fulfilled'
    WHEN o.shipped_date IS NOT NULL THEN 'not fulfilled'
  END AS order_status,
  o.customer_id,
  p.supplier_id,
  o.employee_id,
  dd_order.date_id AS order_date_id,       -- Mapped order_date ID
  dd_required.date_id AS required_date_id, -- Mapped required_date ID
  dd_shipped.date_id AS shipped_date_id    -- Mapped shipped_date ID
FROM 
  `northwind.order_details` AS od
  LEFT JOIN 
    `northwind.orders` AS o
    ON od.order_id = o.order_id
  LEFT JOIN
    `northwind.products` AS p
    ON od.product_id = p.product_id
  LEFT JOIN 
    dim_date AS dd_order
    ON DATE(TIMESTAMP_MILLIS(o.order_date)) = dd_order.date -- Map order_date to date_id
  LEFT JOIN 
    dim_date AS dd_required
    ON DATE(TIMESTAMP_MILLIS(o.required_date)) = dd_required.date -- Map required_date to date_id
  LEFT JOIN 
    dim_date AS dd_shipped
    ON DATE(TIMESTAMP_MILLIS(o.shipped_date)) = dd_shipped.date; -- Map shipped_date to date_id