-- upstream is the process of getting products to our locations for the production process:

-- quesion 1: what is our products?
select * from products;
-- select count(distinct product_name) from products; -- 77 distinct products to be managed

-- question 2: who is our suppliers?
select * from suppliers;

-- where are they from?
select country, count(*)
from suppliers
group by 1
order by 2 desc;
-- select count(distinct company_name) from suppliers; -- 29 distinct suppliers

select 
    -- p.* except supplier_id, this can only be done using gbq
    p.product_id,
    p.product_name,
    p.quantity_per_unit,
    p.unit_price,
    p.units_in_stock,
    p.units_on_order,
    p.reorder_level,
    p.discontinued,
    c.category_name,
    s.company_name supplier_company_name,
    concat(s.contact_title, ' ', s.contact_name) supplier_PIC,
    concat(s.city, ' ', s.country) suppling_from
from products p
    left join suppliers s
        on p.supplier_id = s.supplier_id
    left join categories c 
        on p.category_id = c.category_id;