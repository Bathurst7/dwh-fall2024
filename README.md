# Data Warehousing and Business Analytics - 2024's fall semester

## Team member:
1. Dao Duc Anh - 21070516
2. Dao Duc Trong - 21070079
3. Nguyen Ngoc Van Quynh - 21070076

## Preparation:
1. Create a new postgresql for the testing the database locally
2. 

## 1. Side note:
1. 'customer_customer_demo', and 'customer_demographics' is empty. Thus, we cannot use it in the dimensional modelling (dim_customer)
2. 

## Step by step of dimensional modelling:
1. Identify the Business Process and Requirements: Procurement Process is selected to be analysed.
2. Identify the Grain level: In this case, we choose the lowest level of detail (finest level grain - easy to join between tables). Hence, instead of fact_orders, we choose the fact_order_details
3. Idenify the Dimensions: [Dim_customer], [Dim_products], [Dim_product_suppliers], [Dim_employee], [Dim_emplyee_territories]