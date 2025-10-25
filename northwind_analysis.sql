-- Project: Business Analytics – Northwind Dataset
-- Description: This script sets up the analysis schema, builds the KPI view,
--              and runs the 10 analytical queries required for the project.
-- IMPORTANT: Run each query individually to observe the results.

/* =============================================================================
  				 DATASET IMPORT INSTRUCTIONS

1. Open pgAdmin and connect to their PostgreSQL server.
2. Right-click Databases → Create → Database… → name it, e.g., Northwind.
3. Right-click the new database → Restore
4. Select Format: Plain and locate the uploaded .dump file.
5. Click Restore — pgAdmin will execute the SQL commands to rebuild all tables and load data.
============================================================================= */


-- =============================================================================
-- 01. SETUP: Create schema & standardized views
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS sc;
SET search_path = sc, public;

-- Categories
CREATE OR REPLACE VIEW sc.categories AS
SELECT
  categoryid   AS category_id,
  categoryname AS category_name
FROM public.categories;

-- Customers
CREATE OR REPLACE VIEW sc.customers AS
SELECT
  customerid   AS customer_id,
  companyname  AS company_name,
  country,
  NULL::text   AS region
FROM public.customers;

-- Employees
CREATE OR REPLACE VIEW sc.employees AS
SELECT
  employeeid   AS employee_id,
  employeename AS employee_name,
  title
FROM public.employees;

-- Products
CREATE OR REPLACE VIEW sc.products AS
SELECT
  productid    AS product_id,
  productname  AS product_name,
  categoryid   AS category_id
FROM public.products;

-- Shippers (handles naming differences)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='shippers' AND column_name='shipperid'
  ) THEN
    EXECUTE $SQL$
      CREATE OR REPLACE VIEW sc.shippers AS
      SELECT shipperid AS shipper_id, companyname AS company_name
      FROM public.shippers;
    $SQL$;
  ELSE
    EXECUTE $SQL$
      CREATE OR REPLACE VIEW sc.shippers AS
      SELECT "shipperID" AS shipper_id, "companyName" AS company_name
      FROM public.shippers;
    $SQL$;
  END IF;
END$$;

-- Orders (handles requiredate vs requireddate)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='orders' AND column_name='requiredate'
  ) THEN
    EXECUTE $SQL$
      CREATE OR REPLACE VIEW sc.orders AS
      SELECT
        orderid      AS order_id,
        customerid   AS customer_id,
        employeeid   AS employee_id,
        orderdate    AS order_date,
        requiredate  AS required_date,
        shippeddate  AS shipped_date,
        shipperid    AS ship_via,
        freight
      FROM public.orders;
    $SQL$;
  ELSIF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='orders' AND column_name='requireddate'
  ) THEN
    EXECUTE $SQL$
      CREATE OR REPLACE VIEW sc.orders AS
      SELECT
        orderid      AS order_id,
        customerid   AS customer_id,
        employeeid   AS employee_id,
        orderdate    AS order_date,
        requireddate AS required_date,
        shippeddate  AS shipped_date,
        shipperid    AS ship_via,
        freight
      FROM public.orders;
    $SQL$;
  ELSE
    RAISE EXCEPTION 'Neither requiredate nor requireddate exists in public.orders';
  END IF;
END$$;

-- Order Items
CREATE OR REPLACE VIEW sc.order_items AS
SELECT
  orderid    AS order_id,
  productid  AS product_id,
  unitprice  AS unit_price,
  quantity,
  discount
FROM public.order_details;


-- =============================================================================
-- 02. KPI VIEW: Create v_sales for analysis
-- =============================================================================

DROP VIEW IF EXISTS v_sales;

CREATE OR REPLACE VIEW v_sales AS
WITH line AS (
  SELECT
    oi.order_id,
    SUM(oi.unit_price * oi.quantity * (1 - COALESCE(oi.discount,0)))::numeric(14,2) AS revenue
  FROM sc.order_items oi
  GROUP BY oi.order_id
)
SELECT
  o.order_id,
  o.customer_id,
  o.employee_id,
  o.order_date,
  o.required_date,
  o.shipped_date,
  o.ship_via,
  COALESCE(o.freight,0)::numeric(12,2) AS freight,
  COALESCE(line.revenue,0)::numeric(14,2) AS revenue,
  CASE WHEN o.shipped_date IS NOT NULL AND o.required_date IS NOT NULL
         AND o.shipped_date <= o.required_date THEN 1 ELSE 0 END AS on_time_flag,
  CASE WHEN o.shipped_date IS NULL THEN 0 ELSE 1 END AS shipped_flag,
  CASE
    WHEN o.shipped_date IS NOT NULL AND o.order_date IS NOT NULL
      THEN EXTRACT(EPOCH FROM (o.shipped_date::timestamp - o.order_date::timestamp)) / 86400.0
    ELSE NULL
  END AS order_cycle_days
FROM sc.orders o
LEFT JOIN line USING(order_id);


-- =============================================================================
-- 03. ANALYSIS QUERIES
-- =============================================================================

SET search_path = sc, public;

-- ==============================
-- Q1: Annual Revenue & Order Volume
-- Group orders by year; sum revenue (from v_sales); count orders
-- ==============================
SELECT DATE_PART('year', order_date)::int AS year,
       ROUND(SUM(revenue)::numeric,2)     AS revenue,
       COUNT(*)                           AS orders
FROM v_sales
WHERE order_date IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- ==============================
-- Q2: Top 10 Products by Revenue
-- Sum line revenue per product (unit_price * qty * (1 - discount))
-- ==============================
SELECT p.product_id, p.product_name,
       ROUND(SUM(oi.unit_price*oi.quantity*(1-COALESCE(oi.discount,0)))::numeric,2) AS revenue
FROM sc.order_items oi
JOIN sc.products p ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name
ORDER BY revenue DESC
LIMIT 10;

-- ==============================
-- Q3: Top 10 Customers by Revenue
-- Aggregate v_sales revenue by customer
-- ==============================
SELECT c.customer_id, c.company_name,
       ROUND(SUM(v.revenue)::numeric,2) AS revenue,
       COUNT(*)                         AS orders
FROM v_sales v
JOIN sc.customers c ON c.customer_id = v.customer_id
GROUP BY c.customer_id, c.company_name
ORDER BY revenue DESC
LIMIT 10;

-- ==============================
-- Q4: Employee Sales Performance
-- Aggregate revenue/orders by employee
-- ==============================
SELECT e.employee_id,
       e.employee_name,
       ROUND(SUM(v.revenue)::numeric,2) AS revenue,
       COUNT(*)                         AS orders
FROM v_sales v
JOIN sc.employees e ON e.employee_id = v.employee_id
GROUP BY e.employee_id, e.employee_name
ORDER BY revenue DESC;

-- ==============================
-- Q5: Customer Order Frequency & Basket Size
-- Count distinct orders and average order value per customer
-- ==============================
WITH per_order AS (
  SELECT customer_id, order_id, COALESCE(revenue,0) AS revenue
  FROM v_sales
)
SELECT c.customer_id, c.company_name,
       COUNT(DISTINCT p.order_id)        AS orders_count,
       ROUND(AVG(p.revenue)::numeric,2)  AS avg_order_value
FROM per_order p
JOIN sc.customers c ON c.customer_id = p.customer_id
GROUP BY c.customer_id, c.company_name
ORDER BY orders_count DESC, avg_order_value DESC
LIMIT 15;

-- ==============================
-- Q6: Avg Delivery Time & On-time % by Shipper
-- Use v_sales order_cycle_days (order→ship) and on_time_flag by shipper
-- ==============================
SELECT s.company_name AS shipper,
       ROUND(AVG(v.order_cycle_days)::numeric,2) AS avg_days_order_to_ship,
       ROUND(100.0*AVG(v.on_time_flag)::numeric,2) AS otd_percent,
       COUNT(*) FILTER (WHERE v.shipped_flag=1)  AS shipped_orders
FROM v_sales v
JOIN sc.shippers s ON s.shipper_id = v.ship_via
WHERE v.shipped_flag = 1
GROUP BY s.company_name
ORDER BY avg_days_order_to_ship DESC;

-- ==============================
-- Q7: Monthly Revenue vs Shipping Cost
-- Sum revenue + freight by month(order_date)
-- ==============================
SELECT DATE_TRUNC('month', v.order_date) AS month,
       ROUND(SUM(v.revenue)::numeric,2)  AS revenue,
       ROUND(SUM(v.freight)::numeric,2)  AS freight
FROM v_sales v
WHERE v.order_date IS NOT NULL
GROUP BY DATE_TRUNC('month', v.order_date)
ORDER BY month;

-- ==============================
-- Q8: Regional Product Preferences
-- Join lines→products→categories and v_sales→customers; aggregate by country×category
-- ==============================

SELECT COALESCE(cust.country,'(unknown)') AS country,
       cat.category_name,
       ROUND(SUM(oi.unit_price*oi.quantity*(1-COALESCE(oi.discount,0)))::numeric,2) AS revenue,
       COUNT(DISTINCT v.order_id) AS orders
FROM sc.order_items oi
JOIN v_sales v         ON v.order_id = oi.order_id
JOIN sc.products p     ON p.product_id = oi.product_id
JOIN sc.categories cat ON cat.category_id = p.category_id
JOIN sc.customers cust ON cust.customer_id = v.customer_id
GROUP BY country, cat.category_name
ORDER BY country, revenue DESC;

-- ==============================
-- Q9: Flag products with a declining trend
-- ==============================

WITH m AS (
  -- Revenue per product per month
  SELECT
    p.product_id, p.product_name,
    DATE_TRUNC('month', v.order_date) AS month,
    SUM(oi.unit_price*oi.quantity*(1-COALESCE(oi.discount,0))) AS rev
  FROM sc.order_items oi
  JOIN v_sales v     ON v.order_id = oi.order_id
  JOIN sc.products p ON p.product_id = oi.product_id
  WHERE v.order_date IS NOT NULL
  GROUP BY p.product_id, p.product_name, DATE_TRUNC('month', v.order_date)
),
bounds AS (
  -- First and last month per product
  SELECT product_id, MIN(month) AS min_month, MAX(month) AS max_month,
         COUNT(DISTINCT month) AS months_with_sales
  FROM m GROUP BY product_id
),
pick AS (
  -- Join first and last values; compute average monthly slope
  SELECT
    b.product_id, m1.product_name,
    m1.rev AS first_rev, m2.rev AS last_rev,
    (m2.rev - m1.rev) / NULLIF(b.months_with_sales - 1, 0) AS monthly_slope
  FROM bounds b
  JOIN m m1 ON m1.product_id=b.product_id AND m1.month=b.min_month
  JOIN m m2 ON m2.product_id=b.product_id AND m2.month=b.max_month
)
SELECT
  product_id, product_name,
  ROUND(monthly_slope::numeric,2) AS monthly_revenue_slope
FROM pick
WHERE monthly_slope < 0           -- Keep only declines
ORDER BY monthly_revenue_slope ASC
LIMIT 20;

-- ==============================
-- Q10: Flag products with an increasing trend
-- ==============================

WITH m AS (
  SELECT
    p.product_id, p.product_name,
    DATE_TRUNC('month', v.order_date) AS month,
    SUM(oi.unit_price * oi.quantity * (1 - COALESCE(oi.discount, 0))) AS rev
  FROM sc.order_items oi
  JOIN v_sales   v ON v.order_id   = oi.order_id
  JOIN sc.products p ON p.product_id = oi.product_id
  WHERE v.order_date IS NOT NULL
  GROUP BY p.product_id, p.product_name, DATE_TRUNC('month', v.order_date)
), bounds AS (
  SELECT product_id,
         MIN(month) AS min_month,
         MAX(month) AS max_month,
         COUNT(DISTINCT month) AS months_with_sales
  FROM m
  GROUP BY product_id
), pick AS (
  SELECT
    b.product_id,
    m1.product_name,
    m1.rev AS first_rev,
    m2.rev AS last_rev,
    (m2.rev - m1.rev) / NULLIF(b.months_with_sales - 1, 0) AS monthly_slope
  FROM bounds b
  JOIN m m1 ON m1.product_id = b.product_id AND m1.month = b.min_month
  JOIN m m2 ON m2.product_id = b.product_id AND m2.month = b.max_month
)
SELECT
  product_id,
  product_name,
  ROUND(monthly_slope::numeric, 2) AS monthly_revenue_slope
FROM pick
WHERE monthly_slope > 0
ORDER BY monthly_revenue_slope DESC
LIMIT 20;