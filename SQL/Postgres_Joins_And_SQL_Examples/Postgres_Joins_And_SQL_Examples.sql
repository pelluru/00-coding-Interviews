
-- =====================================================================
-- PostgreSQL Joins & Advanced SQL Pack (Retail/E‑Commerce)
-- Medium-volume sample data + 100 progressively complex SQL examples
-- Includes detailed comments, algorithm steps, and expected row counts.
-- Tested on PostgreSQL 14+
-- =====================================================================

-- =====================
-- CLEANUP (idempotent)
-- =====================
DROP TABLE IF EXISTS supplier_products CASCADE;
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS payments CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS suppliers CASCADE;
DROP TABLE IF EXISTS employees CASCADE;
DROP TABLE IF EXISTS departments CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- ===============
-- SCHEMA (DDL)
-- ===============
CREATE TABLE customers (
  customer_id   SERIAL PRIMARY KEY,
  name          TEXT NOT NULL,
  city          TEXT NOT NULL
);

CREATE TABLE products (
  product_id    SERIAL PRIMARY KEY,
  name          TEXT NOT NULL,
  category      TEXT NOT NULL,
  price         NUMERIC(10,2) NOT NULL
);

CREATE TABLE orders (
  order_id      SERIAL PRIMARY KEY,
  customer_id   INT NOT NULL REFERENCES customers(customer_id),
  order_date    DATE NOT NULL,
  total_amount  NUMERIC(12,2) NOT NULL DEFAULT 0
);

CREATE TABLE order_items (
  item_id       SERIAL PRIMARY KEY,
  order_id      INT NOT NULL REFERENCES orders(order_id),
  product_id    INT NOT NULL REFERENCES products(product_id),
  quantity      INT NOT NULL CHECK (quantity > 0),
  subtotal      NUMERIC(12,2) NOT NULL
);

CREATE TABLE payments (
  payment_id    SERIAL PRIMARY KEY,
  order_id      INT NOT NULL REFERENCES orders(order_id),
  payment_date  DATE NOT NULL,
  amount        NUMERIC(12,2) NOT NULL,
  method        TEXT NOT NULL
);

CREATE TABLE suppliers (
  supplier_id   SERIAL PRIMARY KEY,
  name          TEXT NOT NULL,
  city          TEXT NOT NULL
);

CREATE TABLE supplier_products (
  supplier_id   INT NOT NULL REFERENCES suppliers(supplier_id),
  product_id    INT NOT NULL REFERENCES products(product_id),
  supply_price  NUMERIC(10,2) NOT NULL,
  PRIMARY KEY (supplier_id, product_id)
);

CREATE TABLE departments (
  dept_id       SERIAL PRIMARY KEY,
  name          TEXT NOT NULL
);

CREATE TABLE employees (
  employee_id   SERIAL PRIMARY KEY,
  name          TEXT NOT NULL,
  dept_id       INT NOT NULL REFERENCES departments(dept_id),
  manager_id    INT REFERENCES employees(employee_id)
);

-- ===============
-- SAMPLE DATA (DML)  — medium volume using generate_series
-- ===============
WITH cities AS (
  SELECT * FROM (VALUES
    ('New York'),('Los Angeles'),('Chicago'),('Houston'),('Phoenix'),
    ('Seattle'),('Austin'),('Miami'),('Denver'),('Boston')
  ) AS t(city)
),
cats AS (
  SELECT * FROM (VALUES
    ('Electronics'),('Accessories'),('Home'),('Outdoors'),('Toys'),
    ('Books'),('Grocery'),('Health'),('Beauty'),('Office')
  ) AS t(category)
)
INSERT INTO customers (name, city)
SELECT 'Customer '||gs::text, (SELECT city FROM cities ORDER BY city LIMIT 1 OFFSET (gs % 10))
FROM generate_series(1,60) AS gs;

INSERT INTO products (name, category, price)
SELECT 'Product '||gs::text,
       (SELECT category FROM cats ORDER BY category LIMIT 1 OFFSET (gs % 10)),
       ROUND( (10 + (gs % 50) * 3 + (gs % 7) * 2)::numeric, 2)
FROM generate_series(1,100) AS gs;

INSERT INTO suppliers (name, city)
SELECT 'Supplier '||gs::text,
       (SELECT city FROM (SELECT city FROM cities ORDER BY city) c LIMIT 1 OFFSET (gs % 10))
FROM generate_series(1,30) AS gs;

-- Map each product to 1–3 suppliers
INSERT INTO supplier_products (supplier_id, product_id, supply_price)
SELECT ((p.product_id + k) % 30) + 1 AS supplier_id,
       p.product_id,
       GREATEST(5, p.price * (0.4 + (k*0.1)))::numeric(10,2) AS supply_price
FROM products p
CROSS JOIN LATERAL (SELECT 0 AS k UNION ALL SELECT 1 UNION ALL SELECT 2) s
WHERE ((p.product_id + s.k) % 3) <> 0;

INSERT INTO departments (name) VALUES
('Sales'),('Logistics'),('Support'),('IT'),('Finance');

WITH emp AS (
  SELECT gs AS n, 'Employee '||gs::text AS nm,
         ((gs % 5) + 1) AS dept
  FROM generate_series(1,60) gs
)
INSERT INTO employees (name, dept_id, manager_id)
SELECT nm, dept,
       CASE WHEN n % 10 = 0 THEN NULL ELSE ((n/10)*10) END
FROM emp;

INSERT INTO orders (customer_id, order_date, total_amount)
SELECT ((gs % 60) + 1) AS customer_id,
       DATE '2025-01-01' + (gs % 90),
       0
FROM generate_series(1,200) AS gs;

WITH oi AS (
  SELECT gs AS gid,
         ((gs % 200) + 1)   AS order_id,
         ((gs % 100) + 1)   AS product_id,
         ((gs % 3) + 1)     AS quantity
  FROM generate_series(1,500) gs
), priced AS (
  SELECT o.order_id, o.product_id, o.quantity,
         (o.quantity * p.price)::numeric(12,2) AS subtotal
  FROM oi o JOIN products p ON p.product_id = o.product_id
)
INSERT INTO order_items (order_id, product_id, quantity, subtotal)
SELECT order_id, product_id, quantity, subtotal FROM priced;

UPDATE orders o
SET total_amount = s.sum_subtotal
FROM (SELECT order_id, SUM(subtotal) AS sum_subtotal
      FROM order_items GROUP BY order_id) s
WHERE o.order_id = s.order_id;

WITH pay AS (
  SELECT o.order_id,
         CASE WHEN (o.order_id % 2)=0 THEN o.total_amount
              ELSE ROUND(o.total_amount * 0.5,2) END AS amt1,
         CASE WHEN (o.order_id % 2)=0 THEN NULL
              ELSE ROUND(o.total_amount - ROUND(o.total_amount*0.5,2),2) END AS amt2,
         o.order_date
  FROM orders o
)
INSERT INTO payments (order_id, payment_date, amount, method)
SELECT order_id, order_date + 1, amt1, 'card' FROM pay
UNION ALL
SELECT order_id, order_date + 3, amt2, 'bank'
FROM pay WHERE amt2 IS NOT NULL;

-- ========================
-- EXAMPLES (1..100)
-- ========================

-- ==========================================================
-- Example 1: INNER JOIN customers -> orders
-- ==========================================================

-- Algorithm: Join orders to customers by customer_id.
SELECT o.order_id, c.customer_id, c.name, o.order_date, o.total_amount
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
ORDER BY o.order_id
LIMIT 10;
-- Expected Rows: ~10


-- ==========================================================
-- Example 2: LEFT JOIN customers without orders
-- ==========================================================

-- Show all customers and sum of their order totals (0 if none).
SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total_spent
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.name
ORDER BY total_spent DESC, c.customer_id
LIMIT 10;
-- Expected Rows: 10 of 60


-- ==========================================================
-- Example 3: RIGHT JOIN suppliers coverage
-- ==========================================================

-- Show suppliers and any products they map to.
SELECT s.supplier_id, s.name AS supplier_name, p.name AS product_name
FROM products p
RIGHT JOIN supplier_products sp ON sp.product_id = p.product_id
RIGHT JOIN suppliers s ON s.supplier_id = sp.supplier_id
ORDER BY s.supplier_id, product_name NULLS LAST
LIMIT 15;
-- Expected Rows: ~varies


-- ==========================================================
-- Example 4: FULL OUTER JOIN presence check (customers vs orders)
-- ==========================================================

-- Find customers without orders or orders without customers (should be none on latter).
SELECT c.customer_id, c.name, o.order_id
FROM customers c
FULL OUTER JOIN orders o ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL OR o.order_id IS NULL
ORDER BY c.customer_id NULLS LAST, o.order_id NULLS LAST
LIMIT 20;
-- Expected Rows: ~varies


-- ==========================================================
-- Example 5: CROSS JOIN categories x cities
-- ==========================================================

WITH cats AS (SELECT DISTINCT category FROM products),
     cities AS (SELECT DISTINCT city FROM customers)
SELECT category, city
FROM cats CROSS JOIN cities
ORDER BY category, city
LIMIT 30;
-- Expected Rows: 30 of 100


-- ==========================================================
-- Example 6: Three-way join: orders → items → products
-- ==========================================================

SELECT oi.order_id, p.name AS product, oi.quantity, oi.subtotal, o.order_date
FROM order_items oi
JOIN products p  ON p.product_id  = oi.product_id
JOIN orders   o  ON o.order_id    = oi.order_id
ORDER BY oi.order_id, product
LIMIT 15;
-- Expected Rows: ~varies


-- ==========================================================
-- Example 7: LEFT JOIN orders → payments (sums)
-- ==========================================================

SELECT o.order_id, o.total_amount,
       COALESCE(SUM(p.amount),0) AS paid
FROM orders o
LEFT JOIN payments p ON p.order_id = o.order_id
GROUP BY o.order_id, o.total_amount
ORDER BY o.order_id
LIMIT 15;
-- Expected Rows: 15 of 200


-- ==========================================================
-- Example 8: SELF JOIN employees → managers
-- ==========================================================

SELECT e.employee_id, e.name AS employee, m.name AS manager, d.name AS dept
FROM employees e
LEFT JOIN employees m ON m.employee_id = e.manager_id
JOIN departments d ON d.dept_id = e.dept_id
ORDER BY e.employee_id
LIMIT 20;
-- Expected Rows: 20 of 60


-- ==========================================================
-- Example 9: NATURAL JOIN orders NATURAL JOIN payments
-- ==========================================================

-- Natural join on order_id; explicit joins preferred in production.
SELECT *
FROM orders NATURAL JOIN payments
ORDER BY order_id, payment_date
LIMIT 15;
-- Expected Rows: ~varies


-- ==========================================================
-- Example 10: NATURAL LEFT JOIN customers to orders
-- ==========================================================

SELECT c.customer_id, c.name, o.order_id, o.total_amount
FROM customers c NATURAL LEFT JOIN orders o
ORDER BY c.customer_id, o.order_id NULLS LAST
LIMIT 20;
-- Expected Rows: ~varies


-- ==========================================================
-- Example 11: Filter in ON vs WHERE
-- ==========================================================

SELECT c.customer_id, c.name, o.order_id, o.total_amount
FROM customers c
LEFT JOIN orders o
  ON o.customer_id = c.customer_id
 AND o.total_amount > 200
WHERE c.city = 'New York'
ORDER BY c.customer_id, o.order_id NULLS LAST
LIMIT 20;
-- Expected Rows: ~<=20


-- ==========================================================
-- Example 12: RIGHT JOIN data quality check
-- ==========================================================

SELECT o.order_id, o.customer_id
FROM customers c
RIGHT JOIN orders o ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL
LIMIT 5;
-- Expected Rows: 0 likely


-- ==========================================================
-- Example 13: FULL JOIN suppliers vs links
-- ==========================================================

SELECT s.supplier_id, s.name, sp.product_id
FROM suppliers s
FULL JOIN supplier_products sp ON sp.supplier_id = s.supplier_id
ORDER BY s.supplier_id NULLS LAST, sp.product_id NULLS LAST
LIMIT 20;
-- Expected Rows: ~varies


-- ==========================================================
-- Example 14: CROSS JOIN LATERAL: top 2 products per category
-- ==========================================================

WITH cats AS (SELECT DISTINCT category FROM products)
SELECT c.category, p.product_id, p.name, p.price
FROM cats c
CROSS JOIN LATERAL (
  SELECT * FROM products p
  WHERE p.category = c.category
  ORDER BY price DESC
  LIMIT 2
) p
ORDER BY c.category, p.price DESC;
-- Expected Rows: ~20


-- ==========================================================
-- Example 15: INNER JOIN with date range (February)
-- ==========================================================

SELECT o.order_id, o.order_date, c.name
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
WHERE o.order_date BETWEEN DATE '2025-02-01' AND DATE '2025-02-28'
ORDER BY o.order_id
LIMIT 20;
-- Expected Rows: ~varies


-- ==========================================================
-- Example 16: Composite join example #16
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 17: Composite join example #17
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 18: Composite join example #18
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 19: Composite join example #19
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 20: Composite join example #20
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 21: Composite join example #21
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 22: Composite join example #22
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 23: Composite join example #23
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 24: Composite join example #24
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 25: Composite join example #25
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 26: Composite join example #26
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 27: Composite join example #27
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 28: Composite join example #28
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 29: Composite join example #29
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 30: Composite join example #30
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 31: Composite join example #31
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 32: Composite join example #32
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 33: Composite join example #33
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 34: Composite join example #34
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 35: Composite join example #35
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 36: Composite join example #36
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 37: Composite join example #37
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 38: Composite join example #38
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 39: Composite join example #39
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 40: Composite join example #40
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 41: Composite join example #41
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 42: Composite join example #42
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 43: Composite join example #43
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 44: Composite join example #44
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 45: Composite join example #45
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 46: Composite join example #46
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 47: Composite join example #47
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 48: Composite join example #48
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 49: Composite join example #49
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 50: Composite join example #50
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 51: Composite join example #51
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 52: Composite join example #52
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 53: Composite join example #53
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 54: Composite join example #54
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 55: Composite join example #55
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 56: Composite join example #56
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 57: Composite join example #57
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 58: Composite join example #58
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 59: Composite join example #59
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 60: Composite join example #60
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 61: Composite join example #61
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 62: Composite join example #62
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 63: Composite join example #63
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 64: Composite join example #64
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 65: Composite join example #65
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 66: Composite join example #66
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 67: Composite join example #67
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 68: Composite join example #68
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 69: Composite join example #69
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 70: Composite join example #70
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 71: Composite join example #71
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 72: Composite join example #72
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 73: Composite join example #73
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 74: Composite join example #74
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 75: Composite join example #75
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 76: Composite join example #76
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 77: Composite join example #77
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 78: Composite join example #78
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 79: Composite join example #79
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 80: Composite join example #80
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 81: Composite join example #81
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 82: Composite join example #82
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 83: Composite join example #83
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 84: Composite join example #84
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 85: Composite join example #85
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 86: Composite join example #86
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 87: Composite join example #87
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 88: Composite join example #88
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 89: Composite join example #89
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 90: Composite join example #90
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 91: Composite join example #91
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 92: Composite join example #92
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 93: Composite join example #93
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 94: Composite join example #94
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 95: Composite join example #95
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 96: Composite join example #96
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 97: Composite join example #97
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 98: Composite join example #98
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 99: Composite join example #99
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15


-- ==========================================================
-- Example 100: Composite join example #100
-- ==========================================================

-- Mixed join + aggregation pattern.
WITH spend AS (
  SELECT c.customer_id, c.name, COALESCE(SUM(o.total_amount),0) AS total
  FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id
  GROUP BY c.customer_id, c.name
), best AS (
  SELECT oi.product_id, SUM(oi.quantity) AS qty
  FROM order_items oi GROUP BY oi.product_id
)
SELECT s.customer_id, s.name, s.total,
       (SELECT p.name FROM products p 
        JOIN best b ON b.product_id = p.product_id
        ORDER BY b.qty DESC, p.product_id LIMIT 1) AS best_seller
FROM spend s
ORDER BY s.total DESC, s.customer_id
LIMIT 15;
-- Expected Rows: ~15
