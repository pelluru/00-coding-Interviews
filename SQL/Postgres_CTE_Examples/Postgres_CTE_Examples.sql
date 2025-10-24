-- =====================================================================
-- PostgreSQL CTE Master Pack
-- DDL + DML sample data + 20 heavily commented CTE examples
-- Includes expected outputs as SQL comments for learning/reference.
-- Tested for PostgreSQL 14+.
-- =====================================================================

-- =====================
-- CLEANUP (idempotent)
-- =====================
DROP TABLE IF EXISTS sales CASCADE;
DROP TABLE IF EXISTS projects CASCADE;
DROP TABLE IF EXISTS employees CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS departments CASCADE;

-- ===============
-- SCHEMA (DDL)
-- ===============
CREATE TABLE departments (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    location    TEXT NOT NULL
);

CREATE TABLE employees (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    manager_id  INT REFERENCES employees(id),
    dept_id     INT REFERENCES departments(id) NOT NULL,
    salary      NUMERIC(12,2) NOT NULL
);

CREATE TABLE products (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    category    TEXT NOT NULL,
    price       NUMERIC(10,2) NOT NULL
);

CREATE TABLE sales (
    id          SERIAL PRIMARY KEY,
    emp_id      INT REFERENCES employees(id) NOT NULL,
    product_id  INT REFERENCES products(id) NOT NULL,
    amount      NUMERIC(12,2) NOT NULL,
    sale_date   DATE NOT NULL
);

CREATE TABLE projects (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    start_date  DATE NOT NULL,
    end_date    DATE,
    dept_id     INT REFERENCES departments(id) NOT NULL
);

-- ===============
-- SAMPLE DATA
-- ===============
INSERT INTO departments (name, location) VALUES
 ('Engineering','NY'),
 ('Sales','NY'),
 ('Marketing','CA'),
 ('Finance','TX');

-- Employees: include a small hierarchy
INSERT INTO employees (name, manager_id, dept_id, salary) VALUES
 ('Alice',   NULL, 1, 150000.00),
 ('Bob',        1, 1, 120000.00),
 ('Carol',      1, 1, 115000.00),
 ('Dave',    NULL, 2, 140000.00),
 ('Eve',         4, 2, 100000.00),
 ('Frank',       4, 2,  95000.00),
 ('Grace',    NULL, 3, 130000.00),
 ('Heidi',       7, 3,  90000.00),
 ('Ivan',     NULL, 4, 125000.00),
 ('Judy',        9, 4,  85000.00);

INSERT INTO products (name, category, price) VALUES
 ('Laptop Pro',        'Hardware', 2200.00),
 ('Laptop Air',        'Hardware', 1600.00),
 ('Phone X',           'Hardware',  999.00),
 ('Monitor 27"',       'Hardware',  350.00),
 ('Support Plan',      'Service',   299.00),
 ('Training Course',   'Service',   799.00);

-- Sales: spread across employees and products/dates
INSERT INTO sales (emp_id, product_id, amount, sale_date) VALUES
 (4, 1, 2200.00, '2025-01-05'),
 (5, 3,  999.00, '2025-01-08'),
 (5, 5,  299.00, '2025-01-08'),
 (6, 2, 1600.00, '2025-01-12'),
 (6, 4,  350.00, '2025-01-12'),
 (4, 6,  799.00, '2025-02-03'),
 (5, 1, 2200.00, '2025-02-07'),
 (6, 3,  999.00, '2025-02-09'),
 (8, 6,  799.00, '2025-02-10'),
 (2, 4,  350.00, '2025-02-15'),
 (3, 5,  299.00, '2025-02-16'),
 (5, 2, 1600.00, '2025-03-01'),
 (10,4,  350.00, '2025-03-02'),
 (8, 2, 1600.00, '2025-03-03'),
 (4, 3,  999.00, '2025-03-10');

INSERT INTO projects (name, start_date, end_date, dept_id) VALUES
 ('Genie Platform',       '2025-01-10', NULL, 1),
 ('Mobile Revamp',        '2025-02-01', NULL, 3),
 ('NY Q1 Sales Push',     '2025-01-01', '2025-03-31', 2),
 ('Cost Optimization',    '2025-01-15', NULL, 4);

-- =====================================================================
-- 20 CTE EXAMPLES (with heavy comments and expected outputs)
-- Note: Expected outputs are illustrative; exact row order may vary.
-- =====================================================================

-- 1) SIMPLE CTE: Rename/clarify a base selection
WITH ny_depts AS (
  SELECT id, name
  FROM departments
  WHERE location = 'NY'
)
SELECT * FROM ny_depts;
-- Expected output (columns: id, name)
-- 1 | Engineering
-- 2 | Sales

-- 2) MULTIPLE CTEs: Compose results then join
WITH eng AS (
  SELECT id FROM departments WHERE name = 'Engineering'
), eng_emps AS (
  SELECT e.id, e.name, e.salary
  FROM employees e
  JOIN eng ON e.dept_id = eng.id
)
SELECT * FROM eng_emps ORDER BY salary DESC;
-- Expected (id, name, salary): Alice, Bob, Carol

-- 3) CTE + WHERE filter
WITH high_paid AS (
  SELECT id, name, salary FROM employees WHERE salary >= 120000
)
SELECT name, salary FROM high_paid ORDER BY salary DESC;
-- Expected: Alice 150k, Dave 140k, Grace 130k, Ivan 125k, Bob 120k

-- 4) CTE + JOIN inside the CTE
WITH emp_with_dept AS (
  SELECT e.id, e.name, d.name AS dept, e.salary
  FROM employees e
  JOIN departments d ON e.dept_id = d.id
)
SELECT * FROM emp_with_dept WHERE dept = 'Sales' ORDER BY salary DESC;
-- Expected: Dave, Eve, Frank (with Sales as dept)

-- 5) CTE + AGGREGATION
WITH dept_avg AS (
  SELECT dept_id, AVG(salary) AS avg_salary
  FROM employees GROUP BY dept_id
)
SELECT d.name, ROUND(avg_salary,2) AS avg_salary
FROM dept_avg a JOIN departments d ON d.id = a.dept_id
ORDER BY avg_salary DESC;
-- Expected top ~ Engineering/Sales/Marketing/Finance in some order

-- 6) REUSE THE SAME CTE MULTIPLE TIMES
WITH dept_sales AS (
  SELECT d.id AS dept_id, d.name AS dept_name, SUM(s.amount) AS total_sales
  FROM sales s
  JOIN employees e ON e.id = s.emp_id
  JOIN departments d ON d.id = e.dept_id
  GROUP BY d.id, d.name
)
SELECT * FROM dept_sales WHERE total_sales > 3000
UNION ALL
SELECT * FROM dept_sales WHERE total_sales <= 3000;
-- Expected: All departments with their total sales split by threshold

-- 7) NESTED CTEs
WITH base AS (
  SELECT e.id, e.name, d.name AS dept, e.salary
  FROM employees e JOIN departments d ON d.id = e.dept_id
), filtered AS (
  SELECT * FROM base WHERE salary >= 100000
)
SELECT dept, COUNT(*) AS cnt FROM filtered GROUP BY dept ORDER BY cnt DESC;
-- Expected counts per dept of employees earning >= 100k

-- 8) CTE + CASE logic
WITH sales_enriched AS (
  SELECT s.*, 
         CASE 
           WHEN amount >= 1600 THEN 'High'
           WHEN amount >= 800  THEN 'Medium'
           ELSE 'Low'
         END AS amount_band
  FROM sales s
)
SELECT amount_band, COUNT(*) AS orders
FROM sales_enriched
GROUP BY amount_band
ORDER BY orders DESC;
-- Expected buckets: High/Medium/Low with counts

-- 9) CTE for ranking top-N per department (window fn)
WITH dept_totals AS (
  SELECT d.name AS dept_name, e.id AS emp_id, e.name AS emp_name,
         SUM(s.amount) AS total_sales
  FROM employees e
  JOIN departments d ON d.id = e.dept_id
  LEFT JOIN sales s ON s.emp_id = e.id
  GROUP BY d.name, e.id, e.name
), ranked AS (
  SELECT *, RANK() OVER (PARTITION BY dept_name ORDER BY total_sales DESC NULLS LAST) AS rnk
  FROM dept_totals
)
SELECT dept_name, emp_name, total_sales
FROM ranked
WHERE rnk <= 2
ORDER BY dept_name, rnk;
-- Expected: top 2 sellers per department (NULL totals appear last)

-- 10) RECURSIVE CTE: generate numbers 1..10
WITH RECURSIVE nums(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM nums WHERE n < 10
)
SELECT * FROM nums;
-- Expected: rows 1..10

-- 11) RECURSIVE HIERARCHY: employee → manager chain for Bob
WITH RECURSIVE chain AS (
  SELECT id, name, manager_id, 0 AS lvl FROM employees WHERE name = 'Bob'
  UNION ALL
  SELECT e.id, e.name, e.manager_id, c.lvl + 1
  FROM employees e 
  JOIN chain c ON e.id = c.manager_id
)
SELECT name, lvl FROM chain ORDER BY lvl;
-- Expected:
-- Bob (0), Alice (1)

-- 12) RECURSIVE PARENT-CHILD AGGREGATION: reports under a manager
WITH RECURSIVE subtree AS (
  SELECT id, name, manager_id, dept_id, salary FROM employees WHERE name = 'Dave'
  UNION ALL
  SELECT e.* FROM employees e
  JOIN subtree s ON e.manager_id = s.id
)
SELECT COUNT(*) AS team_size, SUM(salary) AS payroll FROM subtree;
-- Expected: team_size=3 (Dave,Eve,Frank), payroll sum

-- 13) WINDOW FN via CTE: running totals by date
WITH daily AS (
  SELECT sale_date, SUM(amount) AS day_total
  FROM sales GROUP BY sale_date
), running AS (
  SELECT sale_date, day_total,
         SUM(day_total) OVER (ORDER BY sale_date) AS running_total
  FROM daily
)
SELECT * FROM running ORDER BY sale_date;
-- Expected: chronological daily totals and cumulative running total

-- 14) CTE + UNION to combine sets
WITH hw AS (SELECT id, name, 'Hardware' AS k FROM products WHERE category='Hardware'),
     svc AS (SELECT id, name, 'Service'  AS k FROM products WHERE category='Service')
SELECT * FROM hw
UNION ALL
SELECT * FROM svc
ORDER BY k, name;
-- Expected: all products with a type tag, hardware first, then service

-- 15) CTE FEEDING INSERT
WITH big_orders AS (
  SELECT emp_id, SUM(amount) AS total
  FROM sales GROUP BY emp_id HAVING SUM(amount) >= 3000
)
-- Create a small table to store awards (for example purposes)
, _mk AS (
  SELECT 1 FROM (SELECT 1) t
)
-- The table may not exist on first run; create it if missing:
-- (For idempotency in a single script, use CREATE TABLE IF NOT EXISTS)
SELECT 1;
CREATE TABLE IF NOT EXISTS emp_awards (
  emp_id INT PRIMARY KEY,
  remarks TEXT NOT NULL
);
INSERT INTO emp_awards (emp_id, remarks)
SELECT emp_id, 'Top seller award' FROM big_orders
ON CONFLICT (emp_id) DO NOTHING;
-- Expected: rows inserted for employees whose total sales >= 3000

-- 16) CTE FEEDING UPDATE
WITH raises AS (
  SELECT id, CASE WHEN salary < 100000 THEN salary * 1.05 ELSE salary END AS new_salary
  FROM employees
)
UPDATE employees e
SET salary = r.new_salary
FROM raises r
WHERE e.id = r.id AND e.salary <> r.new_salary;
-- Expected: employees under 100k receive 5% raise

-- 17) CTE FEEDING DELETE
WITH small_products AS (
  SELECT id FROM products WHERE price < 350
)
DELETE FROM products p
USING small_products s
WHERE p.id = s.id;
-- Expected: products below $350 deleted (e.g., Support Plan at 299), adjust as desired

-- 18) MATERIALIZED CTE (Postgres 12+): encourage reuse caching
WITH MATERIALIZED dept_sums AS (
  SELECT d.id, SUM(s.amount) AS total
  FROM departments d
  JOIN employees e ON e.dept_id = d.id
  LEFT JOIN sales s ON s.emp_id = e.id
  GROUP BY d.id
)
SELECT * FROM dept_sums ORDER BY total DESC NULLS LAST;
-- Expected: totals per department, computed once due to MATERIALIZED

-- 19) CTE WITH JSON AGGREGATION
WITH dept_emp AS (
  SELECT d.name AS dept, json_agg(e.name ORDER BY e.name) AS employees
  FROM departments d
  LEFT JOIN employees e ON e.dept_id = d.id
  GROUP BY d.name
)
SELECT dept, employees FROM dept_emp ORDER BY dept;
-- Expected: one row per department with JSON array of employee names

-- 20) COMPLEX BUSINESS QUERY:
-- Goal: For each department, show:
--  - total sales,
--  - top-selling employee (by sales),
--  - count of active projects,
--  - and whether avg salary > 110k
WITH dept_sales AS (
  SELECT d.id AS dept_id, SUM(s.amount) AS total_sales
  FROM departments d
  JOIN employees e ON e.dept_id = d.id
  LEFT JOIN sales s ON s.emp_id = e.id
  GROUP BY d.id
),
emp_totals AS (
  SELECT e.id, e.name, e.dept_id, COALESCE(SUM(s.amount),0) AS emp_sales
  FROM employees e LEFT JOIN sales s ON s.emp_id = e.id
  GROUP BY e.id, e.name, e.dept_id
),
top_emp AS (
  SELECT dept_id, (ARRAY_AGG(name ORDER BY emp_sales DESC, name))[1] AS top_employee,
         MAX(emp_sales) AS top_sales
  FROM emp_totals GROUP BY dept_id
),
proj_counts AS (
  SELECT dept_id, COUNT(*) AS active_projects
  FROM projects WHERE end_date IS NULL GROUP BY dept_id
),
avg_sal AS (
  SELECT dept_id, AVG(salary) AS avg_salary FROM employees GROUP BY dept_id
)
SELECT d.name AS department,
       ds.total_sales,
       te.top_employee,
       te.top_sales,
       COALESCE(pc.active_projects,0) AS active_projects,
       (a.avg_salary > 110000) AS avg_salary_gt_110k
FROM departments d
LEFT JOIN dept_sales ds ON ds.dept_id = d.id
LEFT JOIN top_emp te     ON te.dept_id = d.id
LEFT JOIN proj_counts pc ON pc.dept_id = d.id
LEFT JOIN avg_sal a      ON a.dept_id = d.id
ORDER BY department;
-- Expected: one row per department with combined KPIs.
-- =====================================================================
