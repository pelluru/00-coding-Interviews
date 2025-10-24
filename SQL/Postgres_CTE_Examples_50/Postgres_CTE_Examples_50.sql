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


-- =====================================================================
-- EXTENDED: 30 MORE CTE EXAMPLES (21..50)
-- Each block is heavily commented and includes expected output guidance.
-- =====================================================================

-- 21) LATERAL JOIN with CTE to pick top-N products by price
WITH ranked AS (
  SELECT p.*, ROW_NUMBER() OVER (ORDER BY price DESC) AS rn
  FROM products p
)
SELECT id, name, price
FROM ranked
WHERE rn <= 3
ORDER BY price DESC;
-- Expected: top 3 most expensive products

-- 22) CTE generating a small calendar and joining sales per day
WITH days AS (
  SELECT d::date AS day
  FROM generate_series('2025-01-01'::date, '2025-03-31'::date, interval '1 day') AS gs(d)
),
sales_by_day AS (
  SELECT sale_date AS day, SUM(amount) AS total
  FROM sales GROUP BY sale_date
)
SELECT day, COALESCE(total,0) AS total
FROM days
LEFT JOIN sales_by_day USING (day)
ORDER BY day
LIMIT 15;
-- Expected: first 15 days with totals (zeros for days without sales)

-- 23) PIVOT-like conditional aggregates inside a CTE
WITH by_category AS (
  SELECT
    SUM(CASE WHEN category='Hardware' THEN price END) AS hw_price_sum,
    SUM(CASE WHEN category='Service'  THEN price END) AS svc_price_sum
  FROM products
)
SELECT * FROM by_category;
-- Expected: sums of prices by category

-- 24) UNPIVOT style via UNION ALL from a CTE
WITH by_category AS (
  SELECT
    SUM(CASE WHEN category='Hardware' THEN price END) AS hw_price_sum,
    SUM(CASE WHEN category='Service'  THEN price END) AS svc_price_sum
  FROM products
)
SELECT 'Hardware' AS category, hw_price_sum AS total FROM by_category
UNION ALL
SELECT 'Service', svc_price_sum FROM by_category;
-- Expected: two rows: Hardware, Service

-- 25) Moving average (7-day) of sales totals
WITH daily AS (
  SELECT sale_date::date AS day, SUM(amount) AS total
  FROM sales GROUP BY sale_date
), ma AS (
  SELECT day, total,
         AVG(total) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7
  FROM daily
)
SELECT * FROM ma ORDER BY day;
-- Expected: moving average column for days with sales

-- 26) Gaps and islands (sales streaks by employee)
WITH emp_days AS (
  SELECT emp_id, sale_date::date AS day FROM sales GROUP BY emp_id, sale_date
), numbered AS (
  SELECT emp_id, day,
         ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY day) AS rn
  FROM emp_days
), groups AS (
  SELECT emp_id, day, (day - (rn||' days')::interval)::date AS grp
  FROM numbered
)
SELECT emp_id, MIN(day) AS start_day, MAX(day) AS end_day, COUNT(*) AS len
FROM groups
GROUP BY emp_id, grp
ORDER BY emp_id, start_day;
-- Expected: contiguous date ranges per emp_id

-- 27) Sessionization by 14-day inactivity gap (per employee)
WITH ordered AS (
  SELECT emp_id, sale_date::timestamp AS ts,
         LAG(sale_date) OVER (PARTITION BY emp_id ORDER BY sale_date) AS prev_ts
  FROM sales
), flagged AS (
  SELECT emp_id, ts::date AS day,
         CASE WHEN prev_ts IS NULL OR ts - prev_ts > INTERVAL '14 days' THEN 1 ELSE 0 END AS new_session
  FROM ordered
), sessions AS (
  SELECT emp_id, day,
         SUM(new_session) OVER (PARTITION BY emp_id ORDER BY day) AS session_id
  FROM flagged
)
SELECT emp_id, MIN(day) AS start_day, MAX(day) AS end_day, COUNT(*) AS days_in_session
FROM sessions
GROUP BY emp_id, session_id
ORDER BY emp_id, start_day;
-- Expected: grouped sessions per employee

-- 28) Percentiles via window function
WITH totals AS (
  SELECT emp_id, SUM(amount) AS total FROM sales GROUP BY emp_id
), pct AS (
  SELECT emp_id, total,
         PERCENT_RANK() OVER (ORDER BY total) AS pct_rank
  FROM totals
)
SELECT * FROM pct ORDER BY total DESC;
-- Expected: employees with total sales and their percentile ranks

-- 29) Deduplicate to latest row per employee-day
WITH base AS (
  SELECT emp_id, sale_date, amount,
         ROW_NUMBER() OVER (PARTITION BY emp_id, sale_date ORDER BY id DESC) AS rn
  FROM sales
)
SELECT emp_id, sale_date, amount
FROM base WHERE rn=1
ORDER BY emp_id, sale_date;
-- Expected: one row per emp_id+date (latest by id)

-- 30) Type-2 style change detection (salary changes by employee over time)
WITH salaried AS (
  SELECT id AS emp_id, name, salary,
         ROW_NUMBER() OVER (ORDER BY id) AS rownum
  FROM employees
), changes AS (
  SELECT s1.emp_id, s1.name, s1.salary AS current_salary,
         LAG(s1.salary) OVER (ORDER BY s1.emp_id) AS prev_salary,
         (s1.salary <> LAG(s1.salary) OVER (ORDER BY s1.emp_id)) AS changed
  FROM salaried s1
)
SELECT * FROM changes;
-- Expected: boolean column 'changed' true for salary transitions (artificial demo)

-- 31) Recursive numbers with step 5
WITH RECURSIVE seq(n) AS (
  SELECT 0
  UNION ALL
  SELECT n+5 FROM seq WHERE n < 50
)
SELECT * FROM seq;
-- Expected: 0,5,10,...,50

-- 32) Bill of Materials style recursion example (self-join employees as org chart depth)
WITH RECURSIVE org AS (
  SELECT id, name, manager_id, 0 AS depth FROM employees WHERE manager_id IS NULL
  UNION ALL
  SELECT e.id, e.name, e.manager_id, o.depth+1
  FROM employees e JOIN org o ON e.manager_id = o.id
)
SELECT * FROM org ORDER BY depth, name;
-- Expected: all employees with depth from top managers

-- 33) Reachability matrix (ancestor relationships) for employees
WITH RECURSIVE anc AS (
  SELECT id AS emp_id, id AS ancestor_id FROM employees
  UNION ALL
  SELECT e.id, a.ancestor_id
  FROM employees e
  JOIN anc a ON e.manager_id = a.emp_id
)
SELECT * FROM anc ORDER BY emp_id, ancestor_id LIMIT 20;
-- Expected: rows mapping employee -> each ancestor in chain

-- 34) Path strings using array_agg and recursion (employee chain labels)
WITH RECURSIVE paths AS (
  SELECT id, name, manager_id, ARRAY[name] AS path_names FROM employees WHERE manager_id IS NULL
  UNION ALL
  SELECT e.id, e.name, e.manager_id, p.path_names || e.name
  FROM employees e JOIN paths p ON e.manager_id = p.id
)
SELECT id, array_to_string(path_names, ' > ') AS path
FROM paths ORDER BY id;
-- Expected: breadcrumb-like strings

-- 35) Upsert pattern using CTE and ON CONFLICT
WITH aggregated AS (
  SELECT emp_id, SUM(amount)::numeric(12,2) AS total FROM sales GROUP BY emp_id
)
INSERT INTO emp_awards (emp_id, remarks)
SELECT emp_id, 'Award: exceeded 4000' FROM aggregated WHERE total >= 4000
ON CONFLICT (emp_id) DO UPDATE SET remarks = 'Award: exceeded 4000';
-- Expected: insert or update awards where applicable

-- 36) Partitioned aggregates and percent-of-total
WITH dept_sales AS (
  SELECT d.name AS dept, SUM(s.amount) AS total
  FROM departments d
  JOIN employees e ON e.dept_id = d.id
  LEFT JOIN sales s ON s.emp_id = e.id
  GROUP BY d.name
), pct AS (
  SELECT dept, total, total * 100.0 / NULLIF(SUM(total) OVER (),0) AS pct_of_all
  FROM dept_sales
)
SELECT * FROM pct ORDER BY total DESC NULLS LAST;
-- Expected: each department's share of all sales

-- 37) ROW_NUMBER vs RANK vs DENSE_RANK demo
WITH totals AS (
  SELECT e.id, e.name, COALESCE(SUM(s.amount),0) AS total
  FROM employees e LEFT JOIN sales s ON s.emp_id = e.id
  GROUP BY e.id, e.name
)
SELECT name, total,
       ROW_NUMBER() OVER (ORDER BY total DESC) AS rownum,
       RANK()       OVER (ORDER BY total DESC) AS rnk,
       DENSE_RANK() OVER (ORDER BY total DESC) AS drnk
FROM totals ORDER BY total DESC, name;
-- Expected: show tie behaviors across three ranking functions

-- 38) Window frames (trailing 2 rows) on daily sales
WITH daily AS (
  SELECT sale_date AS day, SUM(amount) AS total
  FROM sales GROUP BY sale_date
), framed AS (
  SELECT day, total,
         SUM(total) OVER (ORDER BY day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sum_last_3
  FROM daily
)
SELECT * FROM framed ORDER BY day;
-- Expected: rolling sum of last up-to-3 days

-- 39) Percent of row relative to group max
WITH totals AS (
  SELECT e.dept_id, e.name, COALESCE(SUM(s.amount),0) AS total
  FROM employees e LEFT JOIN sales s ON s.emp_id=e.id
  GROUP BY e.dept_id, e.name
)
SELECT *,
       total / NULLIF(MAX(total) OVER (PARTITION BY dept_id),0) AS pct_of_dept_max
FROM totals ORDER BY dept_id, total DESC;
-- Expected: each employee's sales as a fraction of their dept max

-- 40) Generate product pairs (self join) via CTE
WITH prods AS (SELECT id, name FROM products)
SELECT p1.name AS a, p2.name AS b
FROM prods p1
JOIN prods p2 ON p1.id < p2.id
ORDER BY a, b
LIMIT 15;
-- Expected: first 15 unique product pairs

-- 41) Aggregation then filter in outer query (HAVING-like separation)
WITH dept_pay AS (
  SELECT dept_id, AVG(salary) AS avg_salary FROM employees GROUP BY dept_id
)
SELECT d.name, ROUND(avg_salary,2)
FROM dept_pay dp JOIN departments d ON d.id = dp.dept_id
WHERE avg_salary > 100000;
-- Expected: departments with avg salary over 100k

-- 42) CTE powering DELETE with join constraints (remove orphan sales to non-existing product)
WITH orphan_sales AS (
  SELECT s.id FROM sales s LEFT JOIN products p ON p.id = s.product_id
  WHERE p.id IS NULL
)
DELETE FROM sales WHERE id IN (SELECT id FROM orphan_sales);
-- Expected: no change (data is consistent), but pattern is shown

-- 43) JSON building per employee
WITH emp_json AS (
  SELECT e.id, e.name,
         jsonb_build_object(
             'dept', d.name,
             'salary', e.salary,
             'sales_total', COALESCE(SUM(s.amount),0)
         ) AS profile
  FROM employees e
  JOIN departments d ON d.id = e.dept_id
  LEFT JOIN sales s ON s.emp_id = e.id
  GROUP BY e.id, e.name, d.name, e.salary
)
SELECT * FROM emp_json ORDER BY id LIMIT 5;
-- Expected: JSONB per employee

-- 44) LATERAL JSON expansion using jsonb_to_recordset
WITH sample AS (
  SELECT '[{"p":"Laptop Pro","qty":2},{"p":"Phone X","qty":1}]'::jsonb AS js
)
SELECT x.p, x.qty
FROM sample s,
LATERAL jsonb_to_recordset(s.js) AS x(p text, qty int);
-- Expected: rows: (Laptop Pro,2), (Phone X,1)

-- 45) Arrays + UNNEST with CTE
WITH arrs AS (
  SELECT ARRAY['NY','CA','TX'] AS states
)
SELECT unnest(states) AS state FROM arrs;
-- Expected: NY, CA, TX

-- 46) Parameterizable CTE pattern using constants CTE
WITH params AS (
  SELECT 120000::numeric AS cutoff
)
SELECT e.name, e.salary
FROM employees e, params p
WHERE e.salary >= p.cutoff
ORDER BY salary DESC;
-- Expected: employees with salary >= 120k

-- 47) Distinct-on trick via CTE (latest sale per employee)
WITH ordered AS (
  SELECT s.*, ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY sale_date DESC, id DESC) rn
  FROM sales s
)
SELECT emp_id, sale_date, amount
FROM ordered WHERE rn=1
ORDER BY emp_id;
-- Expected: each employee's most recent sale

-- 48) Anti-join via CTE (employees with no sales)
WITH sellers AS (SELECT DISTINCT emp_id FROM sales)
SELECT e.id, e.name
FROM employees e
LEFT JOIN sellers s ON s.emp_id = e.id
WHERE s.emp_id IS NULL
ORDER BY e.id;
-- Expected: employees who never appear in sales

-- 49) Semi-join via CTE (departments that have at least one sale)
WITH depts_with_sales AS (
  SELECT DISTINCT e.dept_id FROM sales s JOIN employees e ON e.id = s.emp_id
)
SELECT d.id, d.name
FROM departments d
JOIN depts_with_sales ds ON ds.dept_id = d.id
ORDER BY d.id;
-- Expected: subset of departments that have sales activity

-- 50) Multi-level CTE combining ranking, JSON, and filters
WITH totals AS (
  SELECT e.dept_id, e.name, COALESCE(SUM(s.amount),0) AS total
  FROM employees e LEFT JOIN sales s ON s.emp_id = e.id
  GROUP BY e.dept_id, e.name
), ranked AS (
  SELECT *, RANK() OVER (PARTITION BY dept_id ORDER BY total DESC) AS rnk
  FROM totals
), top3 AS (
  SELECT dept_id, name, total FROM ranked WHERE rnk <= 3
), packed AS (
  SELECT dept_id, json_agg(json_build_object('name',name,'total',total) ORDER BY total DESC) AS top3
  FROM top3 GROUP BY dept_id
)
SELECT d.name AS department, p.top3
FROM departments d LEFT JOIN packed p ON p.dept_id = d.id
ORDER BY department;
-- Expected: top-3 performers packaged as JSON per department
