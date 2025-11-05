-- 12_window_before_setops.sql

-- Example 191: Window functions in operands; UNION ALL to stack
WITH
employees(id, name, dept_id) AS (
  VALUES (1,'Ana',10),(2,'Ben',20),(3,'Cia',10),(4,'Dan',30),(5,'Eve',NULL)
),
departments(dept_id, dept_name) AS (
  VALUES (10,'Sales'),(20,'Eng'),(30,'HR'),(40,'Ops')
),
projects(pid, dept_id) AS (
  VALUES (100,10),(101,20),(102,20),(103,50)
),
orders(oid, cust_id, amount) AS (
  VALUES (1,101,50.0),(2,102,80.0),(3,101,50.0),(4,103,NULL)
),
customers(cust_id, name, city) AS (
  VALUES (101,'Acme','NY'),(102,'Bolt','SF'),(103,'Core','NY'),(104,'Delta',NULL)
)

-- Apply window function per source, then UNION
WITH e AS (
  SELECT id, name, dept_id,
         ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY id) AS rn
  FROM employees
),
d AS (
  SELECT dept_id, dept_name,
         ROW_NUMBER() OVER (ORDER BY dept_id) AS rn
  FROM departments
)
SELECT dept_id, rn FROM e
UNION ALL
SELECT dept_id, rn FROM d;

-- Example 192: Window functions in operands; UNION ALL to stack
WITH
employees(id, name, dept_id) AS (
  VALUES (1,'Ana',10),(2,'Ben',20),(3,'Cia',10),(4,'Dan',30),(5,'Eve',NULL)
),
departments(dept_id, dept_name) AS (
  VALUES (10,'Sales'),(20,'Eng'),(30,'HR'),(40,'Ops')
),
projects(pid, dept_id) AS (
  VALUES (100,10),(101,20),(102,20),(103,50)
),
orders(oid, cust_id, amount) AS (
  VALUES (1,101,50.0),(2,102,80.0),(3,101,50.0),(4,103,NULL)
),
customers(cust_id, name, city) AS (
  VALUES (101,'Acme','NY'),(102,'Bolt','SF'),(103,'Core','NY'),(104,'Delta',NULL)
)

-- Apply window function per source, then UNION
WITH e AS (
  SELECT id, name, dept_id,
         ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY id) AS rn
  FROM employees
),
d AS (
  SELECT dept_id, dept_name,
         ROW_NUMBER() OVER (ORDER BY dept_id) AS rn
  FROM departments
)
SELECT dept_id, rn FROM e
UNION ALL
SELECT dept_id, rn FROM d;

-- Example 193: Window functions in operands; UNION ALL to stack
WITH
employees(id, name, dept_id) AS (
  VALUES (1,'Ana',10),(2,'Ben',20),(3,'Cia',10),(4,'Dan',30),(5,'Eve',NULL)
),
departments(dept_id, dept_name) AS (
  VALUES (10,'Sales'),(20,'Eng'),(30,'HR'),(40,'Ops')
),
projects(pid, dept_id) AS (
  VALUES (100,10),(101,20),(102,20),(103,50)
),
orders(oid, cust_id, amount) AS (
  VALUES (1,101,50.0),(2,102,80.0),(3,101,50.0),(4,103,NULL)
),
customers(cust_id, name, city) AS (
  VALUES (101,'Acme','NY'),(102,'Bolt','SF'),(103,'Core','NY'),(104,'Delta',NULL)
)

-- Apply window function per source, then UNION
WITH e AS (
  SELECT id, name, dept_id,
         ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY id) AS rn
  FROM employees
),
d AS (
  SELECT dept_id, dept_name,
         ROW_NUMBER() OVER (ORDER BY dept_id) AS rn
  FROM departments
)
SELECT dept_id, rn FROM e
UNION ALL
SELECT dept_id, rn FROM d;

-- Example 194: Window functions in operands; UNION ALL to stack
WITH
employees(id, name, dept_id) AS (
  VALUES (1,'Ana',10),(2,'Ben',20),(3,'Cia',10),(4,'Dan',30),(5,'Eve',NULL)
),
departments(dept_id, dept_name) AS (
  VALUES (10,'Sales'),(20,'Eng'),(30,'HR'),(40,'Ops')
),
projects(pid, dept_id) AS (
  VALUES (100,10),(101,20),(102,20),(103,50)
),
orders(oid, cust_id, amount) AS (
  VALUES (1,101,50.0),(2,102,80.0),(3,101,50.0),(4,103,NULL)
),
customers(cust_id, name, city) AS (
  VALUES (101,'Acme','NY'),(102,'Bolt','SF'),(103,'Core','NY'),(104,'Delta',NULL)
)

-- Apply window function per source, then UNION
WITH e AS (
  SELECT id, name, dept_id,
         ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY id) AS rn
  FROM employees
),
d AS (
  SELECT dept_id, dept_name,
         ROW_NUMBER() OVER (ORDER BY dept_id) AS rn
  FROM departments
)
SELECT dept_id, rn FROM e
UNION ALL
SELECT dept_id, rn FROM d;

-- Example 195: Window functions in operands; UNION ALL to stack
WITH
employees(id, name, dept_id) AS (
  VALUES (1,'Ana',10),(2,'Ben',20),(3,'Cia',10),(4,'Dan',30),(5,'Eve',NULL)
),
departments(dept_id, dept_name) AS (
  VALUES (10,'Sales'),(20,'Eng'),(30,'HR'),(40,'Ops')
),
projects(pid, dept_id) AS (
  VALUES (100,10),(101,20),(102,20),(103,50)
),
orders(oid, cust_id, amount) AS (
  VALUES (1,101,50.0),(2,102,80.0),(3,101,50.0),(4,103,NULL)
),
customers(cust_id, name, city) AS (
  VALUES (101,'Acme','NY'),(102,'Bolt','SF'),(103,'Core','NY'),(104,'Delta',NULL)
)

-- Apply window function per source, then UNION
WITH e AS (
  SELECT id, name, dept_id,
         ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY id) AS rn
  FROM employees
),
d AS (
  SELECT dept_id, dept_name,
         ROW_NUMBER() OVER (ORDER BY dept_id) AS rn
  FROM departments
)
SELECT dept_id, rn FROM e
UNION ALL
SELECT dept_id, rn FROM d;

-- Example 196: Intersection after windowed filtering (requires QUALIFY; emulate with subqueries if not supported)
WITH
employees(id, name, dept_id) AS (
  VALUES (1,'Ana',10),(2,'Ben',20),(3,'Cia',10),(4,'Dan',30),(5,'Eve',NULL)
),
departments(dept_id, dept_name) AS (
  VALUES (10,'Sales'),(20,'Eng'),(30,'HR'),(40,'Ops')
),
projects(pid, dept_id) AS (
  VALUES (100,10),(101,20),(102,20),(103,50)
),
orders(oid, cust_id, amount) AS (
  VALUES (1,101,50.0),(2,102,80.0),(3,101,50.0),(4,103,NULL)
),
customers(cust_id, name, city) AS (
  VALUES (101,'Acme','NY'),(102,'Bolt','SF'),(103,'Core','NY'),(104,'Delta',NULL)
)

-- Rank top dept_ids from employees and projects separately, then intersect the top-1 sets
WITH e AS (
  SELECT dept_id FROM employees
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY id) = 1
),
p AS (
  SELECT dept_id FROM projects
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY pid) = 1
)
SELECT dept_id FROM e
INTERSECT
SELECT dept_id FROM p;

-- Example 197: Intersection after windowed filtering (requires QUALIFY; emulate with subqueries if not supported)
WITH
employees(id, name, dept_id) AS (
  VALUES (1,'Ana',10),(2,'Ben',20),(3,'Cia',10),(4,'Dan',30),(5,'Eve',NULL)
),
departments(dept_id, dept_name) AS (
  VALUES (10,'Sales'),(20,'Eng'),(30,'HR'),(40,'Ops')
),
projects(pid, dept_id) AS (
  VALUES (100,10),(101,20),(102,20),(103,50)
),
orders(oid, cust_id, amount) AS (
  VALUES (1,101,50.0),(2,102,80.0),(3,101,50.0),(4,103,NULL)
),
customers(cust_id, name, city) AS (
  VALUES (101,'Acme','NY'),(102,'Bolt','SF'),(103,'Core','NY'),(104,'Delta',NULL)
)

-- Rank top dept_ids from employees and projects separately, then intersect the top-1 sets
WITH e AS (
  SELECT dept_id FROM employees
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY id) = 1
),
p AS (
  SELECT dept_id FROM projects
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY pid) = 1
)
SELECT dept_id FROM e
INTERSECT
SELECT dept_id FROM p;

-- Example 198: Intersection after windowed filtering (requires QUALIFY; emulate with subqueries if not supported)
WITH
employees(id, name, dept_id) AS (
  VALUES (1,'Ana',10),(2,'Ben',20),(3,'Cia',10),(4,'Dan',30),(5,'Eve',NULL)
),
departments(dept_id, dept_name) AS (
  VALUES (10,'Sales'),(20,'Eng'),(30,'HR'),(40,'Ops')
),
projects(pid, dept_id) AS (
  VALUES (100,10),(101,20),(102,20),(103,50)
),
orders(oid, cust_id, amount) AS (
  VALUES (1,101,50.0),(2,102,80.0),(3,101,50.0),(4,103,NULL)
),
customers(cust_id, name, city) AS (
  VALUES (101,'Acme','NY'),(102,'Bolt','SF'),(103,'Core','NY'),(104,'Delta',NULL)
)

-- Rank top dept_ids from employees and projects separately, then intersect the top-1 sets
WITH e AS (
  SELECT dept_id FROM employees
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY id) = 1
),
p AS (
  SELECT dept_id FROM projects
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY pid) = 1
)
SELECT dept_id FROM e
INTERSECT
SELECT dept_id FROM p;

-- Example 199: Intersection after windowed filtering (requires QUALIFY; emulate with subqueries if not supported)
WITH
employees(id, name, dept_id) AS (
  VALUES (1,'Ana',10),(2,'Ben',20),(3,'Cia',10),(4,'Dan',30),(5,'Eve',NULL)
),
departments(dept_id, dept_name) AS (
  VALUES (10,'Sales'),(20,'Eng'),(30,'HR'),(40,'Ops')
),
projects(pid, dept_id) AS (
  VALUES (100,10),(101,20),(102,20),(103,50)
),
orders(oid, cust_id, amount) AS (
  VALUES (1,101,50.0),(2,102,80.0),(3,101,50.0),(4,103,NULL)
),
customers(cust_id, name, city) AS (
  VALUES (101,'Acme','NY'),(102,'Bolt','SF'),(103,'Core','NY'),(104,'Delta',NULL)
)

-- Rank top dept_ids from employees and projects separately, then intersect the top-1 sets
WITH e AS (
  SELECT dept_id FROM employees
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY id) = 1
),
p AS (
  SELECT dept_id FROM projects
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY pid) = 1
)
SELECT dept_id FROM e
INTERSECT
SELECT dept_id FROM p;

-- Example 200: Intersection after windowed filtering (requires QUALIFY; emulate with subqueries if not supported)
WITH
employees(id, name, dept_id) AS (
  VALUES (1,'Ana',10),(2,'Ben',20),(3,'Cia',10),(4,'Dan',30),(5,'Eve',NULL)
),
departments(dept_id, dept_name) AS (
  VALUES (10,'Sales'),(20,'Eng'),(30,'HR'),(40,'Ops')
),
projects(pid, dept_id) AS (
  VALUES (100,10),(101,20),(102,20),(103,50)
),
orders(oid, cust_id, amount) AS (
  VALUES (1,101,50.0),(2,102,80.0),(3,101,50.0),(4,103,NULL)
),
customers(cust_id, name, city) AS (
  VALUES (101,'Acme','NY'),(102,'Bolt','SF'),(103,'Core','NY'),(104,'Delta',NULL)
)

-- Rank top dept_ids from employees and projects separately, then intersect the top-1 sets
WITH e AS (
  SELECT dept_id FROM employees
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY id) = 1
),
p AS (
  SELECT dept_id FROM projects
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY pid) = 1
)
SELECT dept_id FROM e
INTERSECT
SELECT dept_id FROM p;

