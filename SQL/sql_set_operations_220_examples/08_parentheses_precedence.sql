-- 08_parentheses_precedence.sql

-- Example 151: Parentheses to control precedence (INTERSECT before UNION)
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

-- Control precedence: INTERSECT binds before UNION in some engines; use parentheses
(SELECT dept_id FROM employees
 INTERSECT
 SELECT dept_id FROM departments)
UNION
SELECT dept_id FROM projects;

-- Example 152: Parentheses to control precedence (INTERSECT before UNION)
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

-- Control precedence: INTERSECT binds before UNION in some engines; use parentheses
(SELECT dept_id FROM employees
 INTERSECT
 SELECT dept_id FROM departments)
UNION
SELECT dept_id FROM projects;

-- Example 153: Parentheses to control precedence (INTERSECT before UNION)
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

-- Control precedence: INTERSECT binds before UNION in some engines; use parentheses
(SELECT dept_id FROM employees
 INTERSECT
 SELECT dept_id FROM departments)
UNION
SELECT dept_id FROM projects;

-- Example 154: Parentheses to control precedence (INTERSECT before UNION)
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

-- Control precedence: INTERSECT binds before UNION in some engines; use parentheses
(SELECT dept_id FROM employees
 INTERSECT
 SELECT dept_id FROM departments)
UNION
SELECT dept_id FROM projects;

-- Example 155: Parentheses to control precedence (INTERSECT before UNION)
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

-- Control precedence: INTERSECT binds before UNION in some engines; use parentheses
(SELECT dept_id FROM employees
 INTERSECT
 SELECT dept_id FROM departments)
UNION
SELECT dept_id FROM projects;

-- Example 156: Mixed UNION and EXCEPT with grouping
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

-- Alternate grouping
SELECT dept_id FROM employees
UNION
(SELECT dept_id FROM departments
 EXCEPT
 SELECT dept_id FROM projects);

-- Example 157: Mixed UNION and EXCEPT with grouping
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

-- Alternate grouping
SELECT dept_id FROM employees
UNION
(SELECT dept_id FROM departments
 EXCEPT
 SELECT dept_id FROM projects);

-- Example 158: Mixed UNION and EXCEPT with grouping
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

-- Alternate grouping
SELECT dept_id FROM employees
UNION
(SELECT dept_id FROM departments
 EXCEPT
 SELECT dept_id FROM projects);

-- Example 159: Mixed UNION and EXCEPT with grouping
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

-- Alternate grouping
SELECT dept_id FROM employees
UNION
(SELECT dept_id FROM departments
 EXCEPT
 SELECT dept_id FROM projects);

-- Example 160: Mixed UNION and EXCEPT with grouping
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

-- Alternate grouping
SELECT dept_id FROM employees
UNION
(SELECT dept_id FROM departments
 EXCEPT
 SELECT dept_id FROM projects);

