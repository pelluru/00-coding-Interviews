-- 11_ctas_insert.sql

-- Example 181: CTAS pattern from set result
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

-- Create-table-as (CTAS) style (may require 'CREATE TABLE AS' in your DB)
-- CREATE TABLE dept_ids AS
SELECT * FROM (
  SELECT dept_id FROM employees
  UNION
  SELECT dept_id FROM departments
) s;

-- Example 182: CTAS pattern from set result
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

-- Create-table-as (CTAS) style (may require 'CREATE TABLE AS' in your DB)
-- CREATE TABLE dept_ids AS
SELECT * FROM (
  SELECT dept_id FROM employees
  UNION
  SELECT dept_id FROM departments
) s;

-- Example 183: CTAS pattern from set result
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

-- Create-table-as (CTAS) style (may require 'CREATE TABLE AS' in your DB)
-- CREATE TABLE dept_ids AS
SELECT * FROM (
  SELECT dept_id FROM employees
  UNION
  SELECT dept_id FROM departments
) s;

-- Example 184: CTAS pattern from set result
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

-- Create-table-as (CTAS) style (may require 'CREATE TABLE AS' in your DB)
-- CREATE TABLE dept_ids AS
SELECT * FROM (
  SELECT dept_id FROM employees
  UNION
  SELECT dept_id FROM departments
) s;

-- Example 185: CTAS pattern from set result
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

-- Create-table-as (CTAS) style (may require 'CREATE TABLE AS' in your DB)
-- CREATE TABLE dept_ids AS
SELECT * FROM (
  SELECT dept_id FROM employees
  UNION
  SELECT dept_id FROM departments
) s;

-- Example 186: INSERT from EXCEPT result
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

-- INSERT-SELECT from set
-- INSERT INTO some_table(col)
SELECT dept_id
FROM (
  SELECT dept_id FROM employees
  EXCEPT
  SELECT dept_id FROM projects
) t;

-- Example 187: INSERT from EXCEPT result
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

-- INSERT-SELECT from set
-- INSERT INTO some_table(col)
SELECT dept_id
FROM (
  SELECT dept_id FROM employees
  EXCEPT
  SELECT dept_id FROM projects
) t;

-- Example 188: INSERT from EXCEPT result
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

-- INSERT-SELECT from set
-- INSERT INTO some_table(col)
SELECT dept_id
FROM (
  SELECT dept_id FROM employees
  EXCEPT
  SELECT dept_id FROM projects
) t;

-- Example 189: INSERT from EXCEPT result
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

-- INSERT-SELECT from set
-- INSERT INTO some_table(col)
SELECT dept_id
FROM (
  SELECT dept_id FROM employees
  EXCEPT
  SELECT dept_id FROM projects
) t;

-- Example 190: INSERT from EXCEPT result
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

-- INSERT-SELECT from set
-- INSERT INTO some_table(col)
SELECT dept_id
FROM (
  SELECT dept_id FROM employees
  EXCEPT
  SELECT dept_id FROM projects
) t;

