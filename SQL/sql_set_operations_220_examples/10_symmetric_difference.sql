-- 10_symmetric_difference.sql

-- Example 171: Symmetric difference of dept_ids
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

-- Symmetric difference: (A EXCEPT B) UNION (B EXCEPT A)
(SELECT dept_id FROM employees
  EXCEPT
 SELECT dept_id FROM departments)
UNION
(SELECT dept_id FROM departments
  EXCEPT
 SELECT dept_id FROM employees);

-- Example 172: Symmetric difference of dept_ids
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

-- Symmetric difference: (A EXCEPT B) UNION (B EXCEPT A)
(SELECT dept_id FROM employees
  EXCEPT
 SELECT dept_id FROM departments)
UNION
(SELECT dept_id FROM departments
  EXCEPT
 SELECT dept_id FROM employees);

-- Example 173: Symmetric difference of dept_ids
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

-- Symmetric difference: (A EXCEPT B) UNION (B EXCEPT A)
(SELECT dept_id FROM employees
  EXCEPT
 SELECT dept_id FROM departments)
UNION
(SELECT dept_id FROM departments
  EXCEPT
 SELECT dept_id FROM employees);

-- Example 174: Symmetric difference of dept_ids
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

-- Symmetric difference: (A EXCEPT B) UNION (B EXCEPT A)
(SELECT dept_id FROM employees
  EXCEPT
 SELECT dept_id FROM departments)
UNION
(SELECT dept_id FROM departments
  EXCEPT
 SELECT dept_id FROM employees);

-- Example 175: Symmetric difference of dept_ids
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

-- Symmetric difference: (A EXCEPT B) UNION (B EXCEPT A)
(SELECT dept_id FROM employees
  EXCEPT
 SELECT dept_id FROM departments)
UNION
(SELECT dept_id FROM departments
  EXCEPT
 SELECT dept_id FROM employees);

-- Example 176: Symmetric difference of names
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

-- Symmetric difference on names
(SELECT name FROM employees
  EXCEPT
 SELECT name FROM customers)
UNION
(SELECT name FROM customers
  EXCEPT
 SELECT name FROM employees);

-- Example 177: Symmetric difference of names
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

-- Symmetric difference on names
(SELECT name FROM employees
  EXCEPT
 SELECT name FROM customers)
UNION
(SELECT name FROM customers
  EXCEPT
 SELECT name FROM employees);

-- Example 178: Symmetric difference of names
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

-- Symmetric difference on names
(SELECT name FROM employees
  EXCEPT
 SELECT name FROM customers)
UNION
(SELECT name FROM customers
  EXCEPT
 SELECT name FROM employees);

-- Example 179: Symmetric difference of names
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

-- Symmetric difference on names
(SELECT name FROM employees
  EXCEPT
 SELECT name FROM customers)
UNION
(SELECT name FROM customers
  EXCEPT
 SELECT name FROM employees);

-- Example 180: Symmetric difference of names
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

-- Symmetric difference on names
(SELECT name FROM employees
  EXCEPT
 SELECT name FROM customers)
UNION
(SELECT name FROM customers
  EXCEPT
 SELECT name FROM employees);

