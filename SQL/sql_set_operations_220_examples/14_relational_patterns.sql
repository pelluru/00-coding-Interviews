-- 14_relational_patterns.sql

-- Example 211: Relational division-ish via EXCEPT (illustrative)
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

-- Relational division: find customers whose city appears among employees' names (silly demo)
-- (customers.city) ⊆ (employee name tokens) — illustrative only
WITH emp_tokens(tok) AS (
  SELECT name FROM employees
)
SELECT DISTINCT city FROM customers
EXCEPT
SELECT tok FROM emp_tokens;

-- Example 212: Relational division-ish via EXCEPT (illustrative)
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

-- Relational division: find customers whose city appears among employees' names (silly demo)
-- (customers.city) ⊆ (employee name tokens) — illustrative only
WITH emp_tokens(tok) AS (
  SELECT name FROM employees
)
SELECT DISTINCT city FROM customers
EXCEPT
SELECT tok FROM emp_tokens;

-- Example 213: Relational division-ish via EXCEPT (illustrative)
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

-- Relational division: find customers whose city appears among employees' names (silly demo)
-- (customers.city) ⊆ (employee name tokens) — illustrative only
WITH emp_tokens(tok) AS (
  SELECT name FROM employees
)
SELECT DISTINCT city FROM customers
EXCEPT
SELECT tok FROM emp_tokens;

-- Example 214: Relational division-ish via EXCEPT (illustrative)
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

-- Relational division: find customers whose city appears among employees' names (silly demo)
-- (customers.city) ⊆ (employee name tokens) — illustrative only
WITH emp_tokens(tok) AS (
  SELECT name FROM employees
)
SELECT DISTINCT city FROM customers
EXCEPT
SELECT tok FROM emp_tokens;

-- Example 215: Relational division-ish via EXCEPT (illustrative)
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

-- Relational division: find customers whose city appears among employees' names (silly demo)
-- (customers.city) ⊆ (employee name tokens) — illustrative only
WITH emp_tokens(tok) AS (
  SELECT name FROM employees
)
SELECT DISTINCT city FROM customers
EXCEPT
SELECT tok FROM emp_tokens;

-- Example 216: Exclusive OR (XOR) of two predicates via symmetric difference
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

-- Using set ops to implement classic problems: customers in NY or SF but not both
(SELECT cust_id FROM customers WHERE city='NY'
 EXCEPT
 SELECT cust_id FROM customers WHERE city='SF')
UNION
(SELECT cust_id FROM customers WHERE city='SF'
 EXCEPT
 SELECT cust_id FROM customers WHERE city='NY');

-- Example 217: Exclusive OR (XOR) of two predicates via symmetric difference
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

-- Using set ops to implement classic problems: customers in NY or SF but not both
(SELECT cust_id FROM customers WHERE city='NY'
 EXCEPT
 SELECT cust_id FROM customers WHERE city='SF')
UNION
(SELECT cust_id FROM customers WHERE city='SF'
 EXCEPT
 SELECT cust_id FROM customers WHERE city='NY');

-- Example 218: Exclusive OR (XOR) of two predicates via symmetric difference
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

-- Using set ops to implement classic problems: customers in NY or SF but not both
(SELECT cust_id FROM customers WHERE city='NY'
 EXCEPT
 SELECT cust_id FROM customers WHERE city='SF')
UNION
(SELECT cust_id FROM customers WHERE city='SF'
 EXCEPT
 SELECT cust_id FROM customers WHERE city='NY');

-- Example 219: Exclusive OR (XOR) of two predicates via symmetric difference
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

-- Using set ops to implement classic problems: customers in NY or SF but not both
(SELECT cust_id FROM customers WHERE city='NY'
 EXCEPT
 SELECT cust_id FROM customers WHERE city='SF')
UNION
(SELECT cust_id FROM customers WHERE city='SF'
 EXCEPT
 SELECT cust_id FROM customers WHERE city='NY');

-- Example 220: Exclusive OR (XOR) of two predicates via symmetric difference
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

-- Using set ops to implement classic problems: customers in NY or SF but not both
(SELECT cust_id FROM customers WHERE city='NY'
 EXCEPT
 SELECT cust_id FROM customers WHERE city='SF')
UNION
(SELECT cust_id FROM customers WHERE city='SF'
 EXCEPT
 SELECT cust_id FROM customers WHERE city='NY');

