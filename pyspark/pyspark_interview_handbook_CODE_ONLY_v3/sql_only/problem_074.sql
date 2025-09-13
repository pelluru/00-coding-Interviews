-- Problem 074: 074 - Spark SQL: Sql queries challenge
-- Category: Spark SQL

-- Register temp view in PySpark:
-- events.createOrReplaceTempView('events_view');

SELECT *
FROM events_view
LIMIT 100;
