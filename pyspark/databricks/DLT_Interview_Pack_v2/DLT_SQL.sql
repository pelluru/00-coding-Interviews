
CREATE LIVE TABLE orders_summary AS
SELECT customer_id, SUM(amount) AS total_amount
FROM STREAM(live.orders_raw)
GROUP BY customer_id;
