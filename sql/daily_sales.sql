DROP TABLE IF EXISTS daily_sales;

CREATE TABLE daily_sales AS
SELECT 
    DATE("InvoiceDate") AS sales_date,
    SUM("Quantity" * "UnitPrice") AS total_sales,
    COUNT(DISTINCT "InvoiceNo") AS total_orders,
    COUNT(DISTINCT "CustomerID") AS total_customers
FROM orders
WHERE "Quantity" > 0 AND "UnitPrice" > 0
GROUP BY DATE("InvoiceDate")
ORDER BY sales_date;

CREATE INDEX idx_daily_sales_date ON daily_sales(sales_date);

SELECT * FROM daily_sales ORDER BY sales_date LIMIT 10;
