DROP TABLE IF EXISTS sales_per_country;

CREATE TABLE sales_per_country AS
SELECT 
    "Country",
    SUM("Quantity" * "UnitPrice") AS total_sales,
    COUNT(DISTINCT "CustomerID") AS total_customers
FROM orders
WHERE "Quantity" > 0 AND "UnitPrice" > 0
GROUP BY "Country"
ORDER BY total_sales DESC;
