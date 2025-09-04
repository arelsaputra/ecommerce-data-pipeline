DROP TABLE IF EXISTS top_products;

CREATE TABLE top_products AS
SELECT 
    "StockCode",
    "Description",
    SUM("Quantity") AS total_quantity,
    SUM("Quantity" * "UnitPrice") AS total_sales
FROM orders
WHERE "Quantity" > 0 AND "UnitPrice" > 0
GROUP BY "StockCode", "Description"
ORDER BY total_sales DESC
LIMIT 20;
