DROP TABLE IF EXISTS data_quality_issues;

CREATE TABLE data_quality_issues AS
SELECT 'NULL InvoiceNo' AS issue, COUNT(*) AS count
FROM orders
WHERE "InvoiceNo" IS NULL

UNION ALL
SELECT 'Negative Quantity', COUNT(*)
FROM orders
WHERE "Quantity" < 0

UNION ALL
SELECT 'Zero or Negative Price', COUNT(*)
FROM orders
WHERE "UnitPrice" <= 0

UNION ALL
SELECT 'NULL CustomerID', COUNT(*)
FROM orders
WHERE "CustomerID" IS NULL

UNION ALL
SELECT 'Duplicate InvoiceNo + StockCode', COUNT(*)
FROM (
    SELECT "InvoiceNo", "StockCode", COUNT(*)
    FROM orders
    GROUP BY "InvoiceNo", "StockCode"
    HAVING COUNT(*) > 1
) t;
