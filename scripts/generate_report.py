import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

conn = psycopg2.connect(
    host="postgres",
    database="airflow",
    user="airflow",
    password="airflow"
)

daily_sales = pd.read_sql("SELECT * FROM daily_sales ORDER BY sales_date", conn)
top_products = pd.read_sql("SELECT * FROM top_products", conn)
sales_per_country = pd.read_sql("SELECT * FROM sales_per_country", conn)

daily_sales.to_csv("C:/Users/arel/Documents/Bootcamp/Project/ecommerce_data_pipeline/reports/daily_sales1.csv", index=False)
top_products.to_csv("C:/Users/arel/Documents/Bootcamp/Project/ecommerce_data_pipeline/reports/top_products1.csv", index=False)
sales_per_country.to_csv("C:/Users/arel/Documents/Bootcamp/Project/ecommerce_data_pipeline/reports/sales_per_country1.csv", index=False)

plt.figure(figsize=(10,5))
plt.plot(daily_sales['sales_date'], daily_sales['total_sales'], marker='o')
plt.title("Daily Sales Trend")
plt.xlabel("sales_date")
plt.ylabel("Total Sales")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("C:/Users/arel/Documents/Bootcamp/Project/ecommerce_data_pipeline/reports/daily_sales_trend.png")

print("Reports & visualization generated successfully!")
