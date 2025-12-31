import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('inventory.db')

# Read the vendor_sales_summary table
df = pd.read_sql_query("SELECT * FROM vendor_sales_summary", conn)

# Export to CSV
df.to_csv('vendor_sales_summary.csv', index=False)

print(f"CSV file created successfully!")
print(f"Total records: {len(df)}")
print(f"File saved as: vendor_sales_summary.csv")

conn.close()
