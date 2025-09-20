import pandas as pd
import sqlite3
import os

# Paths
csv_path = "./sample_data/customer_info_table.csv"
db_path = "./db/customer_data.db"
table_name = "customer_info"

# Creating db folder if not exists
os.makedirs(os.path.dirname(db_path), exist_ok=True)

# 1. Reading customer info CSV file
df = pd.read_csv(csv_path)

# 2. Create SQLite DB and write table
conn = sqlite3.connect(db_path)
df.to_sql(table_name, conn, index=False, if_exists="replace")
conn.close()

print(f"SQLite database created at {db_path} with table '{table_name}'.")