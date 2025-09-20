import sqlite3
import pandas as pd

# Define your database path and table name
db_path = "./db/customer_data.db"
table_name = "customer_info"

# Connect to the SQLite database
conn = sqlite3.connect(db_path)

# Read the table into a DataFrame
df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

# Close the connection
conn.close()

# Preview the data
print(df.head())