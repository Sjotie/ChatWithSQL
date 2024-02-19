import pandas as pd
import sqlite3
import os

# Step 1: Load the CSV data into a pandas DataFrame
csv_file = 'Walmart_sales.csv'
df = pd.read_csv(csv_file)

# Extract the base name of the file without the extension
base_name = os.path.splitext(os.path.basename(csv_file))[0]

# Step 2: Create a connection to a SQLite database
db_file = base_name + '.db'
conn = sqlite3.connect(db_file)

# Step 3: Write the DataFrame to the SQLite database
df.to_sql(base_name, conn, if_exists='replace', index=False)

# Don't forget to close the connection
conn.close()