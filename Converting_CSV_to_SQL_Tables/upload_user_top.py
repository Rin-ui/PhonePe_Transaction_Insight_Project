import pandas as pd
from sqlalchemy import create_engine

# MySQL Connection
engine = create_engine("mysql+pymysql://root:ilovesuga@localhost:3306/phonepedb")

# Read and clean the CSV
chunk_size = 1000
first_chunk = True

for chunk in pd.read_csv("top/user_top.csv", chunksize=chunk_size):
    # Ensure metric_type is string (not missing)
    if 'metric_type' in chunk.columns:
        chunk['metric_type'] = chunk['metric_type'].fillna('UNKNOWN').astype(str)
    else:
        print("'metric_type' column not found in the CSV.")
    
    # Ensure numeric values
    chunk['transaction_count'] = pd.to_numeric(chunk['transaction_count'], errors='coerce')
    chunk['transaction_amount'] = pd.to_numeric(chunk['transaction_amount'], errors='coerce')

    # Upload to MySQL
    chunk.to_sql("user_top", con=engine, if_exists='replace' if first_chunk else 'append', index=False)
    first_chunk = False

print(" 'user_top' table reloaded successfully with proper 'metric_type'")

