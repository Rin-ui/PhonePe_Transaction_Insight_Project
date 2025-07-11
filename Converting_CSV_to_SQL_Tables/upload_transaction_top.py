import pandas as pd
from sqlalchemy import create_engine

# SQL connection
engine = create_engine("mysql+pymysql://root:ilovesuga@localhost:3306/phonepedb")

# Load in chunks for transaction_top
chunk_size = 1000
first_chunk = True
for chunk in pd.read_csv("top/transaction_top.csv", chunksize=chunk_size):
    # Optional cleanup
    chunk['entity_name'] = chunk['entity_name'].astype(str)
    chunk['transaction_count'] = pd.to_numeric(chunk['transaction_count'], errors='coerce')
    chunk['transaction_amount'] = pd.to_numeric(chunk['transaction_amount'], errors='coerce')
    
    chunk.to_sql("transaction_top", con=engine, if_exists='replace' if first_chunk else 'append', index=False)
    first_chunk = False

print(" transaction_top uploaded successfully")
