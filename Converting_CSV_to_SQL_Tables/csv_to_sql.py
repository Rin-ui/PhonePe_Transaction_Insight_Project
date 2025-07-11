import pandas as pd
from sqlalchemy import create_engine

# MySQL credentials
user = 'root'
password = 'ilovesuga'
host = 'localhost'
port = 3306
database = 'phonepedb'

# Create SQL connection engine
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

# File paths (with folders) and target SQL table names
csv_to_table = {
    'aggregated/insurance_aggregated_with_state_data.csv': 'insurance_aggregated',
    'aggregated/transaction_aggregated_with_state_data.csv': 'transaction_aggregated',
    'aggregated/user_aggregated_with_state_data.csv': 'user_aggregated',
    'map/insurance_map.csv': 'insurance_map',
    'map/transaction_map.csv': 'transaction_map',
    'map/user_map_data.csv': 'user_map',
    'top/insurance_top.csv': 'insurance_top',
    'top/transaction_top.csv': 'transaction_top',
    'top/user_top.csv': 'user_top'
}

# Upload each CSV into SQL table in chunks
chunk_size = 1000
for csv_file, table_name in csv_to_table.items():
    try:
        first_chunk = True
        for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
            chunk.to_sql(table_name, con=engine,
                         if_exists='replace' if first_chunk else 'append',
                         index=False)
            first_chunk = False
        print(f"Successfully uploaded '{csv_file}' to table '{table_name}' in chunks")
    except Exception as e:
        print(f"Failed to upload '{csv_file}': {e}")
