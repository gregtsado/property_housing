import subprocess
import time
from sqlalchemy import create_engine
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
load_dotenv(override=True)

def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    """Wait for PostgreSQL to become available."""
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True)
            if "accepting connections" in result.stdout:
                print("Successfully connected to PostgreSQL!")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error connecting to PostgreSQL: {e}")
            retries += 1
            print(
                f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})")
            time.sleep(delay_seconds)
    print("Max retries reached. Exiting.")
    return False

df = pd.read_csv('synthetic_property_data.csv')

property_dim = df[['property_id', 'construction_year', 'repair_year']].copy().drop_duplicates().reset_index(drop=True)

region_dim = df[[ 'region_name']].copy().drop_duplicates().reset_index(drop=True)
region_dim.index.name = 'region_id'

property_dim = property_dim.reset_index()
region_dim = region_dim.reset_index()

property_fact_table = df.merge(region_dim, on=['region_name'], how ='left')\
[['property_id', 'region_id','construction_year', 'repair_year','repair_count', 'total_repair_cost']]

property_dim.to_csv('property_dim.csv', index=False)
region_dim.to_csv('region_dim.csv', index=False)
property_fact_table.to_csv('property_fact_table.csv', index=False)


user = os.getenv('user')
password = os.getenv('password')
host = os.getenv('host')
port = os.getenv('port')
database = os.getenv('database')




if not all([user, password, host, port, database]):
    raise ValueError("One or more environment variables are missing. Ensure that POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST, and POSTGRES_PORT are set.")


db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"

engine = create_engine(db_url)

try:
  property_dim.to_sql('property_dim', engine, if_exists='replace', index=False)
except Exception as e:
  print(f"Error inserting data into property_dim table: {e}")

try:
  region_dim.to_sql('region_dim', engine, if_exists='replace', index=False)
except Exception as e:
  print(f"Error inserting data into region_dim table: {e}")

try:
  property_fact_table.to_sql('property_fact_table', engine, if_exists='replace', index=False)
except Exception as e:
  print(f"Error inserting data into property_fact_table: {e}")
  
print('End of ELT script. Data Model Successfully Created and loaded into database')