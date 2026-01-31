# Import Packages
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests
from io import StringIO
import urllib3
import sys
import os
import time

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure retry + timeout session
session = requests.Session()

retries = Retry(
    total=5, # Try at most 5 times per request
    backoff_factor=2, # Exponential backoff: wait 2s, then 4s, then 8s... between retries
    status_forcelist=[429,500,502,503,504],  # Only retry on these HTTP status codes
    allowed_methods=["GET"]     # Only retry GET requests (not POST, PUT, etc.)
)

adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

now = datetime.now()
year = now.year
month = now.strftime("%b").lower()  # jan, feb, mar
table_suffix = f"{year}_{month}"

# Initialize BigQuery client
client = bigquery.Client(project='data-storage-485106')

# Suppress InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Define Commodities
commodities = [
    140, # Cattle
    168, # Goats
    167, # Sheep
    211, # Pigs
    227, # Chicken
]

# Create New Empty DataFrame
bigdata = pd.DataFrame()

# Loop through commodities
for commodity in commodities:
    base_url = "https://kamis.kilimo.go.ke/site/market{}?product=" + str(commodity)+ "&per_page=3000"

    # Define Offset
    offset = 0

    # Run
    while True:
        try:
            # Handle first page (no offset in URL)
            url = base_url.format("" if offset == 0 else f"/{offset}")
            print(f"Fetching: {url}")
            
            response = session.get(url, verify=False, timeout=60)
            market_prices = pd.read_html(StringIO(response.text))

        except Exception as e:
            print(f"[WARN] KAMIS timeout or error for {url}: {e}")
            break  # stop paging for this commodity, continue script
        
        market_prices = market_prices[0]
        
        bigdata = pd.concat([bigdata, market_prices], ignore_index=True)
        offset += 3000

print(f"Collected {len(bigdata)} rows in total")

if bigdata.empty:
    print("[WARN] No data collected from KAMIS. Skipping BigQuery load.")
    
    # Exit script cleanly with success
    sys.exit(0)
# Clean Names
bigdata.columns = (
    bigdata.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace(r"[^0-9a-zA-Z_]", "", regex=True)
)

# Standardize Data Types
bigdata['date'] = pd.to_datetime(bigdata['date'])
bigdata['wholesale'] = pd.to_numeric(bigdata['wholesale'].str.extract(r'(\d+\.?\d*)')[0], errors='coerce')
bigdata['retail'] = pd.to_numeric(bigdata['retail'].str.extract(r'(\d+\.?\d*)')[0], errors='coerce')

# Define Table ID
table_id = f"data-storage-485106.livestock.market_prices_{table_suffix}"

if now.day == 1:
    try:
        prev_month_date = now.replace(day=1) - timedelta(days=1)
        prev_table_suffix = f"{prev_month_date.year}_{prev_month_date.strftime('%b').lower()}"
        prev_table_id = f"data-storage-485106.livestock.market_prices_{prev_table_suffix}"
        
        try:
            prev_data = client.query(
                f"SELECT * FROM `{prev_table_id}`"
            ).to_dataframe()
            bigdata = pd.concat([prev_data, bigdata], ignore_index=True)
            print(f"Appended {len(prev_data)} rows from previous month table.")
        except NotFound:
            print("No previous month table found, skipping append.")
        
        job = client.load_table_from_dataframe(
            bigdata,
            table_id,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        )
        job.result()
        print(f"All data loaded into {table_id}, total rows: {len(bigdata)}")

    except Exception as e:
        print(f"Error during 1st-of-month load: {e}")

else:
    # 🔥 NORMAL WORKFLOW (this was missing)
    job = client.load_table_from_dataframe(
        bigdata,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    print(f"Normal load completed into {table_id}, rows: {len(bigdata)}")

# Define SQL Query to Retrieve Open Weather Data from Google Cloud BigQuery
sql = (f"""
        SELECT *
        FROM `{table_id}`
       """)
    
# Run SQL Query
data = client.query(sql).to_dataframe()

# Check Shape of data from BigQuery
print(f"Shape of dataset from BigQuery : {data.shape}")

# Delete Original Table
client.delete_table(table_id)
print(f"Table deleted successfully.")

# Check Total Number of Duplicate Records
duplicated = data.duplicated(subset=['commodity', 'classification', 'grade', 'sex', 'market', 'wholesale',
       'retail', 'supply_volume', 'county', 'date']).sum()
    
# Remove Duplicate Records
data.drop_duplicates(subset=['commodity', 'classification', 'grade', 'sex', 'market', 'wholesale',
       'retail', 'supply_volume', 'county', 'date'], inplace=True)

# Define the dataset ID and table ID
dataset_id = 'livestock'
table_id = f'market_prices_{table_suffix}'
    
# Define the table schema for new table
schema = [
    bigquery.SchemaField("commodity", "STRING"),
    bigquery.SchemaField("classification", "STRING"),
    bigquery.SchemaField("grade", "STRING"),
    bigquery.SchemaField("sex", "STRING"),
    bigquery.SchemaField("market", "STRING"),
    bigquery.SchemaField("wholesale", "FLOAT"),
    bigquery.SchemaField("retail", "FLOAT"),
    bigquery.SchemaField("supply_volume", "FLOAT"),
    bigquery.SchemaField("county", "STRING"),
    bigquery.SchemaField("date", "DATE") 
]
    
# Define the table reference
table_ref = client.dataset(dataset_id).table(table_id)
    
# Create the table object
table = bigquery.Table(table_ref, schema=schema)

try:
    # Create the table in BigQuery
    table = client.create_table(table)
    print(f"Table {table.table_id} created successfully.")
except Exception as e:
    print(f"Table {table.table_id} failed")

# Define the BigQuery table ID
table_id = f'data-storage-485106.livestock.market_prices_{table_suffix}'

# Load the data into the BigQuery table
job = client.load_table_from_dataframe(data, table_id)

# Wait for the job to complete
while job.state != 'DONE':
    time.sleep(2)
    job.reload()
    print(job.state)

# Return Data Info
print(f"Livestock data of shape {data.shape} has been successfully retrieved, saved, and appended to the BigQuery table.")
