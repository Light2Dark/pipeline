from google.cloud import bigquery
import pandas as pd

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "pipeline-361916-22e8e98ff216.json"

pd.set_option("display.max_columns", 15)

# BigQuery client
client = bigquery.Client()

def imperative():
    dataset_ref = client.dataset("openaq", project="bigquery-public-data")
    dataset = client.get_dataset(dataset_ref)

    tables = list(client.list_tables(dataset))
        
    table_ref = dataset_ref.table("global_air_quality")

    table = client.get_table(table_ref)

    rows = client.list_rows(table, max_results=10).to_dataframe()


def dryRunQuery(query):
    dry_run_config = bigquery.QueryJobConfig(dry_run=True)
    dry_run_query_job = client.query(query, job_config=dry_run_config)
    
    mb_used = dry_run_query_job.total_bytes_processed * pow(10, -6)
    
    print("This query will process {} MB".format(mb_used))
    
def sql():
    query = """
        SELECT * FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country LIKE 'MY'
        LIMIT 10
    """
    
    ONE_HUNDRED_MB = 1000*1000*100
    safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=ONE_HUNDRED_MB)
    safe_query_job = client.query(query, job_config=safe_config)
    
    data = safe_query_job.to_dataframe()
    print(data)

    


if __name__ == "__main__":
    sql()