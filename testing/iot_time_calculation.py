import pandas as pd
from google.cloud import bigquery
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r"C:\Users\Cristian\Downloads\aparkapp_bq_private_key.json"

# Construct a BigQuery client object.
client = bigquery.Client()

job_config = bigquery.QueryJobConfig(destination="dp2-test-342416.edemDataset.status")

df_input = pd.read_csv("testing/testing-iot.csv")
print(df_input)


""" READ BIGQUERY STATUS"""
# q = """SELECT * FROM `dp2-test-342416.edemDataset.status` where parking_id = 'parking1' order by timeStamp desc Limit 1;"""
q = """SELECT * FROM `dp2-test-342416.edemDataset.status` where parking_id = 'parking1';"""


status_df = (
    client.query(q)
    .result()
    .to_dataframe())
print(status_df)

