import pandas as pd
from google.cloud import bigquery
import os
TABLE =" dp2-test-342416.edemDataset.status"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r"C:\Users\Cristian\Downloads\aparkapp_bq_private_key.json"

# Construct a BigQuery client object.
client = bigquery.Client()

job_config = bigquery.QueryJobConfig(destination=TABLE)

df_input = pd.read_csv("testing/testing-iot.csv").to_dict('records')
print(df_input)


def check_True(e, status_df):
    print('TRUE')
    if status_df:
        print('not empty')
    else:
        print('empty')
        dataset_ref = client.dataset(TABLE)
        table_ref = dataset_ref.table('my_table_id')
        table = bigquery_client.get_table(table_ref)  # API call
        client.insert_rows(table, rows_to_insert)

def check_False(e, status_df):
    print('FALSE')
    if status_df:
        print('not empty')
    else:
        print('empty')

for e in df_input:

    print(e)
    """ READ BIGQUERY STATUS"""
    # q = """SELECT * FROM `dp2-test-342416.edemDataset.status` where parking_id = 'parking1' order by timeStamp desc Limit 1;"""
    q = f"""SELECT * FROM '{TABLE}' where parking_id = '{e['parking_id']}' order by arrival_time desc Limit 1;"""

    status_dc = (
        client.query(q)
        .result()
        .to_dataframe()).to_dict('records')
    print(status_dc)

    if e['ocupado']:
        check_True(e, status_dc)
    else:
        check_False(e, status_dc)

