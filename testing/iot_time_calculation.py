import time

from google.cloud import bigquery
import pandas as pd
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r"C:\Users\Cristian\Downloads\aparkapp_bq_private_key.json"
TABLE ="dp2-test-342416.edemDataset.status"

# Construct a BigQuery client object.
client = bigquery.Client()
job_config = bigquery.QueryJobConfig(destination=TABLE)

df_input = pd.read_csv("testing/testing-iot.csv").to_dict('records')
print(df_input)

def upload_status(data):
    print('TO UPLOAD')
    print(data)
    data = [{'parking_id':data['parking_id'],'arrival_time':data['timeStamp'],'departure_time':None,'total_time':None}]
    bq_client = bigquery.Client()
    table = bq_client.get_table("{}.{}.{}".format("dp2-test-342416", "edemDataset", "status")) # TODO: Change to unified table variable
    errors = bq_client.insert_rows_json(table, data)
    if errors == []:
        print("success")

def update_status(data, previous_data):
    print(data)
    exit(0)
    errors = client.insert_rows_json(TABLE, data)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

def process_false(current_status, previus_status):
    if previus_status and previus_status['departure_time']!=None:

        start_timestamp = previus_status['arrival_time']
        end_timestamp = current_status['timeStamp']
        print(start_timestamp,end_timestamp)
        print(type(start_timestamp),type(end_timestamp))
        exit(0)

for current_status in df_input:

    """ READ BIGQUERY STATUS"""
    # q = """SELECT * FROM `dp2-test-342416.edemDataset.status` where parking_id = 'parking1' order by timeStamp desc Limit 1;"""
    q = f"""SELECT * FROM {TABLE} where parking_id = '{current_status['parking_id']}' order by arrival_time desc Limit 1;"""
    print(q)

    previus_status = (
        client.query(q)
        .result()
        .to_dataframe()).to_dict('records')
    print('#~#~#~#~#~#~#~#~#~#~# OJITO CON LO QUE TENEMOS #~#~#~#~#~#~#~#~#~#~#')
    print(previus_status, current_status)
    print(type(previus_status), current_status)

    if current_status['ocupado']:
        print('OCUPADO')
        upload_status(current_status)
    else:
        print('DESOCUPADO')
        process_false(current_status, previus_status)

    time.sleep(2) # TODO: quitar
