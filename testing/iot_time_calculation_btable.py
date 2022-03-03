import time
import datetime

from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters
import pandas as pd
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r"C:\Users\Cristian\Downloads\aparkapp_bq_private_key.json"
project_id ="dp2-test-342416"
instance_id = table_id = "status"

# Construct a BigQuery client object.
client = bigtable.Client(project=project_id, admin=True)
instance = client.instance(instance_id)

df_input = pd.read_csv("testing/testing-iot_1.csv").to_dict('records')

def upload_status(data):

    data = [{'parking_id':data['parking_id'],'arrival_time':data['timeStamp'],'departure_time':None,'total_time':None}]
    bq_client = bigquery.Client()
    table = bq_client.get_table("{}.{}.{}".format("dp2-test-342416", "edemDataset", "status")) # TODO: Change to unified table variable
    errors = bq_client.insert_rows_json(table, data)
    if errors == []:
        print("success")

def update_status(data):
    print('DATA TO UPDATE')
    print(data)
    query = f"UPDATE '{TABLE}' SET " \
    f"departure_time = '{data['departure_time']}', "\
    f"total_time = '{data['total_time']}' " \
    f"WHERE parking_id = '{data['parking_id']}' AND arrival_time = '{data['arrival_time']}'"
    print(query)
    # query_job = client.query((dml_statement))  # API request
    # query_job.result()  # Waits for statement to finish
    client.query(query)

def process_true(current_status, previous_status):
    if not previous_status:
        print('BQ VACIO')
        upload_status(current_status)
    else:
        if previous_status[0]['departure_time'] != None:
            upload_status(current_status)

def process_false(current_status, previous_status):
    print('ENTRO A PRERPOCESS',previous_status)
    if not previous_status:
        print('BQ VACIO')
    elif previous_status[0]['departure_time']==None:
        previous_status = previous_status[0]
        date_format_str = '%Y-%m-%d %H:%M:%S.%f'
        start_timestamp = datetime.strptime(previous_status['arrival_time'], date_format_str)
        end_timestamp = datetime.strptime(current_status['timeStamp'], date_format_str)
        diff = end_timestamp - start_timestamp
        print(diff)
        status_update = {'parking_id':previous_status['parking_id'],'arrival_time':previous_status['arrival_time'],'departure_time':current_status['timeStamp'],'total_time':str(diff)}
        update_status(status_update)

for c,current_status in enumerate(df_input):

    """ READ BIGQUERY STATUS"""
    q = f"""SELECT * FROM {TABLE} where parking_id = '{current_status['parking_id']}' order by arrival_time desc Limit 1;"""

    previous_status = (
        client.query(q)
        .result()
        .to_dataframe()).to_dict('records')
    print(f'#~#~#~#~#~#~#~#~#~#~# {c} #~#~#~#~#~#~#~#~#~#~#')
    # print(previous_status, current_status)

    if current_status['ocupado']:
        print('OCUPADO')
        process_true(current_status, previous_status)
    else:
        print('DESOCUPADO')
        process_false(current_status, previous_status)

    time.sleep(2) # TODO: quitar
