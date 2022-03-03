import time
from datetime import datetime

from google.cloud import bigquery
import pandas as pd
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r"C:\Users\Cristian\Downloads\aparkapp_bq_private_key.json"
TABLE ="dp2-test-342416.edemDataset.status"

# Construct a BigQuery client object.
client = bigquery.Client()
job_config = bigquery.QueryJobConfig(destination=TABLE)

df_input = pd.read_csv(r"testing/testing-iot_1.csv").to_dict('records')

def upload_status(data , current_time):

    data = [{'parking_id':data['parking_id'],'arrival_time':data['timeStamp'],'departure_time':None,'total_time':None,'current_time':str(current_time)}]
    bq_client = bigquery.Client()
    table = bq_client.get_table("{}.{}.{}".format("dp2-test-342416", "edemDataset", "status")) # TODO: Change to unified table variable
    errors = bq_client.insert_rows_json(table, data)
    if errors == []:
        print("success")

def update_status(data):
    print('DATA TO UPDATE')
    print(data)
    table = client.get_table("{}.{}.{}".format("dp2-test-342416", "edemDataset", "status")) # TODO: Change to unified table variable
    errors = client.insert_rows_json(table, data)
    if errors == []:
        print("success")

def process_true(current_status, previous_status , current_time):
    if not previous_status:
        print('BQ VACIO')
        upload_status(current_status, current_time)
    else:
        if previous_status[0]['total_time'] != None:
            print('DEPARTURE TRIME IS NOT NONE.')
            upload_status(current_status, current_time)

def process_false(current_status, previous_status, current_time):
    if not previous_status:
        print('BQ VACIO')
    elif previous_status[0]['total_time']==None:
        previous_status = previous_status[0]

        date_format_str = '%Y-%m-%d %H:%M:%S.%f'
        start_timestamp = previous_status['arrival_time'].replace(tzinfo=None)
        print(start_timestamp,type(start_timestamp))
        start_timestamp = datetime.strptime(str(start_timestamp), date_format_str)
        start_timestamp = start_timestamp.strftime(date_format_str)
        print(start_timestamp, type(start_timestamp))
        end_timestamp = datetime.strptime(current_status['timeStamp'], date_format_str)
        diff = end_timestamp - start_timestamp

        status_update = [{'parking_id':previous_status['parking_id'],'arrival_time':previous_status['arrival_time'],'departure_time':current_status['timeStamp'],'total_time':str(diff),'current_time':current_time}]
        update_status(status_update)

for c,current_status in enumerate(df_input):
    current_time = str(datetime.now())
    print(current_time, type(current_time))
    current_time = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S.%f')
    print(current_time, type(current_time))
    """ READ BIGQUERY STATUS"""
    q = f"""SELECT * FROM `dp2-test-342416.edemDataset.status` WHERE `current_time`  = (SELECT MAX(`current_time`) FROM `{TABLE}`) \
            AND parking_id = '{current_status['parking_id']}'"""
    previous_status = (
        client.query(q)
        .result()
        .to_dataframe()).to_dict('records')
    print(f"################################ {c} #########################################")
    print(f'#~#~#~#~#~#~#~#~#~#~# PREVIOUS: {previous_status} #~#~#~#~#~#~#~#~#~#~#')
    # print(f'#~#~#~#~#~#~#~#~#~#~# CURRENT: {current_status} #~#~#~#~#~#~#~#~#~#~#')

    if current_status['ocupado']:
        print('OCUPADO')
        process_true(current_status, previous_status, current_time)
    else:
        print('DESOCUPADO')
        process_false(current_status, previous_status, current_time)

    time.sleep(2) # TODO: quitar
