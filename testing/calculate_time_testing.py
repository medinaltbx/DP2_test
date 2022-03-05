import time
from datetime import datetime

from google.cloud import bigquery
import pandas as pd
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r"C:\Users\Cristian\Downloads\aparkapp_bq_private_key.json"


def calculate_time():
    TABLE_READ = "`dp2-test-342416.edemDataset.iotToBigQuery`"
    TABLE_DESTINATION ="dp2-test-342416.edemDataset.status"

    # Construct a BigQuery client object.
    client = bigquery.Client()

    message = {'parking_id':'parking1','status':'salida','timeStamp':'2022-03-05 19:52:59.293516'}

    def parse_time(timestamp):
        return datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')

    departure_time = parse_time(message['timeStamp'])
    print(departure_time, type(departure_time))

    """ READ BIGQUERY STATUS"""
    q = f"""SELECT * FROM {TABLE_READ} WHERE parking_id = '{message['parking_id']}' AND status = 'llegada'"""
    df = (client.query(q).result().to_dataframe())

    """ GET LAST ARRIVAL """
    df['timeStamp'] = df['timeStamp'].apply(parse_time)
    df = df[df['timeStamp']<departure_time].sort_values(by=['timeStamp'])
    arrival_time = df['timeStamp'].iloc[-1]

    ellapsed_time = departure_time - arrival_time

    status = [{'parking_id': message['parking_id'], 'arrival_time': str(arrival_time),
                     'departure_time': str(departure_time), 'total_time': str(ellapsed_time)}]
    bq_client = bigquery.Client()
    table = bq_client.get_table(TABLE_DESTINATION)
    errors = bq_client.insert_rows_json(table, status)
    if errors == []:
        print("success")


calculate_time()



