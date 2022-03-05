# Cloud Function triggered by PubSub Event
# When a temperature over 23ºC or under 17ºC is received, a IoT Core command will be throw.

# Import libraries
from datetime import datetime
from google.cloud import bigquery
import base64, json, sys, os
import google.cloud.logging
import logging
import pandas as pd

# Read from PubSub
def calculate_time():
    client = google.cloud.logging.Client()
    client.setup_logging()

    # Read message from Pubsub (decode from Base64)
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    # Load json
    message = json.loads(pubsub_message)

    logging.info(message)
    logging.debug(message)

    if message['status'] == 'salida':
        TABLE_READ = "`dp2-test-342416.edemDataset.iotToBigQuery`"
        TABLE_DESTINATION ="dp2-test-342416.edemDataset.status"

        # Construct a BigQuery client object.
        client = bigquery.Client()

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

        status = [{'parking_id': message['parking_id'],
                    'arrival_time': str(arrival_time),
                    'departure_time': str(departure_time),
                    'total_time': str(ellapsed_time)}]
        bq_client = bigquery.Client()

        table = bq_client.get_table(TABLE_DESTINATION)
        errors = bq_client.insert_rows_json(table, status)
        if errors == []:
            logging.info(" #~#~#~#~#~#~# SUCCESS #~#~#~#~#~#~#")
        else:
            logging.info(" #~#~#~#~#~#~# FALLO #~#~#~#~#~#~#")
