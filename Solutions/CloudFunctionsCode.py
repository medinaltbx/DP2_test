# Cloud Function triggered by PubSub Event

# Import libraries
import base64, json, sys, os


# Read from PubSub
def pubsub_to_iot(event, context):
    # Read message from Pubsub (decode from Base64)
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    # Load json
    message = json.loads(pubsub_message)

    console.log('ME LLEGA MENSAJE EN LA FUNCION CLOUD: %s', message)
    if message['status'] == 'salida':
        console.log("SALIDA")