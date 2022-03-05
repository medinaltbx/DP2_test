# Cloud Function triggered by PubSub Event
# When a temperature over 23ºC or under 17ºC is received, a IoT Core command will be throw.

# Import libraries
import base64, json, sys, os



# Read from PubSub
def pubsub_to_iot(event, context):
    # Read message from Pubsub (decode from Base64)
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    # Load json
    message = json.loads(pubsub_message)

    console.log('ME LLEGA MENSAJE EN LA FUNCION CLOUD: ', message)
    if message['status'] == 'salida':
        console.log("SALIDA")