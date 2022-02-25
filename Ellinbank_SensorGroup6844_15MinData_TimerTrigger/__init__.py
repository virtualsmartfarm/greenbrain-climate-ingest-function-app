import datetime
import logging
import azure.functions as func
import json
import pendulum
import requests
import os
from azure.cosmos import CosmosClient, PartitionKey, exceptions
import pandas as pd
try:
    greenbrain_username=os.environ['GREENBRAIN_USERNAME']
except:
    pass
greenbrain_username="matthew.cox@ecodev.vic.gov.au"
try:
    greenbrain_password=os.environ['GREENBRAIN_PASSWORD']
except:
    pass
greenbrain_password="FrlyuzucRIm0"
try:
    cosmosdb_key_vsfdatawatch=os.environ['COSMOSDB_KEY_VSFDATAWATCH']
except:
    pass
cosmosdb_key_vsfdatawatch='0E8mMnMMb3LzIgQkUFlpcjiyH8R2rJHOpq8ejuWG14hiGxehLYzROEuHSyaYIV1yj0yBgQrSm6S6Gb7dvnhsHA=='
greenbrain_endpoint='https://api.greenbrain.net.au/v3'
auth_login = '/auth/login'
bootstrap_uri = '/bootstrap'
cosmosdb_endpoint='https://vsfdatawatch.documents.azure.com:443/'
client = CosmosClient(url=cosmosdb_endpoint, credential=cosmosdb_key_vsfdatawatch)
database_name = 'scheduled_ingest'
database = client.get_database_client(database_name)
container_name = 'greenbrain'
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    api_payload = {
        'email': greenbrain_username, 
        'password': greenbrain_password
    }
    # print(api_payload)
    login_header = {
        'Content-type': 'application/json'
    }
    # print(login_header)
    try:
        container = database.create_container(id=container_name, partition_key=PartitionKey(path="/date/month"))
    except exceptions.CosmosResourceExistsError:
        container = database.get_container_client(container_name)
    except exceptions.CosmosHttpResponseError:
        raise
    # https://api.greenbrain.net.au/v3/docs
    response = requests.post(greenbrain_endpoint+auth_login, headers=login_header, data=json.dumps(api_payload))
    # print(response.text)
    greenbrain_token = response.json()['token']
    # print(greenbrain_token)
    bearer_token = str('Bearer '+ greenbrain_token)
    # print(bearer_token)
    bootstrap_header = {
        'Authorization': bearer_token
    }
    # print(bootstrap_header)
    bootstrap_response = requests.get(greenbrain_endpoint + bootstrap_uri, headers=bootstrap_header)
    bootstrap_response = json.loads(bootstrap_response.text)
    # print(bootstrap_response)
    # MEA weather station in Ellinbank ['systems'][0] ['stations'][3] has systems timezone set in Australia/Adelaide, got this from bootstrap
    device_timezone = bootstrap_response['systems'][0]['stations'][3]['timezone']
    # print(device_timezone)
    ellinbank_6844_longitude = bootstrap_response['systems'][0]['stations'][3]['longitude']
    # print(ellinbank_6844_longitude)
    ellinbank_6844_latitude = bootstrap_response['systems'][0]['stations'][3]['latitude']
    # print(ellinbank_6844_latitude)
    yesterday_timestamp = pendulum.now(device_timezone).end_of('day').subtract(days=1).in_timezone('UTC').format('YYYY-MM-DDTHH:mm:ss')+'Z' # format for Cosmos DQ query 1970-01-01 00:00:01
    # print(yesterday_timestamp)
    yesterdays_date = pendulum.parse(yesterday_timestamp).format('YYYY-MM-DD') # format for Greenbrain API 1970-01-01
    # print(yesterdays_date)
    def payload_df(df_name, metric_title, sensor_title):
        df_name.rename(columns={'time': 'vendor_timestamp'}, inplace=True)
        df_name['timestamp_utc'] = df_name['vendor_timestamp'].apply(lambda x: pendulum.parse(x, tz=device_timezone).in_timezone('UTC').format('YYYY-MM-DDTHH:mm:ss'))+'Z'
        df_name["vendor"] = "greenbrain"
        df_name['type'] = "climate"
        df_name['metric'] = metric_title
        df_name['sensor'] = sensor_title
        df_name['location'] = "ellinbank_smartfarm"
        df_name['coordinate'] = f'{{"latitude": {ellinbank_6844_latitude}, "longitude": {ellinbank_6844_longitude}}}'
        df_name['coordinate'] = df_name['coordinate'].apply(lambda x: json.loads(x))
        df_name['time'] = df_name["timestamp_utc"].apply(lambda x: pendulum.parse(x).format('[{"sec": ]s [,"min": ] m [,"hour":] H[}]'))
        df_name['time'] = df_name["time"].apply(lambda x: json.loads(x))
        df_name['date'] = df_name["timestamp_utc"].apply(lambda x: pendulum.parse(x).format('[{"day": ]D [,"month": ] M [,"year":] YYYY[}]'))
        df_name['date'] = df_name["date"].apply(lambda x: json.loads(x))
        df_name['id'] = df_name['timestamp_utc'].apply(lambda x: pendulum.parse(x).format('X[_]') + sensor_title)
        for i in range(0,df_name.shape[0]):
            data_dict = dict(df_name.iloc[i,:])
            data_dict = json.dumps(data_dict)
            # print(data_dict)
            container.upsert_item(json.loads(data_dict)) # comment this out to stop upload to Cosmos Db
        logging.info('Ellinbank records inserted successfully into CosmosDB.')
    response=requests.get("{}/sensor-groups/{}/readings?date={}".format(greenbrain_endpoint, 6844, yesterdays_date), headers=bootstrap_header)
    response_6844 = json.loads(response.text)
    # print(response_6844)
    # Minimum temperature sensor reading from 'sensor groups' 6844
    response_46977_min_df = pd.json_normalize(response_6844['sensorTypes']['airTemperature']['sensors']['minimum']['readings'])
    payload_df(response_46977_min_df, 'degree_celsius', '46977airtempmin')
    # Average temperature sensor reading from 'sensor groups' 6844
    response_46978_avg_df = pd.json_normalize(response_6844['sensorTypes']['airTemperature']['sensors']['average']['readings'])
    payload_df(response_46978_avg_df, 'degree_celsius', '46978airtempavg')
    # Maximum temperature sensor reading from 'sensor groups' 6844
    response_46979_max_df = pd.json_normalize(response_6844['sensorTypes']['airTemperature']['sensors']['maximum']['readings'])
    payload_df(response_46979_max_df, 'degree_celsius', '46979airtempmax')
    # Rainfall sensor reading from 'sensor groups' 6844
    response_46991_rainfall_df = pd.json_normalize(response_6844['sensorTypes']['rainfall']['sensors']['rainfall']['readings'])
    payload_df(response_46991_rainfall_df, 'mm', '46991rainfall')
    if mytimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
