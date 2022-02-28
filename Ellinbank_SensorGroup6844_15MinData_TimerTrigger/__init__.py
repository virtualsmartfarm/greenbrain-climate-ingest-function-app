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
try:
    greenbrain_password=os.environ['GREENBRAIN_PASSWORD']
except:
    pass
try:
    cosmosdb_key_vsfdatawatch=os.environ['COSMOSDB_KEY_VSFDATAWATCH']
except:
    pass
try:
    greenbrain_endpoint=os.environ['GREENBRAIN_ENDPOINT']
except:
    pass
auth_login = '/auth/login'
bootstrap_uri = '/bootstrap'
try: 
    cosmosdb_endpoint=os.environ['COSMOSDB_ENDPOINT']
except:
    pass
try:
    adls_avrvsfdatawatch_credentials = os.environ['ADLS_AVRVSFDATAWATCH_CREDENTIALS']
except:
    pass
from azure.storage.filedatalake import DataLakeServiceClient
# function writes the API response to the Azure Data Lake Storage Gen2
def write_response(file_name, requests_response, file_system, storage_folder_name, storage_account_name, adls_credentials):
    try:
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name), credential=adls_credentials)
        file_system_client = service_client.get_file_system_client(file_system=file_system)
        directory_client = file_system_client.get_directory_client(storage_folder_name)
        file_client = directory_client.get_file_client(file_name)
        try:
            file_client.create_file()
            file_client.append_data(requests_response, offset=0, length=len(requests_response))
            file_client.flush_data(len(requests_response))
            logging.info("the file " + file_name + " was created from a Cosmos DB query and uploaded to Azure Data Lake Storage (Gen2)")
        except Exception as e:
            logging.info(e)
            logging.info("the file " + file_name + " already exists or an error occured")
    except Exception as e:
        logging.info(e)
def sensor_query (sensor_name, location_name, record_name, metric):
    record_name = []
    for item in container.query_items(
        query = "SELECT * FROM vsfdatawatch c WHERE c.sensor='{}' AND c.location='{}' ORDER BY c.timestamp_utc DESC".format(sensor_name, location_name), enable_cross_partition_query=True):
        # print(json.dumps(item, indent=True))
        record_name.append(item)
    payload_df=pd.DataFrame(record_name)
    payload_df=payload_df.filter(['timestamp_utc', 'value'], axis=1)
    payload_df=payload_df.rename(columns={"timestamp_utc": "timestamp"})
    payload_csv=payload_df.to_csv(index=False)
    # print(payload_csv)
    if payload_csv != '[]':
        write_response(f'greenbrain-{sensor_name}-{metric}.csv', payload_csv, 'greenbrain', 'curated', 'avrvsfdatawatch', adls_avrvsfdatawatch_credentials) 
    else:
        logging.info('The query to Cosmos DB did not return any data. No data added to the curated folder during the past 24 hours.')
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    api_payload = {
        'email': greenbrain_username, 
        'password': greenbrain_password
    }
    login_header = {
        'Content-type': 'application/json'
    }
    client = CosmosClient(url=cosmosdb_endpoint, credential=cosmosdb_key_vsfdatawatch)
    database_name = 'scheduled_ingest'
    database = client.get_database_client(database_name)
    container_name = 'greenbrain'
    try:
        container = database.create_container(id=container_name, partition_key=PartitionKey(path="/date/month"))
    except exceptions.CosmosResourceExistsError:
        container = database.get_container_client(container_name)
    except exceptions.CosmosHttpResponseError:
        raise
    # https://api.greenbrain.net.au/v3/docs
    response = requests.post(greenbrain_endpoint+auth_login, headers=login_header, data=json.dumps(api_payload))
    greenbrain_token = response.json()['token']
    bearer_token = str('Bearer '+ greenbrain_token)
    bootstrap_header = {
        'Authorization': bearer_token
    }
    bootstrap_response = requests.get(greenbrain_endpoint + bootstrap_uri, headers=bootstrap_header)
    bootstrap_response = json.loads(bootstrap_response.text)
    # MEA weather station in Ellinbank ['systems'][0] ['stations'][3] has systems timezone set in Australia/Adelaide, got this from bootstrap
    device_timezone = bootstrap_response['systems'][0]['stations'][3]['timezone']
    ellinbank_6844_longitude = bootstrap_response['systems'][0]['stations'][3]['longitude']
    ellinbank_6844_latitude = bootstrap_response['systems'][0]['stations'][3]['latitude']
    yesterday_timestamp = pendulum.now(device_timezone).end_of('day').subtract(days=1).in_timezone('UTC').format('YYYY-MM-DDTHH:mm:ss')+'Z' # format for Cosmos DQ query 1970-01-01 00:00:01
    yesterdays_date = pendulum.parse(yesterday_timestamp).format('YYYY-MM-DD') # format for Greenbrain API 1970-01-01
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
            logging.info(data_dict)
            container.upsert_item(json.loads(data_dict)) # comment this out to stop upload to Cosmos Db
        logging.info('Ellinbank records inserted successfully into CosmosDB.')
    response=requests.get("{}/sensor-groups/{}/readings?date={}".format(greenbrain_endpoint, 6844, yesterdays_date), headers=bootstrap_header)
    response_6844 = json.loads(response.text)
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
    # Query Cosmos Db to create a CSV of all records
    sensor_query('46977airtempmin', 'airtempmin46977', 'ellinbank_smartfarm', 'degree')
    sensor_query('46978airtempavg', 'airtempavg46978', 'ellinbank_smartfarm', 'degree')
    sensor_query('46979airtempmax', 'airtempmax46979', 'ellinbank_smartfarm', 'degree')
    sensor_query('46991rainfall', 'rainfall46991', 'ellinbank_smartfarm', 'mm')
    if mytimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
