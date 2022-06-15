import datetime
import logging
import azure.functions as func
import json
import pendulum
import requests
import os
from azure.cosmos import CosmosClient, PartitionKey, exceptions
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
import numpy as np
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
try: 
    cosmosdb_endpoint=os.environ['COSMOSDB_ENDPOINT']
except:
    pass
try:
    adls_avrvsfdatawatch_key = os.environ['ADLS_AVRVSFDATAWATCH_CREDENTIALS']
except:
    pass
auth_login = '/auth/login'
bootstrap_uri = '/bootstrap'
api_payload = {
    'email': greenbrain_username, 
    'password': greenbrain_password
}
login_header = {
    'Content-type': 'application/json'
}
# Need to keep a sensor name in the array as sensors without min max avg are call the same thing twice
sensors = np.array([
    ['airTemperature', 'minimum', 'airtempmin', 'celsius'],
    ['airTemperature', 'average', 'airtempavg', 'celsius'],
    ['airTemperature', 'maximum', 'airtempmax', 'celsius'],
    ['chillUnits', 'chillUnits', 'chillunits', 'unit'],
    ['daylight', 'daylight', 'daylight', 'hrs'],
    ['degreeDays', 'degreeDays', 'degreeday', 'unit'],
    ['evapotranspiration', 'evapotranspiration', 'evapotranspiration', 'mm'],
    ['frostHours', 'frostHours', 'frosthours', 'hrs'],
    ['rainfall', 'rainfall', 'rainfall', 'mm'],  
    ['relativeHumidity', 'minimum', 'relativehumiditymin', 'percent'],
    ['relativeHumidity', 'average', 'relativehumidityavg', 'percent'],
    ['relativeHumidity', 'maximum', 'relativehumiditymax', 'percent'],
    ['soilTemperature', 'minimum', 'soiltempmin', 'celsius'],
    ['soilTemperature', 'average', 'soiltempavg', 'celsius'],
    ['soilTemperature', 'maximum', 'soiltempmax', 'celsius'],
    ['solarRadiation', 'average', 'solarradiationavg', 'irradiance'],
    ['solarRadiation', 'maximum', 'solarradiationmax', 'irradiance'],
    ['windSpeed', 'minimum', 'windspeedmin', 'kmh'],
    ['windSpeed', 'average', 'windspeedavg', 'kmh'],
    ['windSpeed', 'maximum', 'windspeedmax', 'kmh']
])
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    client = CosmosClient(url=cosmosdb_endpoint, credential=cosmosdb_key_vsfdatawatch)
    database_name = 'scheduled_ingest'
    database = client.get_database_client(database_name)
    container_name = 'greenbrain_apidaily'
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
    # print(bootstrap_response)
    # MEA weather station in Mildura (Mid Area Weather Station 'systems' #2) has systems timezone set in Australia/Adelaide, get this from bootstrap
    device_timezone = bootstrap_response['systems'][2]['stations'][0]['timezone']
    longitude = bootstrap_response['systems'][2]['stations'][0]['longitude']
    latitude = bootstrap_response['systems'][2]['stations'][0]['latitude']
    logging.info(device_timezone)

    # yesterdays_date = pendulum.parse(yesterday_timestamp).format('YYYY-MM-DD') # format for Greenbrain API 1970-01-01
    yesterdays_date = '2022-06-15'
    logging.info(yesterdays_date)

    # yesterday_timestamp = pendulum.now(device_timezone).end_of('day').subtract(days=1).in_timezone('UTC').format('YYYY-MM-DDTHH:mm:ss')+'Z' # format for Cosmos DQ query 1970-01-01 00:00:01
    # logging.info(yesterday_timestamp)
    # yesterday_timestamp = f'{yesterdays_date}T14:29:59Z'

    try:
        response=requests.get("{}/sensor-groups/{}/readings?date={}".format(greenbrain_endpoint, 8714, yesterdays_date), headers=bootstrap_header)
        response_8714 = json.loads(response.text)
    except Exception as e:
        logging.info(e)
    logging.info(response_8714)

    # Query Cosmos Db entries to create a CSV of all records
    def sensor_ingest(df_name, sensor_name, metric_name):
        df_name.rename(columns={'time': 'vendor_timestamp'}, inplace=True)
        df_name['timestamp_utc'] = df_name['vendor_timestamp'].apply(lambda x: pendulum.parse(x, tz=device_timezone).in_timezone('UTC').format('YYYY-MM-DDTHH:mm:ss'))+'Z'
        df_name["vendor"] = "greenbrain"
        df_name['type'] = "climate"
        try:
            sensor_value = str(df_name['value'].iloc[0])
        except Exception as e:
            logging.info(e)
            sensor_value = str(0)
        logging.info(sensor_value)
        df_name['payload']=f'{{"metric": "{metric_name}", "value": {sensor_value}}}'
        df_name['payload']=df_name['payload'].apply(lambda x: json.loads(x))
        df_name['sensor']=sensor_name
        df_name['location']="mildura_smartfarm"
        df_name['coordinate']=f'{{"latitude": {latitude}, "longitude": {longitude}}}'
        df_name['coordinate']=df_name['coordinate'].apply(lambda x: json.loads(x))
        df_name['time'] = df_name["timestamp_utc"].apply(lambda x: pendulum.parse(x).format('[{"sec": ]s [,"min": ] m [,"hour":] H[}]'))
        df_name['time'] = df_name["time"].apply(lambda x: json.loads(x))
        df_name['date'] = df_name["timestamp_utc"].apply(lambda x: pendulum.parse(x).format('[{"day": ]D [,"month": ] M [,"year":] YYYY[}]'))
        df_name['date'] = df_name["date"].apply(lambda x: json.loads(x))
        # df_name['id'] = f'{utc_timestamp}_{sensor_name}' # df_name['id'] can be replaced if this is a know datetime
        df_name['id'] = df_name['timestamp_utc'].apply(lambda x: pendulum.parse(x).format('X[_]') + sensor_name)
        df_name=df_name.drop(['value'], axis=1)
        for i in range(0,df_name.shape[0]):
            data_dict = dict(df_name.iloc[i,:])
            data_dict = json.dumps(data_dict)
            # logging.info(data_dict)
            container.upsert_item(json.loads(data_dict)) # comment this out to stop upload to Cosmos Db
        logging.info('Mildura records inserted successfully into CosmosDB.')

    # End: iterative loop through response
    for i in sensors:
        df = pd.json_normalize(response_8714['sensorTypes'][i[0]]['sensors'][i[1]]['readings'])
        sensor_ingest(df, i[2], i[3])
        logging.info(df)

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
                logging.info("the file " + file_name + " was created from a Cosmos DB query and uploaded to Azure Data Lake Storage Gen2.")
            except Exception as e:
                logging.info(e)
        except Exception as e:
            logging.info(e)
    def sensor_query (sensor_name, location_name, metric):
        captured_records = []
        for item in container.query_items(
            query = "SELECT * FROM vsfdatawatch c WHERE c.sensor='{}' AND c.location='mildura_smartfarm' ORDER BY c.timestamp_utc ASC".format(sensor_name), enable_cross_partition_query=True):
            # logging.info(json.dumps(item, indent=True))
            captured_records.append(item)
        payload_df=pd.json_normalize(captured_records)
        # payload_df=pd.DataFrame(captured_records)
        # logging.info(payload_df)
        payload_df=payload_df.filter(['timestamp_utc', 'payload.value'], axis=1)
        payload_df=payload_df.rename(columns={"timestamp_utc": "timestamp", "payload.value": "value"})
        payload_df=payload_df.drop_duplicates()
        payload_csv=payload_df.to_csv(index=False)
        # logging.info(payload_csv)
        if payload_csv != '[]':
            # comment this out to stop file written to DataLake
            write_response('greenbrain-{}-{}apidaily-{}.csv'.format(location_name.replace("_smartfarm", ""), sensor_name, metric), payload_csv, 'greenbrain', 'curated', 'avrvsfdatawatch', adls_avrvsfdatawatch_key)
            # comment this out to stop file written to DataLake
            logging.info('This ran')
        else:
            logging.info('The Azure Function ran but the Cosmos DB query did not return any records.')
    # Query Cosmos Db and iterative loop to create a series of CSV file containing all records
    for i in sensors:
        sensor_query(i[2], 'mildura_smartfarm', i[3])
    if mytimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
