import datetime
import logging
import azure.functions as func
import pandas as pd
# import json
import pendulum
import os
from azure.storage.filedatalake import DataLakeServiceClient
from azure.cosmos import CosmosClient, PartitionKey, exceptions
try:
    cosmosdb_key_vsfdatawatch=os.environ['COSMOSDB_KEY_VSFDATAWATCH']
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
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    if mytimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
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
                logging.info("the file "+file_name+" was created from a Cosmos DB query and uploaded to Azure Data Lake Storage (Gen2)")
            except:
                logging.info("the file "+file_name+" already exists or an error occured")
        except Exception as e:
            logging.info(e)
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
    def generate_daily_csv (cosmosdb_sensorid, smartfarm_location):
        sensor_records=[]
        for item in container.query_items(
            query="SELECT * FROM vsfdatawatch c WHERE c.sensor='{}' AND c.location='{}' ORDER BY c.datetime ASC".format(cosmosdb_sensorid, smartfarm_location), enable_cross_partition_query=True):
            # logging.info(json.dumps(item, indent=True))
            sensor_records.append(item) 
        sensor_records_df=pd.json_normalize(sensor_records)
        payload_df=sensor_records_df[['timestamp_utc', 'value']].copy()
        try:
            metric = sensor_records_df['metric'][0]
            logging.info(metric)
        except Exception as e:
            metric = 'error'
            logging.info(e)
        payload_df.rename(columns = {'timestamp_utc':'datetime'}, inplace = True)
        payload_df['datetime'] = pd.to_datetime(payload_df['datetime'], format='%Y-%m-%d', utc=True)
        payload_df=payload_df.groupby(pd.Grouper(key='datetime', axis=0, freq='D')).sum()
        payload_df=payload_df.reset_index()
        payload_df=payload_df.sort_values(["datetime"], ascending = (True))
        payload_df['datetime'] = payload_df['datetime'].astype('string')
        payload_df['datetime'] = payload_df['datetime'].apply(lambda x: pendulum.parse(x).set(tz='Australia/Melbourne').end_of('day').in_timezone('UTC').format('YYYY-MM-DDTHH:mm:ss')+'Z')
        payload_df = payload_df.loc[(payload_df['datetime'] >= payload_df['datetime'].iloc[0]) & (payload_df['datetime'] <= pendulum.now('Australia/Melbourne').end_of('month').in_timezone('UTC').format('YYYY-MM-DDTHH:mm:ss'))]
        # logging.info(payload_df)
        payload_csv=payload_df.to_csv(index=False)
        # logging.info(payload_csv)
        if payload_csv != '[]':
            write_response('greenbrain-{}-{}daily-{}.csv'.format(smartfarm_location.replace("_smartfarm", ""), cosmosdb_sensorid, metric), payload_csv, 'greenbrain', 'curated', 'avrvsfdatawatch', adls_avrvsfdatawatch_key)
            logging.info('The write_response ran.')
        else:
            logging.info('The Azure Function ran but the Cosmos DB query did not return any records.')
    generate_daily_csv('airtempmin59213', 'mildura_smartfarm')
    generate_daily_csv('airtempavg59214', 'mildura_smartfarm')
    generate_daily_csv('airtempmax59215', 'mildura_smartfarm')
    generate_daily_csv('rainfall59226', 'mildura_smartfarm')
   