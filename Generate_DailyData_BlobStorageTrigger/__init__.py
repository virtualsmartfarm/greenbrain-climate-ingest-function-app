import logging
import azure.functions as func
import pendulum
# import json
import pandas as pd
import numpy as np
from azure.storage.filedatalake import DataLakeServiceClient
from azure.cosmos import CosmosClient, PartitionKey, exceptions
import os
try:
    cosmosdb_key_vsfdatawatch=os.environ['COSMOSDB_KEY_VSFDATAWATCH']
except:
    pass
try:
    adls_avrvsfdatawatch_credentials = os.environ['ADLS_AVRVSFDATAWATCH_CREDENTIALS']
except:
    pass


def main(inputBlob: func.InputStream):
    cosmosdb_endpoint='https://vsfdatawatch.documents.azure.com:443/'
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

    def sensor_query (sensor_name, location_name, metric):
        captured_records = []
        for item in container.query_items(
            query = "SELECT * FROM vsfdatawatch c WHERE c.sensor='{}' AND c.location='{}' ORDER BY c.timestamp_utc DESC".format(sensor_name, location_name), enable_cross_partition_query=True):
            # print(json.dumps(item, indent=True))
            captured_records.append(item)
        payload_df=pd.DataFrame(captured_records)
        payload_df=payload_df.filter(['timestamp_utc', 'value'], axis=1)
        payload_df["value"] = payload_df["value"].astype(object)
        payload_df['timestamp_local'] = payload_df['timestamp_utc'].apply(lambda x: pendulum.parse(x, tz='UTC').in_tz('Australia/Melbourne').format('YYYY-MM-DDTHH:mm:ss')+'Z')
        payload_df['timestamp_local'] = pd.to_datetime(payload_df['timestamp_utc'], format='%Y-%m-%dT%H:%M:%SZ')
        payload_df_daily = payload_df.groupby(pd.Grouper(key='timestamp_local', axis=0, freq='D')).max()
        payload_df_daily['timestamp_utc'] = payload_df_daily['timestamp_utc'].apply(lambda x: pendulum.parse(x).in_tz('Australia/Melbourne').end_of('day').in_timezone('UTC').format('YYYY-MM-DDTHH:mm:ss')+'Z')
        payload_df_daily=payload_df_daily.rename(columns={"timestamp_utc": "timestamp"})
        payload_csv=payload_df_daily.to_csv(index=False)
        # print(payload_csv)
        if payload_csv != '[]':
            write_response(f'greenbrain-{sensor_name}daily-{metric}.csv', payload_csv, 'greenbrain', '.', 'avrvsfdatawatch', adls_avrvsfdatawatch_credentials) 
            # logging.info('success write_response')
        else:
            logging.info('The query to Cosmos DB did not return any data. No data added to the curated folder during the past 24 hours.')
    # Query Cosmos Db to create a CSV of all records
    logging.info(f"inputBlob.name: {inputBlob.name}\n")
    # logging.info(f"inputBlob.length: {inputBlob.length}\n")
    # logging.info(f"inputBlob.uri: {inputBlob.uri}\n")

    sensors = np.array([
        ['greenbrain/greenbrain-airtempmax59215-degree.csv',    'airtempmax59215',  'main mildura_smartfarm',   'celsius'],
        ['greenbrain/greenbrain-airtempmin59213-degree.csv',    'airtempmin59213',  'mildura_smartfarm',        'celsius'],
        ['greenbrain/greenbrain-airtempavg59214-degree.csv',    'airtempavg59214',  'mildura_smartfarm',        'celsius'],
        ['greenbrain/greenbrain-rainfall59226-mm.csv',          'rainfall59226',    'mildura_smartfarm',        'mm'],
        ['greenbrain/greenbrain-airtempmin46977-degree.csv',    'airtempmin46977',  'ellinbank_smartfarm',      'degree'],
        ['greenbrain/greenbrain-airtempavg46978-degree.csv',    'airtempavg46978',  'ellinbank_smartfarm',      'degree'],
        ['greenbrain/greenbrain-airtempmax46979-degree.csv',    'airtempmax46979',  'ellinbank_smartfarm',      'degree'],
        ['greenbrain/greenbrain-rainfall46991-mm.csv',          'rainfall46991',    'ellinbank_smartfarm',      'mm'],
        ['greenbrain/greenbrain-airtempmin90893-degree.csv',    'airtempmin90893',  'hamilton_smartfarm',       'celsius'],
        ['greenbrain/greenbrain-airtempavg90894-degree.csv',    'airtempavg90894',  'hamilton_smartfarm',       'celsius'],
        ['greenbrain/greenbrain-airtempmax90895-degree.csv',    'airtempmax90895',  'hamilton_smartfarm',       'celsius'],
        ['greenbrain/greenbrain-rainfall90906-mm.csv',          'rainfall90906',    'hamilton_smartfarm',       'mm']
    ])

    for sensor in range(len(sensors)):
        # print(sensors[sensor, 0])
        # print(sensors[sensor, 1])
        # print(sensors[sensor, 2])
        # print(sensors[sensor, 3])
        if str(f'{inputBlob.name}') == sensors[sensor, 0]:
            # print(sensor_query(sensors[sensor, 1], sensors[sensor, 2], sensors[sensor, 3]))
            print(f"Matched {sensors[sensor, 0]}")
        else:
            pass
            # print(f"No match for {sensors[sensor, 0]}")

#
#    if str(f'{inputBlob.name}') == 'greenbrain/greenbrain-airtempmax59215-degree.csv':
#        logging.info("Matched 'airtempmax59215'")
#        sensor_query('airtempmax59215', 'mildura_smartfarm', 'celsius')
#    elif str(f'{inputBlob.name}') == 'greenbrain/greenbrain-airtempmin59213-degree.csv':
#        logging.info("Matched 'airtempmin59213'")
#        sensor_query('airtempmin59213', 'mildura_smartfarm', 'celsius')
#    elif str(f'{inputBlob.name}') == 'greenbrain/greenbrain-airtempavg59214-degree.csv':
#        logging.info("Matched 'airtempavg59214'")
#        sensor_query('airtempavg59214', 'mildura_smartfarm', 'celsius')
#    elif str(f'{inputBlob.name}') == 'greenbrain/greenbrain-rainfall59226-degree.csv':
#        logging.info("Matched 'rainfall59226'")
#        sensor_query('rainfall59226', 'mildura_smartfarm', 'mm')
#    else:
#        logging.info("No matches")
