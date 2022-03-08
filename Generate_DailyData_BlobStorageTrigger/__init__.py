import logging
import azure.functions as func
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
from io import BytesIO
import pendulum
import os
import datetime

try:
    adls_avrvsfdatawatch_credentials = os.environ['ADLS_AVRVSFDATAWATCH_CREDENTIALS']
except:
    pass

from azure.storage.filedatalake import DataLakeServiceClient
# FUNCTION WRITES THE API RESPONSE TO AZURE DATA LAKE STORAGE (GEN2)
def write_response(file_name, payload, file_system, storage_folder_name, storage_account_name, adls_credentials):
    try:
        service_client=DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https",storage_account_name),credential=adls_credentials)
        file_system_client=service_client.get_file_system_client(file_system=file_system)
        directory_client=file_system_client.get_directory_client(storage_folder_name)
        file_client=directory_client.get_file_client(file_name)
        try:
            file_client.create_file()
            file_client.append_data(payload,offset=0,length=len(payload))
            file_client.flush_data(len(payload))
            logging.info("the file '"+file_name+"' was created from a local query and uploaded to Azure Data Lake Storage Gen2")
            # print(f"the file '{file_name}' was created from a local query and uploaded to Azure Data Lake Storage Gen2")
        except Exception as e:
            logging.info(e)
            # print(e)
    except Exception as e:
        logging.info(e)
        # print(e)

def main(inputBlob: func.InputStream):
    sensor_name = (f'{inputBlob.name}'.split("-")[1])
    logging.info(sensor_name)
    sensor_metric = (f'{inputBlob.name}'.split("-")[2])
    sensor_metric = (sensor_metric.split(".")[0])
    logging.info(sensor_metric)
    daily_df = pd.DataFrame()
    if sensor_metric == 'celsius' or sensor_metric == 'degree':
        logging.info('celsius or degree')
        payload_df = pd.read_csv(BytesIO(inputBlob.read()))
        # 1975-05-21T22:00:00+00:00
        payload_df['timestamp_local']=payload_df['timestamp'].apply(lambda x: pendulum.parse(x).set(tz='UTC').in_tz('Australia/Melbourne').format('YYYY-MM-DDTHH:mm:ssZ'))
        payload_df['timestamp_local']=pd.to_datetime(payload_df['timestamp_local'], format='%Y-%m-%d')
        daily_df = payload_df.groupby(pd.Grouper(key='timestamp_local', axis=0, freq='D')).max()
        # logging.info(daily_df)
        daily_df=daily_df.reset_index()
        daily_df=daily_df.sort_values(["timestamp_local"], ascending = (False))
        daily_df['timestamp_local']=daily_df['timestamp_local'].astype('string')
        daily_df['timestamp_local']=daily_df['timestamp_local'].apply(lambda x: pendulum.parse(x).end_of('day').in_timezone('UTC').format('YYYY-MM-DDTHH:mm:ss')+'Z')
        daily_df = daily_df.iloc[1: , :]
        logging.info(daily_df)
        daily_csv=daily_df.to_csv(index=False)
        # logging.info(daily_csv)
        if daily_csv != '[]':
            write_response('greenbrain-{}-{}.csv'.format(sensor_name, sensor_metric), daily_csv, 'greenbrain', 'tmp', 'avrvsfdatawatch', adls_avrvsfdatawatch_credentials)
        else:
            logging.inf('The CSV query did not return any data. No data added to the Greenbain container for the the past 24 hours.')

    elif sensor_metric == 'mm':
        payload_df = pd.read_csv(BytesIO(inputBlob.read()))
        # 1975-05-21T22:00:00+00:00
        payload_df['timestamp_local']=payload_df['timestamp'].apply(lambda x: pendulum.parse(x).set(tz='UTC').in_tz('Australia/Melbourne').format('YYYY-MM-DDTHH:mm:ssZ'))
        payload_df['timestamp_local']=pd.to_datetime(payload_df['timestamp_local'], format='%Y-%m-%d')
        daily_df = payload_df.groupby(pd.Grouper(key='timestamp_local', axis=0, freq='D')).sum()
        # logging.info(daily_df)
        daily_df=daily_df.reset_index()
        daily_df=daily_df.sort_values(["timestamp_local"], ascending = (False))
        daily_df['timestamp_local']=daily_df['timestamp_local'].astype('string')
        daily_df['timestamp_local']=daily_df['timestamp_local'].apply(lambda x: pendulum.parse(x).end_of('day').in_timezone('UTC').format('YYYY-MM-DDTHH:mm:ss')+'Z')
        daily_df = daily_df.iloc[1: , :]
        logging.info(daily_df)
        daily_csv=daily_df.to_csv(index=False)
        # logging.info(daily_csv)
        if daily_csv != '[]':
            write_response('greenbrain-{}-{}.csv'.format(sensor_name, sensor_metric), daily_csv, 'greenbrain', 'tmp', 'avrvsfdatawatch', adls_avrvsfdatawatch_credentials)
        else:
            logging.inf('The CSV query did not return any data. No data added to the Greenbain container for the the past 24 hours.')
    else:
        pass
        logging.info('fail')

