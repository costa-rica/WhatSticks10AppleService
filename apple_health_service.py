import os
import json
from ws_models import sess, engine, AppleHealthKit
from ws_config import ConfigLocal, ConfigDev, ConfigProd
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
import logging
from logging.handlers import RotatingFileHandler
from sys import argv
import pandas as pd
import requests

match os.environ.get('FLASK_CONFIG_TYPE'):
    case 'dev':
        config = ConfigDev()
        print('- WhatSticks10AppleService/config: Development')
    case 'prod':
        config = ConfigProd()
        print('- WhatSticks10AppleService/config: Production')
    case _:
        config = ConfigLocal()
        print('- WhatSticks10AppleService/config: Local')

#Setting up Logger
formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
formatter_terminal = logging.Formatter('%(asctime)s:%(filename)s:%(name)s:%(message)s')

#initialize a logger
logger_apple = logging.getLogger(__name__)
logger_apple.setLevel(logging.DEBUG)

#where do we store logging information
file_handler = RotatingFileHandler(os.path.join(config.APPLE_SERVICE_ROOT,'apple_service.log'), mode='a', maxBytes=5*1024*1024,backupCount=2)
file_handler.setFormatter(formatter)

#where the stream_handler will print
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter_terminal)

logger_apple.addHandler(file_handler)
logger_apple.addHandler(stream_handler)

def add_apple_health_to_database(user_id, apple_json_data_filename, check_all_bool=False):
    logger_apple.info(f"- accessed What Sticks 10 Apple Service (WSAS) -")
    logger_apple.info(f"- accessed add_apple_health_to_database for user_id: {user_id} -")
    user_id = int(user_id)

    df_existing_user_data = get_existing_user_data(user_id)

    logger_apple.info(f"- df_existing_user_data count : {len(df_existing_user_data)} -")
    logger_apple.info(f"- {df_existing_user_data.head(1)} -")
    logger_apple.info(f"- ------------------- -")

    # ws_data_folder ="/Users/nick/Documents/_testData/_What_Sticks"
    with open(os.path.join(config.APPLE_HEALTH_DIR, apple_json_data_filename), 'r') as new_user_data_path_and_filename:
        # apple_json_data = json.load(new_user_data_path_and_filename)
        df_new_user_data = pd.read_json(new_user_data_path_and_filename)

    logger_apple.info(f"- df_new_user_data count : {len(df_new_user_data)} -")
    logger_apple.info(f"- {df_new_user_data.head(1)} -")
    logger_apple.info(f"- ------------------- -")

    # Convert the 'value' column in both dataframes to string
    df_new_user_data['value'] = df_new_user_data['value'].astype(str)
    df_new_user_data['quantity'] = df_new_user_data['quantity'].astype(str)
    # Perform the merge on specific columns
    df_merged = pd.merge(df_new_user_data, df_existing_user_data, 
                        on=['sampleType', 'startDate', 'endDate', 'UUID'], 
                        how='outer', indicator=True)
    # Filter out the rows that are only in df_new_user_data
    df_unique_new_user_data = df_merged[df_merged['_merge'] == 'left_only']
    # Drop columns ending with '_y'
    df_unique_new_user_data = df_unique_new_user_data[df_unique_new_user_data.columns.drop(list(df_unique_new_user_data.filter(regex='_y')))]
    # Filter out the rows that are duplicates (in both dataframes)
    df_duplicates = df_merged[df_merged['_merge'] == 'both']
    # Drop the merge indicator column from both dataframes
    df_unique_new_user_data = df_unique_new_user_data.drop(columns=['_merge'])
    df_duplicates = df_duplicates.drop(columns=['_merge'])
    df_unique_new_user_data['user_id'] = user_id
    # Convert 'user_id' from float to integer and then to string
    df_unique_new_user_data['user_id'] = df_unique_new_user_data['user_id'].astype(int)
    # Drop the 'metadataAppleHealth' and 'time_stamp_utc' columns
    df_unique_new_user_data = df_unique_new_user_data.drop(columns=['metadataAppleHealth'])
    # Fill missing values in 'time_stamp_utc' with the current UTC datetime
    default_date = datetime.utcnow()
    df_unique_new_user_data['time_stamp_utc'] = df_unique_new_user_data['time_stamp_utc'].fillna(default_date)

    rename_dict = {}
    rename_dict['metadata']='metadataAppleHealth'
    rename_dict['sourceName_x']='sourceName'
    rename_dict['value_x']='value'
    rename_dict['device_x']='device'
    rename_dict['sourceProductType_x']='sourceProductType'
    rename_dict['sourceVersion_x']='sourceVersion'
    rename_dict['quantity_x']='quantity'
    df_unique_new_user_data.rename(columns=rename_dict, inplace=True)

    count_of_records_added_to_db = df_unique_new_user_data.to_sql('apple_health_kit', con=engine, if_exists='append', index=False)

    logger_apple.info(f"- count_of_records_added_to_db: {count_of_records_added_to_db} -")
    count_of_user_apple_health_records = sess.query(AppleHealthKit).filter_by(user_id=user_id).count()
    logger_apple.info(f"- count of records in db: {count_of_user_apple_health_records}")
    logger_apple.info(f"--- add_apple_health_to_database COMPLETE ---")
    
    call_api_notify_completion(user_id,count_of_records_added_to_db)


def get_existing_user_data(user_id):
    try:
        # Define the query using a parameterized statement for safety
        query = """
        SELECT * 
        FROM apple_health_kit 
        WHERE user_id = :user_id;
        """
        # Execute the query and create a DataFrame
        df_existing_user_data = pd.read_sql_query(query, engine, params={'user_id': user_id})
        return df_existing_user_data
    except SQLAlchemyError as e:
        logger_apple.info(f"An error occurred: {e}")
        return None



def call_api_notify_completion(user_id,count_of_records_added_to_db):
    headers = { 'Content-Type': 'application/json'}
    payload = {}
    payload['WS_API_PASSWORD'] = config.WS_API_PASSWORD
    payload['user_id'] = user_id
    payload['count_of_records_added_to_db'] = f"{count_of_records_added_to_db:,}"
    r_email = requests.request('POST',config.API_URL + '/apple_health_subprocess_complete', headers=headers, 
                                    data=str(json.dumps(payload)))
    
    return r_email.status_code

add_apple_health_to_database(argv[1], argv[2])