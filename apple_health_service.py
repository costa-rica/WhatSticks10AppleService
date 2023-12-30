import os
import json
from ws_models import sess, engine, OuraSleepDescriptions, AppleHealthQuantityCategory
from ws_config import ConfigLocal, ConfigDev, ConfigProd
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
import logging
from logging.handlers import RotatingFileHandler
from sys import argv
import pandas as pd
import requests
# from ws_analysis import corr_sleep_steps
# from ws_analysis import user_correlations
from dependent_variables_dict import sleep_time
from independent_variables_dict import user_correlations

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

######################
# Main WSAS function #
# argv[1] = user_id
# argv[2] = apple_json_data_filename
# argv[3] = boolean add data?
# argv[4] = bool make dashboard .json file
# argv[5] = count_of_records_added_to_db
######################
def what_sticks_health_service(user_id, apple_json_data_filename, add_data_bool, dashboard_bool, count_of_records_added_to_db = 0):

    logger_apple.info(f"- accessed What Sticks 10 Apple Service (WSAS) -")
    logger_apple.info(f"- ******************************************* -")
    logger_apple.info(f"- apple_json_data_filename :::: {apple_json_data_filename} -")

    add_data_bool = add_data_bool == 'True'
    dashboard_bool = dashboard_bool == 'True'
    
    user_datetimestamp_filename_ending = apple_json_data_filename.split("AppleHealthQuantityCategory-")[0]

    # put user data into a dataframe
    # user's existing data in pickle dataframe
    user_apple_health_dataframe_pickle_file_name = f"user_{int(user_id):04}_apple_health_dataframe.pkl"
    user_apple_workouts_dataframe_pickle_file_name = f"user_{int(user_id):04}_apple_workouts_dataframe.pkl"
    pickle_data_path_and_name = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_health_dataframe_pickle_file_name)
    pickle_apple_workouts_data_path_and_name = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_workouts_dataframe_pickle_file_name)

    # create apple workouts filename
    apple_workouts_filename = "AppleWorkouts-" + user_datetimestamp_filename_ending
    logger_apple.info(f"- apple_workouts_filename :::: {apple_workouts_filename} -")


    # if Existing Apple Health (Quantity or Category Type) df pickle file exists use pickle file instead of searching db
    if os.path.exists(pickle_data_path_and_name):
        logger_apple.info(f"- reading pickle file: {pickle_data_path_and_name} -")
        df_existing_user_data=pd.read_pickle(pickle_data_path_and_name)
    else:
        logger_apple.info(f"- NO Apple Health (Quantity or Category Type) pickle file found in: {pickle_data_path_and_name} -")
        logger_apple.info(f"- reading from WSDB -")
        df_existing_user_data = get_existing_user_data(user_id)

    # if Existing Apple Health WORKOUTS exist
    if os.path.exists(pickle_apple_workouts_data_path_and_name):
        logger_apple.info(f"- reading pickle file for workouts: {pickle_apple_workouts_data_path_and_name} -")
        df_existing_user_workouts_data=pd.read_pickle(pickle_apple_workouts_data_path_and_name)
    else:
        logger_apple.info(f"- NO Apple Health Workouts pickle file found in: {pickle_apple_workouts_data_path_and_name} -")
        logger_apple.info(f"- reading from WSDB -")
        df_existing_user_workouts_data = get_existing_user_apple_workouts_data(user_id)   
    
    # At this point there still might be no data in WSDB    
    if add_data_bool:
        # add apple health quantity and category
        count_of_records_added_to_db = add_apple_health_to_database(user_id, apple_json_data_filename, 
                                            df_existing_user_data, pickle_data_path_and_name)

        # add apple health workouts
        count_of_apple_workout_records_to_db = add_apple_workouts_to_database(user_id,apple_workouts_filename,
                                            df_existing_user_workouts_data,pickle_apple_workouts_data_path_and_name)

    if dashboard_bool:
        create_dashboard_table_object_json_file(user_id)

    # notify user
    if count_of_records_added_to_db > 0:
        call_api_notify_completion(user_id,count_of_records_added_to_db)
        create_data_source_object_json_file(user_id)


def add_apple_workouts_to_database(user_id,apple_workouts_filename,df_existing_user_workouts_data,pickle_apple_workouts_data_path_and_name):
    
    #create new apple_workout df
    with open(os.path.join(config.APPLE_HEALTH_DIR, apple_workouts_filename), 'r') as new_user_data_path_and_filename:
        # apple_json_data = json.load(new_user_data_path_and_filename)
        df_new_user_workout_data = pd.read_json(new_user_data_path_and_filename)


    # check if user workout .pckl file exists
    ## if exists:
    ### create df_existing - via read pickle file
    ### compare existing data with new data and create
    ##### - criteria user_id, sampleType, UUID
    ### get unique records
    ### append unique to existing df
    ### add unique records to database

    ## if no pickle exists:
    ### create pickle file  "user_0001_apple_health_dataframe.pkl"
    df_new_user_workout_data.to_pickle(pickle_apple_workouts_data_path_and_name)

    ### add df to database
    count_of_records_added_to_db = df_new_user_workout_data.to_sql('apple_health_workout', con=engine, if_exists='append', index=False)
    
    count_of_user_apple_health_records = len(df_new_user_workout_data)
    logger_apple.info(f"- count of Apple Health Workout records in db: {count_of_user_apple_health_records}")
    logger_apple.info(f"--- add_apple_workouts_to_database COMPLETE ---")

    
    return count_of_records_added_to_db


def add_apple_health_to_database(user_id, apple_json_data_filename, df_existing_user_data, pickle_data_path_and_name, check_all_bool=False):
    logger_apple.info(f"- accessed add_apple_health_to_database for user_id: {user_id} -")
    user_id = int(user_id)

    # # user's existing data in pickle dataframe
    # user_apple_health_dataframe_pickle_file_name = f"user_{int(user_id):04}_apple_health_dataframe.pkl"
    # pickle_data_path_and_name = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_health_dataframe_pickle_file_name)
    # # if df pickle file exists use pickle file instead of searching db
    # if os.path.exists(pickle_data_path_and_name):
    #     logger_apple.info(f"- reading pickle file: {pickle_data_path_and_name} -")
    #     df_existing_user_data=pd.read_pickle(pickle_data_path_and_name)
    # else:
    #     logger_apple.info(f"- NO pickle file found in: {pickle_data_path_and_name} -")
    #     logger_apple.info(f"- reading from WSDB -")
    #     df_existing_user_data = get_existing_user_data(user_id)

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

    # count_of_records_added_to_db = df_unique_new_user_data.to_sql('apple_health_kit', con=engine, if_exists='append', index=False)
    count_of_records_added_to_db = df_unique_new_user_data.to_sql('apple_health_quantity_category', con=engine, if_exists='append', index=False)

    # Concatenate the DataFrames
    df_updated_user_apple_health = pd.concat([df_existing_user_data, df_unique_new_user_data], ignore_index=True)

    # Save the combined DataFrame as a pickle file
    # note: since user_id is string the code below needs convert back to int to use this `:04` shorthand
    # user_apple_health_dataframe_pickle_file_name = f"user_{int(user_id):04}_apple_health_dataframe.pkl"

    # pickle_data_path_and_name = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_health_dataframe_pickle_file_name)
    logger_apple.info(f"Writing file name: {pickle_data_path_and_name}")
    df_updated_user_apple_health.to_pickle(pickle_data_path_and_name)

    logger_apple.info(f"- count_of_records_added_to_db: {count_of_records_added_to_db} -")
    # count_of_user_apple_health_records = sess.query(AppleHealthQuantityCategory).filter_by(user_id=user_id).count()
    count_of_user_apple_health_records = len(df_updated_user_apple_health)
    logger_apple.info(f"- count of records in db: {count_of_user_apple_health_records}")
    logger_apple.info(f"--- add_apple_health_to_database COMPLETE ---")

    
    return count_of_records_added_to_db
    # # If new records found and added to database create 
    # if count_of_records_added_to_db > 0:
    #     create_dashboard_json_file(user_id)
    # else:
    #     call_api_notify_completion(user_id,count_of_records_added_to_db)

def get_existing_user_data(user_id):
    try:
        # Define the query using a parameterized statement for safety
        query = """
        SELECT * 
        FROM apple_health_quantity_category 
        WHERE user_id = :user_id;
        """
        # Execute the query and create a DataFrame
        df_existing_user_data = pd.read_sql_query(query, engine, params={'user_id': user_id})
        logger_apple.info(f"- successfully created df from WSDB -")
        return df_existing_user_data
    except SQLAlchemyError as e:
        logger_apple.info(f"An error occurred: {e}")
        return None

def get_existing_user_apple_workouts_data(user_id):
    try:
        # Define the query using a parameterized statement for safety
        query = """
        SELECT * 
        FROM apple_health_workout 
        WHERE user_id = :user_id;
        """
        # Execute the query and create a DataFrame
        df_existing_user_apple_workouts_data = pd.read_sql_query(query, engine, params={'user_id': user_id})
        logger_apple.info(f"- successfully created df from WSDB -")
        return df_existing_user_apple_workouts_data
    except SQLAlchemyError as e:
        logger_apple.info(f"An error occurred: {e}")
        return None

def call_api_notify_completion(user_id,count_of_records_added_to_db):
    logger_apple.info(f"- WSAS sending WSAPI call to send email notification to user: {user_id} -")
    headers = { 'Content-Type': 'application/json'}
    payload = {}
    payload['WS_API_PASSWORD'] = config.WS_API_PASSWORD
    payload['user_id'] = user_id
    payload['count_of_records_added_to_db'] = f"{count_of_records_added_to_db:,}"
    r_email = requests.request('POST',config.API_URL + '/apple_health_subprocess_complete', headers=headers, 
                                    data=str(json.dumps(payload)))
    
    return r_email.status_code

def create_dashboard_table_object_json_file(user_id):
    logger_apple.info(f"- WSAS creating dashboard file for user: {user_id} -")
    
    # keys to dashboard_table_object must match WSiOS DashboardTableObject
    dashboard_table_object = sleep_time()

    # # keys to indep_var_object must match WSiOS IndepVarObject
    list_of_dictIndepVarObjects = user_correlations(user_id = user_id)# new
    arry_indep_var_objects = []
    # for arryIndepVarObjects_dict in list_of_arryIndepVarObjects_dict:
    for dictIndepVarObjects in list_of_dictIndepVarObjects:
        if dictIndepVarObjects.get('correlationValue') != "insufficient data":
            logger_apple.info(f"- {dictIndepVarObjects.get('name')} (indep var) correlation with {dictIndepVarObjects.get('depVarName')} (dep var): {dictIndepVarObjects.get('correlationValue')} -")
            arry_indep_var_objects.append(dictIndepVarObjects)

    # Sorting (biggest to smallest) the list by the absolute value of correlationValue
    sorted_arry_indep_var_objects = sorted(arry_indep_var_objects, key=lambda x: abs(x['correlationValue']), reverse=True)

    # Converting correlationValue to string without losing precision
    for item in sorted_arry_indep_var_objects:
        item['correlationValue'] = str(item['correlationValue'])
        item['correlationObservationCount'] = str(item['correlationObservationCount'])

    dashboard_table_object['arryIndepVarObjects'] = sorted_arry_indep_var_objects
    # new file name:
    # note: since user_id is string the code below needs convert back to int to use this `:04` shorthand
    user_sleep_dash_json_file_name = f"dt_sleep01_{int(user_id):04}.json"

    json_data_path_and_name = os.path.join(config.DASHBOARD_FILES_DIR, user_sleep_dash_json_file_name)
    print(f"Writing file name: {json_data_path_and_name}")
    with open(json_data_path_and_name, 'w') as file:
        json.dump(dashboard_table_object, file)
    
    logger_apple.info(f"- WSAS COMPLETED dashboard file for user: {user_id} -")
    logger_apple.info(f"- WSAS COMPLETED dashboard file path: {json_data_path_and_name} -")

# File used in Login
def create_data_source_object_json_file(user_id):
    logger_apple.info(f"- WSAS creating data source object file for user: {user_id} -")

    list_data_source_objects = []

    #get user's oura record count
    # keys to data_source_object_oura must match WSiOS DataSourceObject
    data_source_object_oura={}
    data_source_object_oura['name']="Oura Ring"
    record_count_oura = sess.query(OuraSleepDescriptions).filter_by(user_id=int(user_id)).all()
    data_source_object_oura['recordCount']="{:,}".format(len(record_count_oura))
    list_data_source_objects.append(data_source_object_oura)

    #get user's apple health record count
    # keys to data_source_object_apple_health must match WSiOS DataSourceObject
    data_source_object_apple_health={}
    data_source_object_apple_health['name']="Apple Health Data"
    # record_count_apple_health = sess.query(AppleHealthQuantityCategory).filter_by(user_id=current_user.id).all()
    
    user_apple_health_dataframe_pickle_file_name = f"user_{int(user_id):04}_apple_health_dataframe.pkl"
    pickle_data_path_and_name = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_health_dataframe_pickle_file_name)
    df_apple_health = pd.read_pickle(pickle_data_path_and_name)
    data_source_object_apple_health['recordCount']="{:,}".format(len(df_apple_health))
    list_data_source_objects.append(data_source_object_apple_health)

    # note: since user_id is string the code below needs convert back to int to use this `:04` shorthand
    user_data_source_json_file_name = f"data_source_list_for_user_{int(user_id):04}.json"

    json_data_path_and_name = os.path.join(config.DATA_SOURCE_FILES_DIR, user_data_source_json_file_name)
    logger_apple.info(f"Writing file name: {json_data_path_and_name}")
    with open(json_data_path_and_name, 'w') as file:
        json.dump(list_data_source_objects, file)








if os.environ.get('FLASK_CONFIG_TYPE') != 'local':
    # Adjust the argument handling
    if len(argv) > 5:
        what_sticks_health_service(argv[1], argv[2], argv[3], argv[4], argv[5])
    else:
        what_sticks_health_service(argv[1], argv[2], argv[3], argv[4])
