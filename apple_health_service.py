import os
import json
from ws_models import sess, engine, OuraSleepDescriptions, \
    AppleHealthQuantityCategory, AppleHealthWorkout
from ws_config import ConfigLocal, ConfigDev, ConfigProd
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
import logging
from logging.handlers import RotatingFileHandler
from sys import argv
import pandas as pd
import requests
from dashboard_objects.dependent_variables_dict import sleep_time
from dashboard_objects.independent_variables_dict import user_correlations
from add_data_to_db.apple_health_quantity_category import test_func_02, \
    make_df_existing_user_apple_quantity_category, add_apple_health_to_database
from add_data_to_db.apple_workouts import make_df_existing_user_apple_workouts, \
    add_apple_workouts_to_database

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


def test_func_01(test_string):
    logger_apple.info(f"- {test_string} -")
    add_to_apple_health_quantity_category_table(logger_apple, test_string)

def db_diagnostics():
    workout_db_all = sess.query(AppleHealthWorkout).all()
    qty_cat_db_all = sess.query(AppleHealthQuantityCategory).all()
    logger_apple.info(f"- AppleHealthWorkout record count: {len(workout_db_all)} -")
    logger_apple.info(f"- AppleHealthQuantityCategory record count: {len(qty_cat_db_all)} -")



def apple_health_qty_cat_json_filename(user_id, timestamp_str):
    return f"{config.APPLE_HEALTH_QUANTITY_CATEGORY_FILENAME_PREFIX}-user_id{user_id}-{timestamp_str}.json"

def apple_health_workouts_json_filename(user_id, timestamp_str):
    return f"{config.APPLE_HEALTH_WORKOUTS_FILENAME_PREFIX}-user_id{user_id}-{timestamp_str}.json"


######################
# Main WSAS function #
# argv[1] = user_id
# argv[2] = apple_json_data_filename
# argv[3] = boolean add data?
# argv[4] = bool make dashboard .json file
# argv[5] = count_of_records_added_to_db
######################
# def what_sticks_health_service(user_id, apple_json_data_filename, add_data_bool, dashboard_bool, 
                                    # count_of_records_added_to_db = 0):
def what_sticks_health_service(user_id, time_stamp_str, add_qty_cat_bool, add_workouts_bool):

    logger_apple.info(f"- accessed What Sticks 10 Apple Service (WSAS) -")
    logger_apple.info(f"- ******************************************* -")
    # logger_apple.info(f"- apple_json_data_filename :::: {apple_json_data_filename} -")

    # add_data_bool = add_data_bool == 'True'
    # dashboard_bool = dashboard_bool == 'True'
    add_qty_cat_bool = add_qty_cat_bool == 'True'
    add_workouts_bool = add_workouts_bool == 'True'
    # create_dashboard_obj_bool = create_dashboard_obj_bool == 'True'
    # create_data_source_obj_bool = create_data_source_obj_bool == 'True'
    
    # filename example: AppleHealthQuantityCategory-user_id1-20231229-1612.json
    apple_health_qty_cat_json_file_name = apple_health_qty_cat_json_filename(user_id, time_stamp_str)
    apple_health_workouts_json_file_name = apple_health_workouts_json_filename(user_id, time_stamp_str)


    # put user data into a dataframe
    # user's existing data in pickle dataframe
    user_apple_health_dataframe_pickle_file_name = f"user_{int(user_id):04}_apple_health_dataframe.pkl"
    user_apple_workouts_dataframe_pickle_file_name = f"user_{int(user_id):04}_apple_workouts_dataframe.pkl"
    # pickle_data_path_and_name = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_health_dataframe_pickle_file_name)
    pickle_apple_qty_cat_path_and_name = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_health_dataframe_pickle_file_name)
    # pickle_apple_workouts_data_path_and_name
    pickle_apple_workouts_path_and_name = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_workouts_dataframe_pickle_file_name)

    # # create apple workouts filename
    # apple_workouts_filename = "AppleWorkouts-" + user_datetimestamp_filename_ending
    # logger_apple.info(f"- apple_workouts_filename :::: {apple_workouts_filename} -")


    # if Existing Apple Health (Quantity or Category Type) df pickle file exists use pickle file instead of searching db
    df_existing_qty_cat = make_df_existing_user_apple_quantity_category(logger_apple,user_id, pickle_apple_qty_cat_path_and_name)
    # if os.path.exists(pickle_apple_qty_cat_path_and_name):
    #     logger_apple.info(f"- reading pickle file: {pickle_apple_qty_cat_path_and_name} -")
    #     # df_existing_user_data=pd.read_pickle(pickle_apple_qty_cat_path_and_name)
    #     df_existing_qty_cat = pd.read_pickle(pickle_apple_qty_cat_path_and_name)
    # else:
    #     logger_apple.info(f"- NO Apple Health (Quantity or Category Type) pickle file found in: {pickle_apple_qty_cat_path_and_name} -")
    #     logger_apple.info(f"- reading from WSDB -")
    #     # df_existing_user_data = get_existing_user_data(user_id)
    #     # df_existing_user_data = make_df_existing_user_apple_quantity_category(logger_apple, user_id)
    #     df_existing_qty_cat = make_df_existing_user_apple_quantity_category(logger_apple, user_id)

    # # if Existing Apple Health WORKOUTS exist
    df_existing_workouts = make_df_existing_user_apple_workouts(logger_apple, user_id,pickle_apple_workouts_path_and_name)
    # if os.path.exists(pickle_apple_workouts_path_and_name):
    #     logger_apple.info(f"- reading pickle file for workouts: {pickle_apple_workouts_path_and_name} -")
    #     # df_existing_user_workouts_data=pd.read_pickle(pickle_apple_workouts_path_and_name)
    #     df_existing_workouts=pd.read_pickle(pickle_apple_workouts_path_and_name)
    # else:
    #     logger_apple.info(f"- NO Apple Health Workouts pickle file found in: {pickle_apple_workouts_path_and_name} -")
    #     logger_apple.info(f"- reading from WSDB -")
    #     # df_existing_user_workouts_data = make_df_existing_user_apple_workouts(logger_apple, user_id)   
    #     df_existing_workouts = make_df_existing_user_apple_workouts(logger_apple, user_id)   
    
    print(f"**** length of existing workouts: {len(df_existing_workouts)}")


    count_of_qty_cat_records_added_to_db = 0
    count_of_workout_records_to_db = 0

    # At this point there still might be no data in WSDB    
    # if add_data_bool:
    if add_qty_cat_bool:
        logger_apple.info(f"- Adding Apple Health Quantity Category Data -")
        # add apple health quantity and category
        # count_of_records_added_to_db = add_apple_health_to_database(logger_apple, user_id, apple_json_data_filename, 
                                            # df_existing_qty_cat, pickle_apple_qty_cat_path_and_name)
        count_of_qty_cat_records_added_to_db = add_apple_health_to_database(logger_apple, config, user_id, apple_health_qty_cat_json_file_name, 
                                            df_existing_qty_cat, pickle_apple_qty_cat_path_and_name)
    if add_workouts_bool:
        logger_apple.info(f"- Adding Apple Health Workouts Data -")
        # add apple health workouts
        # count_of_apple_workout_records_to_db = add_apple_workouts_to_database(logger_apple, user_id,apple_workouts_filename,
        #                                     df_existing_workouts,pickle_apple_workouts_path_and_name)
        count_of_workout_records_to_db = add_apple_workouts_to_database(logger_apple, config, user_id,apple_health_workouts_json_file_name,
                                            df_existing_workouts,pickle_apple_workouts_path_and_name)

    # create data source notify user
    if count_of_qty_cat_records_added_to_db > 0 | count_of_workout_records_to_db > 0:
        create_data_source_object_json_file(user_id)
        create_dashboard_table_object_json_file(user_id)

        # call_api_notify_completion(user_id,count_of_records_added_to_db)



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
