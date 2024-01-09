import os
import json
from ws_models import sess, engine, OuraSleepDescriptions, \
    AppleHealthQuantityCategory, AppleHealthWorkout, Users
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from sys import argv
import pandas as pd
import requests

from common.config_and_logger import config, logger_apple
from common.utilities import apple_health_qty_cat_json_filename, \
    apple_health_workouts_json_filename, create_pickle_apple_qty_cat_path_and_name, \
    create_pickle_apple_workouts_path_and_name
from dashboard_objects.data_source_obj import create_data_source_object_json_file
from dashboard_objects.dependent_variables_dict import sleep_time, excercise_time
from dashboard_objects.independent_variables_dict import user_sleep_time_correlations, \
    user_workouts_duration_correlations
from add_data_to_db.apple_health_quantity_category import test_func_02, \
    make_df_existing_user_apple_quantity_category, add_apple_health_to_database
from add_data_to_db.apple_workouts import make_df_existing_user_apple_workouts, \
    add_apple_workouts_to_database

def test_func_01(test_string):
    logger_apple.info(f"- {test_string} -")
    add_to_apple_health_quantity_category_table(logger_apple, test_string)

def db_diagnostics():
    workout_db_all = sess.query(AppleHealthWorkout).all()
    qty_cat_db_all = sess.query(AppleHealthQuantityCategory).all()
    logger_apple.info(f"- AppleHealthWorkout record count: {len(workout_db_all)} -")
    logger_apple.info(f"- AppleHealthQuantityCategory record count: {len(qty_cat_db_all)} -")

def what_sticks_health_service(user_id, time_stamp_str, add_qty_cat_bool, add_workouts_bool):

    logger_apple.info(f"- accessed What Sticks 10 Apple Service (WSAS) -")
    logger_apple.info(f"- ******************************************* -")
    logger_apple.info(f"-- add_qty_cat_bool: {add_qty_cat_bool} -")
    logger_apple.info(f"-- add_workouts_bool: {add_workouts_bool} -")

    add_qty_cat_bool = add_qty_cat_bool == 'True'
    add_workouts_bool = add_workouts_bool == 'True'
    
    # filename json file that has new data
    apple_health_qty_cat_json_file_name = apple_health_qty_cat_json_filename(user_id, time_stamp_str)
    apple_health_workouts_json_file_name = apple_health_workouts_json_filename(user_id, time_stamp_str)

    # create pickle file name
    pickle_apple_qty_cat_path_and_name = create_pickle_apple_qty_cat_path_and_name(user_id)
    pickle_apple_workouts_path_and_name = create_pickle_apple_workouts_path_and_name(user_id)

    # create EXISTING Apple Health dfs
    df_existing_qty_cat = make_df_existing_user_apple_quantity_category(user_id, pickle_apple_qty_cat_path_and_name)
    df_existing_workouts = make_df_existing_user_apple_workouts(user_id,pickle_apple_workouts_path_and_name)

    count_of_qty_cat_records_added_to_db = 0
    count_of_workout_records_to_db = 0

    if add_qty_cat_bool:
        logger_apple.info(f"- Adding Apple Health Quantity Category Data -")
        count_of_qty_cat_records_added_to_db = add_apple_health_to_database(user_id, apple_health_qty_cat_json_file_name, 
                                            df_existing_qty_cat, pickle_apple_qty_cat_path_and_name)
    if add_workouts_bool:
        logger_apple.info(f"- Adding Apple Health Workouts Data -")
        count_of_workout_records_to_db = add_apple_workouts_to_database(user_id,apple_health_workouts_json_file_name,
                                            df_existing_workouts,pickle_apple_workouts_path_and_name)

    logger_apple.info(f"- count_of_qty_cat_records_added_to_db: {count_of_qty_cat_records_added_to_db} -")
    logger_apple.info(f"- count_of_workout_records_to_db: {count_of_workout_records_to_db} -")

    # create data source notify user
    if count_of_qty_cat_records_added_to_db > 0 or count_of_workout_records_to_db > 0:
        logger_apple.info(f"- Should be making datasource json file and dashboard json files -")
        create_data_source_object_json_file( user_id)
        create_dashboard_table_object_json_file(user_id)
        # call_api_notify_completion(user_id,count_of_records_added_to_db)


def create_dashboard_table_object_json_file(user_id):
    logger_apple.info(f"- WSAS creating dashboard file for user: {user_id} -")
    timezone_str = sess.get(Users,int(user_id)).timezone
    array_dashboard_table_object = []

    ############# CREATE sleep_time dashbaord object ############################
    # keys to dashboard_table_object must match WSiOS DashboardTableObject
    dashboard_table_object = sleep_time()

    # keys to indep_var_object must match WSiOS IndepVarObject
    list_of_dictIndepVarObjects = user_sleep_time_correlations(user_id = user_id,timezone_str=timezone_str)# new
    arry_indep_var_objects = []

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
    ############# END CREATE sleep_time dashbaord object ############################

    array_dashboard_table_object.append(dashboard_table_object)


    ############# START CREATE excercise_time dashbaord object ############################
    # keys to dashboard_table_object must match WSiOS DashboardTableObject
    dashboard_table_object = excercise_time()

    # keys to indep_var_object must match WSiOS IndepVarObject
    list_of_dictIndepVarObjects = user_workouts_duration_correlations(user_id,timezone_str)# new
    arry_indep_var_objects = []

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
    ############# END CREATE excercise_time dashbaord object ############################

    array_dashboard_table_object.append(dashboard_table_object)


    # new file name:
    # note: since user_id is string the code below needs convert back to int to use this `:04` shorthand
    # user_sleep_dash_json_file_name = f"dt_sleep01_{int(user_id):04}.json"
    user_data_table_array_json_file_name = f"data_table_objects_array_{int(user_id):04}.json"

    # json_data_path_and_name = os.path.join(config.DASHBOARD_FILES_DIR, user_sleep_dash_json_file_name)
    json_data_path_and_name = os.path.join(config.DASHBOARD_FILES_DIR, user_data_table_array_json_file_name)
    print(f"Writing file name: {json_data_path_and_name}")
    with open(json_data_path_and_name, 'w') as file:
        json.dump(array_dashboard_table_object, file)
    
    logger_apple.info(f"- WSAS COMPLETED dashboard file for user: {user_id} -")
    logger_apple.info(f"- WSAS COMPLETED dashboard file path: {json_data_path_and_name} -")



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


######################
# Main WSAS function #
# argv[1] = user_id
# argv[2] = time stamp string for file name
# argv[3] = add_qty_cat_bool
# argv[4] = add_workouts_bool
######################
if os.environ.get('FLASK_CONFIG_TYPE') != 'local':
    what_sticks_health_service(argv[1], argv[2], argv[3], argv[4])
