from common.config_and_logger import config, logger_apple
import os

def apple_health_qty_cat_json_filename(user_id, timestamp_str):
    return f"{config.APPLE_HEALTH_QUANTITY_CATEGORY_FILENAME_PREFIX}-user_id{user_id}-{timestamp_str}.json"

def apple_health_workouts_json_filename(user_id, timestamp_str):
    return f"{config.APPLE_HEALTH_WORKOUTS_FILENAME_PREFIX}-user_id{user_id}-{timestamp_str}.json"

def create_pickle_apple_qty_cat_path_and_name(user_id_str):
    # user's existing data in pickle dataframe
    user_apple_health_dataframe_pickle_file_name = f"user_{int(user_id_str):04}_apple_health_dataframe.pkl"

    #pickle filename and path
    pickle_apple_qty_cat_path_and_name = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_health_dataframe_pickle_file_name)
    return pickle_apple_qty_cat_path_and_name

def create_pickle_apple_workouts_path_and_name(user_id_str):
    # user's existing data in pickle dataframe
    user_apple_workouts_dataframe_pickle_file_name = f"user_{int(user_id_str):04}_apple_workouts_dataframe.pkl"

    #pickle filename and path
    pickle_apple_workouts_path_and_name = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_workouts_dataframe_pickle_file_name)
    return pickle_apple_workouts_path_and_name