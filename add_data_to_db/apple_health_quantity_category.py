from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from ws_models import engine
import os
from datetime import datetime
import numpy as np
from common.config_and_logger import config, logger_apple

# def test_func_02(test_string):
#     logger_apple.info(f"- inside apple_health_quantity_category.add_to_apple_health_quantity_category_table -")
#     logger_apple.info(f"- {test_string} -")


def make_df_existing_user_apple_quantity_category(user_id,pickle_apple_qty_cat_path_and_name):

    if os.path.exists(pickle_apple_qty_cat_path_and_name):
        logger_apple.info(f"- reading pickle file: {pickle_apple_qty_cat_path_and_name} -")
        df_existing_qty_cat = pd.read_pickle(pickle_apple_qty_cat_path_and_name)
        return df_existing_qty_cat
    else:
        logger_apple.info(f"- NO Apple Health (Quantity or Category Type) pickle file found in: {pickle_apple_qty_cat_path_and_name} -")
        logger_apple.info(f"- reading Apple Health (Quantity and Category Type) from WSDB -")

        try:
            # Define the query using a parameterized statement for safety
            query = """
            SELECT * 
            FROM apple_health_quantity_category 
            WHERE user_id = :user_id;
            """
            # Execute the query and create a DataFrame
            df_existing_qty_cat = pd.read_sql_query(query, engine, params={'user_id': user_id})
            logger_apple.info(f"- successfully created df from WSDB -")
            logger_apple.info(f"- Successfully created Apple Quantity and Category df from WSDB -")
            return df_existing_qty_cat
        except SQLAlchemyError as e:
            logger_apple.info(f"An error occurred: {e}")
            return None


def add_apple_health_to_database(user_id, apple_json_data_filename, df_existing_user_data, pickle_data_path_and_name):
    logger_apple.info(f"- accessed add_apple_health_to_database for user_id: {user_id} -")
    user_id = int(user_id)

    # ws_data_folder ="/Users/nick/Documents/_testData/_What_Sticks"
    with open(os.path.join(config.APPLE_HEALTH_DIR, apple_json_data_filename), 'r') as new_user_data_path_and_filename:
        df_new_user_data = pd.read_json(new_user_data_path_and_filename)

    # Convert the 'value' column in both dataframes to string
    try:
        # Try converting the 'value' column to string
        df_new_user_data['value'] = df_new_user_data['value'].astype(str)
    except KeyError:
        # If 'value' column does not exist, create it and fill with a placeholder string
        df_new_user_data['value'] = np.nan

    try:
        # Try converting the 'value' column to string
        df_new_user_data['quantity'] = df_new_user_data['quantity'].astype(str)
    except KeyError:
        # If 'value' column does not exist, create it and fill with a placeholder string
        df_new_user_data['value'] = np.nan

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
    logger_apple.info(f"Writing file name: {pickle_data_path_and_name}")
    df_updated_user_apple_health.to_pickle(pickle_data_path_and_name)

    logger_apple.info(f"- count_of_records_added_to_db: {count_of_records_added_to_db} -")
    count_of_user_apple_health_records = len(df_updated_user_apple_health)
    logger_apple.info(f"- count of records in db: {count_of_user_apple_health_records}")
    logger_apple.info(f"- add_apple_health_to_database COMPLETE -")

    
    return count_of_records_added_to_db


