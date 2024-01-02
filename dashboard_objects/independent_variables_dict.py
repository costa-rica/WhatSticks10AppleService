import pandas as pd
from ws_analysis import create_user_qty_cat_df, corr_sleep_steps, corr_sleep_heart_rate, \
    create_user_workouts_df, corr_sleep_workouts
from config_and_logger import config, logger_apple

def user_correlations(user_id):
    logger_apple.info("- in user_correlations ")
    df, list_of_user_data = create_user_qty_cat_df(user_id=user_id)
    list_of_arryIndepVarObjects_dict = []
    if 'HKCategoryTypeIdentifierSleepAnalysis' in list_of_user_data:
        arryIndepVarObjects_dict = {}
        # corr_sleep_steps(df)
        arryIndepVarObjects_dict["independentVarName"]= "Step Count"
        arryIndepVarObjects_dict["forDepVarName"]= "Sleep Time"
        correlation_value, obs_count = corr_sleep_steps(df)
        arryIndepVarObjects_dict["correlationValue"]= correlation_value
        arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
        arryIndepVarObjects_dict["definition"]= "The count of your daily steps"
        arryIndepVarObjects_dict["noun"]= "daily step count"
        list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)

    if 'HKQuantityTypeIdentifierHeartRate' in list_of_user_data:
        arryIndepVarObjects_dict = {}
        # corr_sleep_heart_rate(df)
        arryIndepVarObjects_dict["independentVarName"]= "Heart Rate Avg"
        arryIndepVarObjects_dict["forDepVarName"]= "Sleep Time"
        correlation_value, obs_count = corr_sleep_heart_rate(df)
        arryIndepVarObjects_dict["correlationValue"]= correlation_value
        arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
        arryIndepVarObjects_dict["definition"]= "The avearge of heart rates recoreded across all your devices"
        arryIndepVarObjects_dict["noun"]= "daily average heart rate"
        list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)

    logger_apple.info("- in user_correlations ")
    df_workouts, sampleTypeList = create_user_workouts_df(user_id=user_id)# <-- error

    if len(df_workouts) > 5:
        logger_apple.info("- found more than 5 workouts -")
        arryIndepVarObjects_dict = {}
        # corr_sleep_heart_rate(df)
        arryIndepVarObjects_dict["independentVarName"]= "Avg Daily Workout Duration"
        arryIndepVarObjects_dict["forDepVarName"]= "Sleep Time"
        correlation_value, obs_count = corr_sleep_workouts(df)
        arryIndepVarObjects_dict["correlationValue"]= correlation_value
        arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
        arryIndepVarObjects_dict["definition"]= "The avearge of daily duration recorded by all your devices and apps that share with Apple Health"
        arryIndepVarObjects_dict["noun"]= "avearge daily minutes worked out"
        list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)
    
    logger_apple.info("- list_of_arryIndepVarObjects_dict -")
    logger_apple.info(list_of_arryIndepVarObjects_dict)
    logger_apple.info("------------------------------------")

    return list_of_arryIndepVarObjects_dict