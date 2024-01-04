import pandas as pd
from ws_analysis import create_user_qty_cat_df, corr_sleep_steps, corr_sleep_heart_rate, \
    create_user_workouts_df, corr_sleep_workouts
from config_and_logger import config, logger_apple

# def user_correlations(user_id):
def user_sleep_time_correlations(user_id):
    logger_apple.info("- in user_sleep_time_correlations ")
    # df, list_of_user_data = create_user_qty_cat_df(user_id=user_id)
    df_qty_cat, sampleTypeListQtyCat = create_user_qty_cat_df(user_id=user_id)
    list_of_arryIndepVarObjects_dict = []
    if 'HKCategoryTypeIdentifierSleepAnalysis' in sampleTypeListQtyCat:
        arryIndepVarObjects_dict = {}
        # Steps
        if 'HKQuantityTypeIdentifierStepCount' in sampleTypeListQtyCat:
            arryIndepVarObjects_dict["independentVarName"]= "Step Count"
            arryIndepVarObjects_dict["forDepVarName"]= "Sleep Time"
            correlation_value, obs_count = corr_sleep_steps(df_qty_cat)
            arryIndepVarObjects_dict["correlationValue"]= correlation_value
            arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
            arryIndepVarObjects_dict["definition"]= "The count of your daily steps"
            arryIndepVarObjects_dict["noun"]= "daily step count"
            list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)

        # Heart Rate
        if 'HKQuantityTypeIdentifierHeartRate' in sampleTypeListQtyCat:
            arryIndepVarObjects_dict = {}
            # corr_sleep_heart_rate(df)
            arryIndepVarObjects_dict["independentVarName"]= "Heart Rate Avg"
            arryIndepVarObjects_dict["forDepVarName"]= "Sleep Time"
            correlation_value, obs_count = corr_sleep_heart_rate(df_qty_cat)
            arryIndepVarObjects_dict["correlationValue"]= correlation_value
            arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
            arryIndepVarObjects_dict["definition"]= "The avearge of heart rates recoreded across all your devices"
            arryIndepVarObjects_dict["noun"]= "daily average heart rate"
            list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)

        # logger_apple.info("- in user_sleep_time_correlations ")
        df_workouts, sampleTypeListWorkouts = create_user_workouts_df(user_id=user_id)# <-- error

        # Workouts 
        logger_apple.info("- found more than 5 workouts -")
        arryIndepVarObjects_dict = {}
        arryIndepVarObjects_dict["independentVarName"]= "Avg Daily Workout Duration"
        arryIndepVarObjects_dict["forDepVarName"]= "Sleep Time"
        correlation_value, obs_count = corr_sleep_workouts(df_qty_cat, df_workouts)
        arryIndepVarObjects_dict["correlationValue"]= correlation_value
        arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
        arryIndepVarObjects_dict["definition"]= "The avearge of daily duration recorded by all your devices and apps that share with Apple Health"
        arryIndepVarObjects_dict["noun"]= "avearge daily minutes worked out"
        list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)
        logger_apple.info("- list_of_arryIndepVarObjects_dict -")
        logger_apple.info(list_of_arryIndepVarObjects_dict)
        logger_apple.info("------------------------------------")

    return list_of_arryIndepVarObjects_dict

def user_workouts_duration_correlations(user_id):
    logger_apple.info("- in user_workouts_duration_correlations ")
    # df, list_of_user_data = create_user_qty_cat_df(user_id=user_id)
    # df_qty_cat, sampleTypeListQtyCat = create_user_qty_cat_df(user_id=user_id)
    # list_of_arryIndepVarObjects_dict = []
    # if 'HKCategoryTypeIdentifierSleepAnalysis' in sampleTypeListQtyCat:
    #     arryIndepVarObjects_dict = {}
    #     # Steps
    #     if 'HKQuantityTypeIdentifierStepCount' in sampleTypeListQtyCat:
    #         arryIndepVarObjects_dict["independentVarName"]= "Step Count"
    #         arryIndepVarObjects_dict["forDepVarName"]= "Sleep Time"
    #         correlation_value, obs_count = corr_sleep_steps(df_qty_cat)
    #         arryIndepVarObjects_dict["correlationValue"]= correlation_value
    #         arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
    #         arryIndepVarObjects_dict["definition"]= "The count of your daily steps"
    #         arryIndepVarObjects_dict["noun"]= "daily step count"
    #         list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)
