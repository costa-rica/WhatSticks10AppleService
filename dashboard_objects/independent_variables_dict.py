import pandas as pd
from ws_analysis import create_user_df, corr_sleep_steps, corr_sleep_heart_rate


def user_correlations(user_id):
    print("- in user_correlations ")
    df, list_of_user_data = create_user_df(user_id=user_id)
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
    
    return list_of_arryIndepVarObjects_dict