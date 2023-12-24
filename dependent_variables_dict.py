def sleep_time():
    dashboard_table_object = {}
    dashboard_table_object['dependentVarName']="Sleep Time"
    dashboard_table_object['sourceDataOfDepVar']="Apple Health Data"
    dashboard_table_object['arryIndepVarObjects']=[]
    dashboard_table_object['definition']="The total numbers of hours slept between 3pm to 3pm the next day."
    dashboard_table_object['verb']="sleep"
    return dashboard_table_object
def excercise_time():
    dashboard_table_object = {}
    dashboard_table_object['dependentVarName']="Exercise Time"
    dashboard_table_object['sourceDataOfDepVar']="Apple Health Data"
    dashboard_table_object['arryIndepVarObjects']=[]
    dashboard_table_object['definition']="The total minutes of daily exercise."
    dashboard_table_object['verb']="exercise"
    return dashboard_table_object