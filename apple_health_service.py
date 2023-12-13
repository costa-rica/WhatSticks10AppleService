import os
import json
from ws_models import sess, AppleHealthKit
from ws_config import ConfigLocal, ConfigDev, ConfigProd
from datetime import datetime
from sqlalchemy.exc import IntegrityError
import logging
from logging.handlers import RotatingFileHandler
from sys import argv

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
    logger_apple.info(f"- accessed add_apple_health_to_database for user_id: {user_id} -")
    print(f"- accessed add_apple_health_to_database for user_id: {user_id} -")
    user_id = int(user_id)
    # ws_data_folder ="/Users/nick/Documents/_testData/_What_Sticks"
    with open(os.path.join(config.APPLE_HEALTH_DIR, apple_json_data_filename), 'r') as file:
        apple_json_data = json.load(file)


    sorted_request_json = sorted(apple_json_data, key=lambda x: parse_date(x.get('startDate')), reverse=True)
    count_of_added_records = 0
    for i in range(0, len(sorted_request_json)):
        # batch = sorted_request_json[i:i + batch_size]
        try:
            if add_entry_to_database(sorted_request_json[i], user_id):
                count_of_added_records += 1
                sess.commit()  # Commit the transaction for the individual entry
            # logger_apple.info(f"- adding i: {count_of_added_records} -")
        except IntegrityError as e:
            sess.rollback()  # Rollback the transaction in case of an IntegrityError
            logger_apple.info(f"IntegrityError encountered in batch: {e}")
            if check_all_bool:
                continue
            else:
                break
    # print(f"- count_of_added_records: {count_of_added_records} -")
    count_of_user_apple_health_records = sess.query(AppleHealthKit).filter_by(user_id=user_id).count()
    logger_apple.info(f"- count of db records: {count_of_user_apple_health_records}")
    logger_apple.info(f"--- add_apple_health_to_database COMPLETE ---")


def add_entry_to_database(entry, user_id):
    new_entry = AppleHealthKit(
                user_id=user_id,
                sampleType=entry.get('sampleType'),
                startDate = entry.get('startDate'),
                endDate = entry.get('endDate'),
                metadataAppleHealth = entry.get('metadata'),
                sourceName = entry.get('sourceName'),
                sourceVersion = entry.get('sourceVersion'),
                sourceProductType = entry.get('sourceProductType'),
                device = entry.get('device'),
                UUID = entry.get('UUID'),
                quantity = entry.get('quantity'),
                value = entry.get('value')
    )
    sess.add(new_entry)
    sess.commit()
    return True

# # Assuming your dates are in a format like '2023-11-11 10:35:46 +0000'
# def parse_date(date_str):
#     return datetime.strptime(date_str.split(' ')[0], '%Y-%m-%d')

def email_user(user_id, message, records_uploaded=0):
    headers = { 'Content-Type': 'application/json'}
    payload = {}
    payload['password'] = config.WSH_API_PASSWORD
    payload['user_id'] = user_id
    payload['message'] = message
    payload['records_uploaded'] = records_uploaded
    r_email = requests.request('GET',config.WSH_API_URL_BASE + '/send_email', headers=headers, 
                                    data=str(json.dumps(payload)))
    
    return r_email.status_code

add_apple_health_to_database(argv[1], argv[2])