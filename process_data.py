# this script process the data from all the json files
# After processing it stores final data as a json file metric_table.json to be read by app1.py

import pandas as pd
import json

#create dataframe to load the models
scam_detection_model_result = pd.read_json(r'1module.json', orient='index')
honeypotIs_result_day_1 = pd.read_json(r'3honeypotB.json', orient='index')
database_check_result = pd.read_json(r'2scamdata.json', orient='index')

## preprocessing for scam detection model dataset
#create lables for scam detection model
scam_detection_model_result['scam'] = scam_detection_model_result['predections'].apply(
    lambda x: (float(x['scamProbability'][:-1]) > 50)
)
#extract date from timestamp
scam_detection_model_result['date'] = pd.to_datetime(scam_detection_model_result['timeRecorded'], unit='ms').dt.date
#reset index
scam_detection_model_result = scam_detection_model_result.rename_axis('token address').reset_index()

## preprocessing for honeypotIs_result_day_1 dataset
honeypotIs_result_day_1['honey_keys'] = honeypotIs_result_day_1['honeyDetails'].apply(lambda d: list(d.keys()))

#function to create label
def create_label(row):
  if row['honey_keys'] == []:
    return 0
  if row['honeyDetails']['IsHoneypot'] == True:
    return 1
  else:
    if row['honeyDetails']['Flags'] == None and row['honeyDetails']['Error'] == None:
      return 0
    else:
      return 1

# create labels
honeypotIs_result_day_1['scam'] = honeypotIs_result_day_1.apply(create_label, axis=1)
#extract date from timestamp
honeypotIs_result_day_1['date'] = pd.to_datetime(honeypotIs_result_day_1['timeRecorded'], unit='ms').dt.date
#resetindex
honeypotIs_result_day_1 = honeypotIs_result_day_1.rename_axis('token address').reset_index()

## preprocessing for database_check_result dataset
def check_scam(details):
  is_scam = list(details.keys())[0]
  return is_scam == 'true'

#create label
database_check_result['scam'] = database_check_result['isScam'].apply(check_scam)
#create date
database_check_result['date'] = pd.to_datetime(database_check_result['timeRecorded'], unit='ms').dt.date
#reset index
database_check_result = database_check_result.rename_axis('token address').reset_index()

#drop duplicates from all tables using token address column
scam_detection_model_result = scam_detection_model_result.drop_duplicates(subset=['token address'])
honeypotIs_result_day_1 = honeypotIs_result_day_1.drop_duplicates(subset=['token address'])
database_check_result = database_check_result.drop_duplicates(subset=['token address'])

#merge datasets
relevant_cols = ['token address', 'scam']
#merge dataframes
# merge ai and database model
ai_database = scam_detection_model_result[['token address', 'scam', 'date']].merge(
    database_check_result[relevant_cols], on=['token address'], how='inner', suffixes=('_bot', '_database')
    )
#merge with honeypot data
ai_database_honeypot = ai_database.merge(
    honeypotIs_result_day_1[relevant_cols], on=['token address'], how='inner', suffixes=('', '_honeypotIs')
    )
ai_database_honeypot.rename(columns={'scam':'scam_honeypotIs'}, inplace=True)

# label scam if both database and bot detects scam
def scam_bot_database_both(row):
  if row['scam_bot'] == True and row['scam_database'] == True:
    return 1
  else:
    return 0

ai_database_honeypot['scam_bot'] = ai_database_honeypot.apply(scam_bot_database_both, axis=1)
#calculate metrics for confusion matrix
TP = ai_database_honeypot[(ai_database_honeypot['scam_honeypotIs'] == 1) & (ai_database_honeypot['scam_bot'] == True)]
TN = ai_database_honeypot[(ai_database_honeypot['scam_honeypotIs'] == 0) & (ai_database_honeypot['scam_bot'] == False)]
FP = ai_database_honeypot[(ai_database_honeypot['scam_honeypotIs'] == 0) & (ai_database_honeypot['scam_bot'] == True)]
FN = ai_database_honeypot[(ai_database_honeypot['scam_honeypotIs'] == 1) & (ai_database_honeypot['scam_bot'] == False)]

# create pivot table to check count of rows for each date and set aggregate column name to table name
TP_pivot = pd.pivot_table(TP, values='scam_honeypotIs', index=['date'], aggfunc='count').reset_index()
TP_pivot.rename(columns={'scam_honeypotIs':'TP'}, inplace=True)
TN_pivot = pd.pivot_table(TN, values='scam_honeypotIs', index=['date'], aggfunc='count').reset_index()
TN_pivot.rename(columns={'scam_honeypotIs':'TN'}, inplace=True)
FP_pivot = pd.pivot_table(FP, values='scam_honeypotIs', index=['date'], aggfunc='count').reset_index()
FP_pivot.rename(columns={'scam_honeypotIs':'FP'}, inplace=True)
FN_pivot = pd.pivot_table(FN, values='scam_honeypotIs', index=['date'], aggfunc='count').reset_index()
FN_pivot.rename(columns={'scam_honeypotIs':'FN'}, inplace=True)

pivot_table = TP_pivot.merge(TN_pivot, on='date', how='left').merge(FP_pivot, on='date', how='left').merge(FN_pivot, on='date', how='left')
#convert date field to date format yyyy-mm-dd
pivot_table['date'] = pd.to_datetime(pivot_table['date']).dt.strftime('%Y-%m-%d')
# transfer table to json format
pivot_table.to_json('metric_table.json', orient='index', date_format='yyyy-mm-dd')
# print(database_check_result.head())