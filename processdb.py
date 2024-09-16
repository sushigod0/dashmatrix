# processdb
import pandas as pd
import json
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
import os


load_dotenv()

# MongoDB connection string
uri = os.getenv("MONGODB_URI")

# Connect to MongoDB
client = MongoClient(uri)
db = client["DB1"]

# Function to convert MongoDB cursor to DataFrame
def cursor_to_dataframe(cursor):
    
    return pd.DataFrame(list(cursor))

# Fetch data from MongoDB collections
scam_detection_model_result = cursor_to_dataframe(db.module.find())
honeypotIs_result_day_1 = cursor_to_dataframe(db.honeypot.find())
database_check_result = cursor_to_dataframe(db.database.find())

# Close MongoDB connection
client.close()

## preprocessing for scam detection model dataset
scam_detection_model_result['scam'] = scam_detection_model_result['predictions'].apply(
    lambda x: (float(x['scamProbability'][:-1]) > 50)
)
scam_detection_model_result['date'] = pd.to_datetime(scam_detection_model_result['timeRecorded'], unit='ms').dt.date
scam_detection_model_result = scam_detection_model_result.reset_index().rename(columns={'index': 'token address'})

## preprocessing for honeypotIs_result_day_1 dataset
honeypotIs_result_day_1['honey_keys'] = honeypotIs_result_day_1['honeyDetails'].apply(lambda d: list(d.keys()))

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

honeypotIs_result_day_1['scam'] = honeypotIs_result_day_1.apply(create_label, axis=1)
honeypotIs_result_day_1['date'] = pd.to_datetime(honeypotIs_result_day_1['timeRecorded'], unit='ms').dt.date
honeypotIs_result_day_1 = honeypotIs_result_day_1.reset_index().rename(columns={'index': 'token address'})

## preprocessing for database_check_result dataset
def check_scam(details):
    is_scam = list(details.keys())[0]
    return is_scam == 'true'

database_check_result['scam'] = database_check_result['isScam'].apply(check_scam)
database_check_result['date'] = pd.to_datetime(database_check_result['timeRecorded'], unit='ms').dt.date
database_check_result = database_check_result.reset_index().rename(columns={'index': 'token address'})

# Drop duplicates from all tables using token address column
scam_detection_model_result = scam_detection_model_result.drop_duplicates(subset=['token address'])
honeypotIs_result_day_1 = honeypotIs_result_day_1.drop_duplicates(subset=['token address'])
database_check_result = database_check_result.drop_duplicates(subset=['token address'])

# Merge datasets
relevant_cols = ['token address', 'scam']
ai_database = scam_detection_model_result[['token address', 'scam', 'date']].merge(
    database_check_result[relevant_cols], on=['token address'], how='inner', suffixes=('_bot', '_database')
)
ai_database_honeypot = ai_database.merge(
    honeypotIs_result_day_1[relevant_cols], on=['token address'], how='inner', suffixes=('', '_honeypotIs')
)
ai_database_honeypot.rename(columns={'scam':'scam_honeypotIs'}, inplace=True)

def scam_bot_database_both(row):
    if row['scam_bot'] == True and row['scam_database'] == True:
        return 1
    else:
        return 0

ai_database_honeypot['scam_bot'] = ai_database_honeypot.apply(scam_bot_database_both, axis=1)

# Calculate metrics for confusion matrix
TP = ai_database_honeypot[(ai_database_honeypot['scam_honeypotIs'] == 1) & (ai_database_honeypot['scam_bot'] == True)]
TN = ai_database_honeypot[(ai_database_honeypot['scam_honeypotIs'] == 0) & (ai_database_honeypot['scam_bot'] == False)]
FP = ai_database_honeypot[(ai_database_honeypot['scam_honeypotIs'] == 0) & (ai_database_honeypot['scam_bot'] == True)]
FN = ai_database_honeypot[(ai_database_honeypot['scam_honeypotIs'] == 1) & (ai_database_honeypot['scam_bot'] == False)]

# Create pivot tables
TP_pivot = pd.pivot_table(TP, values='scam_honeypotIs', index=['date'], aggfunc='count').reset_index()
TP_pivot.rename(columns={'scam_honeypotIs':'TP'}, inplace=True)
TN_pivot = pd.pivot_table(TN, values='scam_honeypotIs', index=['date'], aggfunc='count').reset_index()
TN_pivot.rename(columns={'scam_honeypotIs':'TN'}, inplace=True)
FP_pivot = pd.pivot_table(FP, values='scam_honeypotIs', index=['date'], aggfunc='count').reset_index()
FP_pivot.rename(columns={'scam_honeypotIs':'FP'}, inplace=True)
FN_pivot = pd.pivot_table(FN, values='scam_honeypotIs', index=['date'], aggfunc='count').reset_index()
FN_pivot.rename(columns={'scam_honeypotIs':'FN'}, inplace=True)

pivot_table = TP_pivot.merge(TN_pivot, on='date', how='left').merge(FP_pivot, on='date', how='left').merge(FN_pivot, on='date', how='left')
pivot_table['date'] = pd.to_datetime(pivot_table['date']).dt.strftime('%Y-%m-%d')

# Save to JSON
pivot_table.to_json('metric_table.json', orient='index', date_format='yyyy-mm-dd')