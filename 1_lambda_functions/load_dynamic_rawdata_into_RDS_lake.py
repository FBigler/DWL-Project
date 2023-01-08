import json
import requests
import pandas as pd
import boto3
import psycopg2
import os
import sqlalchemy
from datetime import datetime
import jupyter_notebooks.config_lake_RDS as creds

# set credentials of DB
DB_LOGIN = {
    "db_host": creds.DB_HOST,
    "db_name": creds.DB_NAME,
    "db_port": creds.DB_PORT,
    "db_user": creds.DB_USER,
    "db_pw": creds.DB_PW}

DB_TABLE = creds.DB_TABLE

#DB_LOGIN = {
#    "db_host": os.environ["DB_HOST"],
#    "db_port": os.environ["DB_PORT"],
#    "db_name": os.environ["DB_NAME"],
#    "db_user": os.environ["DB_USER"],
#    "db_pw": os.environ["DB_PW"]
#}
#DB_TABLE = os.environ["DB_TABLE"]

def fetch_dynamic_api_json():
    url = "https://data.geo.admin.ch/ch.bfe.ladestellen-elektromobilitaet/status/oicp/ch.bfe.ladestellen-elektromobilitaet.json"
    response = requests.get(url)
    data = response.json()
    return data

def convert_into_dataframe(json_object):
    df = pd.json_normalize(
        json_object,
        record_path=['EVSEStatuses']
    )
    df = df.explode("EVSEStatusRecord")
    df = pd.concat([df, df["EVSEStatusRecord"].apply(pd.Series)], axis=1).drop(columns=["EVSEStatusRecord"])
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    df["datetime"] = timestamp
    return df

# function to establish db connection (engine)
def create_db_engine(db_host, db_port, db_name, db_user, db_pw):
        engine_url = f"postgresql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_name}"
        engine = sqlalchemy.create_engine(engine_url)
        return engine

def lambda_handler(event, context):
    
    # Connect to DB engine
    engine = create_db_engine(**DB_LOGIN)
    engine.connect()
    
    # fetch data to create df and upload it into RDS
    df = convert_into_dataframe(fetch_dynamic_api_json())
    df.to_sql(name=DB_TABLE, con=engine, if_exists="append", index=False)
    
    return {
        'statusCode': 200,
         'body': json.dumps('Upload completed!')
    }
