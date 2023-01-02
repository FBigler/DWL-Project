import json
import requests
import pandas as pd
import sqlalchemy
import os
import jupyter_notebooks.config_lake_RDS_locations as creds


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

def fetch_location_api_json():
    url = "https://data.geo.admin.ch/ch.bfe.ladestellen-elektromobilitaet/data/ch.bfe.ladestellen-elektromobilitaet_de.json"
    response = requests.get(url)
    data = response.json()
    return data

def convert_into_dataframe(json_object):
    df = pd.json_normalize(json_object,record_path=['features'])
    df = pd.concat([df, df["geometry.coordinates"].apply(pd.Series)], axis=1).drop(columns=["geometry.coordinates"])
    df = df.rename(columns={"id":"ChargingStationId",0:"longitude",1:"latitude"})
    df = df[["ChargingStationId","longitude","latitude"]]
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
    df = convert_into_dataframe(fetch_location_api_json())
    df.to_sql(name=DB_TABLE, con=engine, if_exists="replace", index=False)
    
    return {
        'statusCode': 200,
         'body': json.dumps('Upload completed!')
    }

