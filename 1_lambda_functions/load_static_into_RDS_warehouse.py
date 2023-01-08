import json
import requests
import pandas as pd
import sqlalchemy
import os

DB_LOGIN_lake = {
    "db_host": os.environ["DB_HOST_l"],
    "db_port": os.environ["DB_PORT_l"],
    "db_name": os.environ["DB_NAME_l"],
    "db_user": os.environ["DB_USER_l"],
    "db_pw": os.environ["DB_PW_l"]
}
DB_TABLE_lake = os.environ["DB_TABLE_L"]

DB_LOGIN_warehouse = {
    "db_host": os.environ["DB_HOST_w"],
    "db_port": os.environ["DB_PORT_w"],
    "db_name": os.environ["DB_NAME_w"],
    "db_user": os.environ["DB_USER_w"],
    "db_pw": os.environ["DB_PW_w"]
}
DB_TABLE_warehouse = os.environ["DB_TABLE_w"]

# function to establish db connection to lake (engine)
def create_db_engine_lake(db_host, db_port, db_name, db_user, db_pw):
        engine_url = f"postgresql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_name}"
        engine = sqlalchemy.create_engine(engine_url)
        return engine

def sql_query():

        query_all = """SELECT * FROM chargingstations_static"""
        return query_all

def sql_query_2():

        query_all = """SELECT * FROM cpostal_codes"""
        return query_all


# function to establish db connection (engine)
def create_db_engine(db_host, db_port, db_name, db_user, db_pw):
        engine_url = f"postgresql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_name}"
        engine = sqlalchemy.create_engine(engine_url)
        return engine

def lambda_handler(event, context):
    
    # Connect to DB engine lake
    engine_lake = create_db_engine(**DB_LOGIN_lake)
    engine_lake.connect()

     # Connect to DB engine warehouse
    engine_warehouse = create_db_engine(**DB_LOGIN_warehouse)
    engine_warehouse.connect()
    
    # Read data from lake into dataframe an rename columns
    df_static = pd.read_sql(sql_query(),con=engine_lake)
    df_static = df_static.rename(columns={"OperatorID":"operator_id","EvseID":"evse_id","ChargingStationId":"charging_station_id","City":"city","Street":"street"})
    df_static.pop("OperatorName")

    df_postal_codes = pd.read_sql(sql_query_2(),con=engine_warehouse)

    df_static["PostalCode"] = df_static["PostalCode"].astype(int)
    df_postal_codes["postal_code"] = df_postal_codes["postal_code"].astype(int)

    # merge table to get id_postal_code from postal code table inside the static dfata table (foreign key)
    df_merged_static = pd.merge(left=df_static,left_on="PostalCode",right=df_postal_codes,right_on="postal_code")

    df_merged_static.drop(["canton_abbreviation","postal_code","PostalCode","OperatorName"],axis=1,inplace=True)

    df_merged_static["operator_id"] = df_merged_static["operator_id"].astype(str)
    df_merged_static["charging_station_id"] = df_merged_static["charging_station_id"].astype(str)
    df_merged_static["evse_id"] = df_merged_static["evse_id"].astype(str)
    df_merged_static["city"] = df_merged_static["city"].astype(str)
    df_merged_static["street"] = df_merged_static["street"].astype(str)
    df_merged_static["power"] = df_merged_static["power"].astype(int)
    df_merged_static["id_postal_code"] = df_merged_static["id_postal_code"].astype(int)

    # upload data into warehgouse table
    df_merged_static.to_sql(name=DB_TABLE_warehouse, con=engine_warehouse, if_exists="append", index=False)
    
    return {
        'statusCode': 200,
         'body': json.dumps('Upload completed!')
    }