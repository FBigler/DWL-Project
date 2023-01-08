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
      query_operators = """ SELECT * FROM chargingstations_static """
      return query_operators

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
    df_operators = pd.read_sql(sql_query(),con=engine_lake)
    df_operators_table = df_operators[["OperatorID","OperatorName"]]
    df_operators_table = df_operators_table.rename(columns={"OperatorID":"operator_id","OperatorName":"name_operator"})
    df_operators_table = df_operators_table.drop_duplicates()

    # upload data into warehgouse table
    df_operators.to_sql(name=DB_TABLE_warehouse, con=engine_warehouse, if_exists="append", index=False)
    
    return {
        'statusCode': 200,
         'body': json.dumps('Upload completed!')
    }