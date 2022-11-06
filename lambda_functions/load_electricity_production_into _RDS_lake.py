import json
import os
import io
import psycopg2
import pandas as pd
import geopandas as gpd
import requests
import numpy as np
import zipfile
import config_dbdynamic as creds
import boto3

# Dictionary for mapping of dataframe
dict_maincat = {'maincat_1': 'Hydroelectric power', 'maincat_2': 'Other renewable energies',
                'maincat_3': 'Nuclear energy', 'maincat_4': 'Fossil fuel'}

dict_subcat = {'subcat_1': 'Hydroelectric power', 'subcat_2': 'Photovoltaic', 'subcat_3': 'Wind energy',
               'subcat_4': 'Biomass', 'subcat_5': 'Geothermal energy', 'subcat_6': 'Nuclear energy',
               'subcat_7': 'Crude oil', 'subcat_8': 'Natural gas', 'subcat_9': 'Coal', 'subcat_10': 'Waste'}


# Function to create list of datatypes for PostgreSQL
def getColumnDtypes(dataTypes):
    dataList = []
    for x in dataTypes:
        if (x == 'int64'):
            dataList.append('int')
        elif (x == 'float64'):
            dataList.append('float')
        elif (x == 'bool'):
            dataList.append('boolean')
        else:
            dataList.append('varchar')
    return dataList


def lambda_handler(event, context):
    try:
        # Set up a connection to the postgres server.
        conn_string = "host=" + creds.PGHOST + " port=" + "5432" + " dbname=" + creds.PGDATABASE + " user=" + creds.PGUSER \
                      + " password=" + creds.PGPASSWORD

        conn = psycopg2.connect(conn_string)
        print("Connected!")

    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        # Create a cursor object
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)

    # Auto commit is very important
    conn.set_session(autocommit=True)

    # Request data from CKAN-API
    try:
        # meta data produktionsanlagen
        url = 'https://ckan.opendata.swiss/api/3/action/package_show?id=elektrizitatsproduktionsanlagen'
        response = requests.get(url)
        data = response.json()

        # Extract download_url
        position = response.text.find('gpkg')
        start = int(position - 80)
        end = int(position + 80)
        string = response.text[start:end]
        start_url = string.find('https')
        end_url = string.find('.zip')
        print('gpkg file download_url: ', string[start_url:(end_url + 4)])
        download_url = str(string[start_url:(end_url + 4)])

        # download gpkg file
        response = requests.get(download_url)
        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall('/tmp')  # hidden storage folder
        df = gpd.read_file('/tmp/ElectricityProductionPlants.gpkg')
        print('df created')

        # transform dataframe
        df1 = df.copy()
        # Transform floats/obj to strings where needed
        df1['MainCategory'] = df['MainCategory'].values.astype(str)
        df1['SubCategory'] = df['SubCategory'].values.astype(str)
        df1['Address'] = df['Address'].values.astype(str)
        df1['Municipality'] = df['Municipality'].values.astype(str)
        df1['Canton'] = df['Canton'].values.astype(str)
        df1['BeginningOfOperation'] = df['BeginningOfOperation'].values.astype(str)
        # Mapping
        df_mapped = df1.copy()
        df_mapped['MainCategory'] = df_mapped['MainCategory'].map(dict_maincat)
        df_mapped['SubCategory'] = df_mapped['SubCategory'].map(dict_subcat)
        # Extracting year of beginning of operation / Change it back to int
        df_mapped['BeginningOfOperation'] = df['BeginningOfOperation'].str[0:4]
        df_mapped['BeginningOfOperation'] = df_mapped['BeginningOfOperation'].values.astype(int)
        df_essential = df_mapped[
            ['xtf_id', 'PostCode', 'Municipality', 'Canton', 'BeginningOfOperation', 'MainCategory', 'SubCategory',
             'InitialPower', 'TotalPower']]
        print('df_essential created')

    except requests.exceptions.RequestException as e:
        raise SystemExit(e)

    try:
        # Prepare import to PostgreSQL
        columnName = list(df_essential.columns.values)
        columnDataType = getColumnDtypes(df_essential.dtypes)
        # Create table statement
        createTableStatement = 'CREATE TABLE IF NOT EXISTS ElectricityProductionPlants ('
        for i in range(len(columnDataType)):
            createTableStatement = createTableStatement + '\n' + columnName[i] + ' ' + columnDataType[i] + ','
        createTableStatement = createTableStatement[:-1] + ' );'
        # Create table
        cur.execute(createTableStatement)
    except psycopg2.Error as e:
        print("Error: Creating Table")
        print(e)

    try:
        cur.execute("SELECT * FROM ElectricityProductionPlants WHERE PostCode = 5082 ;")
    except psycopg2.Error as e:
        print("Error: select *")
        print(e)

    row = cur.fetchone()
    while row:
        print(row)
        row = cur.fetchone()

    # try:
    #    cur.execute("DROP TABLE ElectricityProductionPlants;")
    #    print("table deleted")
    # except psycopg2.Error as e:
    #    print("Error: select *")
    #    print (e)

    cur.close()
    conn.close()

