import psycopg2
import psycopg2.extras as extras
import config_lake as creds
import pandas as pd
import boto3
from os import listdir
from os.path import isfile, join


# Inserting dataframe into database
def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS MeasuringPointsCoordinates \
        (Nr int, Status_BGDI int, Measuring_Point varchar, \
        Canton varchar, Street varchar, Coordinate_East int, \
        Coordinate_Nord int);")  # create
        extras.execute_values(cur, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cur.close()
        return 1
    print("the dataframe is inserted")
    cur.close()


def lambda_handler(event, context):
    try:
        # Set up a connection to the postgres server.
        conn_string = "host=" + creds.PGHOST + " port=" + "5432" + " dbname=" + \
                      creds.PGDATABASE + " user=" + creds.PGUSER + " password=" + creds.PGPASSWORD

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

    # Accessing the S3 buckets using boto3 client
    s3_client = boto3.client('s3')
    s3_bucket_name = 'electricbucket'
    s3 = boto3.resource('s3',
                        aws_access_key_id='ASIAWGCGFD37AOGGODRE',
                        aws_secret_access_key='etd3epsKezyB1568g8CpbxsB6QDD1yasSGHPWb9O',
                        aws_session_token='FwoGZXIvYXdzEID//////////wEaDFEwGx8qiW4vMDAaly\
                        K2ASdvYMq4EwAzeT4pU364iL8PgbSbEKT60Ik4glk5Dph4J6Wd+vBqZDBvVHookwL\
                        RjK6vf0/c+QU7tuzLkJyMkQHXAfaJQD8QgJCixk6yFM9lpPXu9Jh8SkvFVr1dVNL1\
                        E+gJ1VRoQjqXmXteo8k8t4uxXXJ19JYSQFlwsBfBiDOGjGEVo6RErnvpLNQqCHVAc\
                        Ix9Pw5yu/K7DZjMFDllyFNTTpHu9QylFD6x21UlRtr6nVZS20c7KJf56JsGMi1Tdh\
                        vEgfXGDS2zK928R22NxlmjZkLY+s3xHcXREzUxiH0Goe34lvlhF91jQY0=')

    my_bucket = s3.Bucket(s3_bucket_name)

    # Objects in bucket
    for my_bucket_object in my_bucket.objects.all():
        print(my_bucket_object)

    # Download
    s3_client.download_file('electricbucket', 'Messstellen.xlsx', '/tmp/Messstellen.xlsx')

    # Check if file was saved
    onlyfiles = [f for f in listdir('/tmp') if isfile(join('/tmp', f))]
    print(onlyfiles)

    # Convert to df
    df = pd.read_excel('/tmp/Messstellen.xlsx')
    # print(df.head())

    # Extract need columns
    df_coordinates = df[['Nr', 'Status_BGDI', 'Zaehlstellen_Bezeichnung', 'Kanton', 'Strasse',
                         'Koordinate_Ost', 'Koordinate_Nord']]

    # Rename columns
    df_coordinates.rename(columns={'Zaehlstellen_Bezeichnung': 'Measuring_Point', \
                                   'Kanton': 'Canton', 'Strasse': 'Street', 'Koordinate_Ost': 'Coordinate_East', \
                                   'Koordinate_Nord': 'Coordinate_Nord'}, inplace=True)

    print(df_coordinates.head())

    # Insert dataframe
    try:
        execute_values(conn, df_coordinates, 'MeasuringPointsCoordinates')
    except psycopg2.Error as e:
        print("Error: Creating Table")
        print(e)

    # try:
    #    cur.execute("DROP TABLE MeasuringPointsCoordinates;")
    #    print("table deleted")
    # except psycopg2.Error as e:
    #    print("Error: select *")
    #    print (e)
    #
    # cur.close()
    # conn.close()