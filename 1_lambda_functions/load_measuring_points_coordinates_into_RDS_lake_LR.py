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
        Coordinate_Nord int, Status varchar, Type_Measuring_Point varchar, \
        Number_of_lanes int);")  # create
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
                        aws_access_key_id= creds.aws_access_key_id,
                        aws_secret_access_key= creds.aws_secret_access_key,
                        aws_session_token= creds.aws_session_token)

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
                         'Koordinate_Ost', 'Koordinate_Nord', 'Status', 'Messstellentyp',
                         'Anzahl_Fahrstreifen_Tot']]

    # Rename columns
    df_coordinates.rename(columns={'Zaehlstellen_Bezeichnung': 'Measuring_Point', \
                                   'Kanton': 'Canton', 'Strasse': 'Street', 'Koordinate_Ost': 'Coordinate_East', \
                                   'Koordinate_Nord': 'Coordinate_Nord', 'Messstellentyp': 'Type_Measuring_Point', \
                                   'Anzahl_Fahrstreifen_Tot': 'Number_of_lanes'}, inplace=True)

    for col in df_coordinates.columns:
        print(col)

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
