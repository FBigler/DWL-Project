import boto3
import sys
import os
import pandas as pd
import csv
import io
import sqlalchemy


def lambda_handler(event, context):
    #"""Accessing the S3 buckets using boto3 client"""
    s3_client = boto3.client('s3')
    # s3_bucket_name='electriccars21'
    s3_bucket_name = 'measuringpoints'
       s3 = boto3.resource('s3',
                        aws_access_key_id= creds.aws_access_key_id,
                        aws_secret_access_key= creds.aws_secret_access_key,
                        aws_session_token= creds.aws_session_token)

    #""" Getting data files from the AWS S3 bucket as denoted above and printing the first 10 file names having prefix "2019/7/8" """
    my_bucket = s3.Bucket(s3_bucket_name)

    for my_bucket_object in my_bucket.objects.all():
        print(my_bucket_object)

    with open('/tmp/Electric_cars_21.csv', 'wb') as f:
       s3_client.download_fileobj('electriccars21', 'Electric_cars_21.csv', f)
       df = pd.read_csv('/tmp/Electric_cars_21.csv', sep=';')
       print(df)


    df_electriccars = df[['Canton', 'Vehicle group / type', 'Year of first registration', 'Fuel', '2021']]

    # for col in df_electriccars.columns:
    #    print(col)

    print(df_electriccars.head())

    DB_LOGIN = {
        "db_host": os.environ['DB_HOST'],
        "db_name": os.environ['DB_NAME'],
        "d-b_port": os.environ['DB_PORT'],
        "db_user": os.environ['DB_USER'],
        "db_pw": os.environ['DB_PW']
    }

    # DB_TABLE = creds.DB_TABLE

    def create_db_engine(db_host, db_port, db_name, db_user, db_pw):
        engine_url = f"postgresql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_name}"
        engine = sqlalchemy.create_engine(engine_url)
        return engine

    engine = create_db_engine(**DB_LOGIN)
    engine.connect()

    df.to_sql(name='electriccars21', con=engine, if_exists="replace", index=True)

