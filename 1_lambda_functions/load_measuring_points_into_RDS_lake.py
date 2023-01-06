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
    s3_bucket_name = 'measuringpoints'
    s3 = boto3.resource('s3',
                        aws_access_key_id='ASIARB6VMN3JD6ZCZ4EE',
                        aws_secret_access_key='j9Iv+R9tbl/7YIIWi3y5bq9jAfv1775tvsx8ERgr',
                        aws_session_token='FwoGZXIvYXdzEEEaDEzvdRLdrORfO3MXZyK4AfSf7rabfISrNNFWZi0K7AMVz8GF/Oaf2z81Xo41YJLfn5DudUJS5TXmU7CHNWybaK0ItQKgSCbLIxTNq6gI8sWvmeGk62sjKvHtpwxY3wi0z2ImM/dJBNcHB3Vi780CfSIHH6UJd1EjQ8rwiFeD9CTQzntHuACpU//a7q06x1thwbqNFkDsis9Pbt1VbzGhHse18VQx1hHTDfdawlzy7NAaszWyl/loV2iv4o9enQjUKHEfOJ+a5FIo3szLnAYyLV8juKquCF6vXJBsbOQ76Y2yXcMOnmpiKHpXbNc2qVIpy3kqzkxErzAahXyVYQ==')

    #""" Getting data files from the AWS S3 bucket as denoted above and printing the first 10 file names having prefix "2019/7/8" """
    my_bucket = s3.Bucket(s3_bucket_name)

    for my_bucket_object in my_bucket.objects.all():
        print(my_bucket_object)


    with open('/tmp/Measuring_points_Annual_results_2021_.csv', 'wb') as f:
        s3_client.download_fileobj('measuringpoints', 'Measuring_points_Annual_results_2021_.csv', f)
        df = pd.read_csv('/tmp/Measuring_points_Annual_results_2021_.csv', sep=';')
        print(df)

    DB_LOGIN = {
        "db_host": os.environ['DB_HOST'],
        "db_name": os.environ['DB_NAME'],
        "db_port": os.environ['DB_PORT'],
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

    df.to_sql(name='measuringpoints', con=engine, if_exists="replace", index=True)

