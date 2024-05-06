import json
import csv
import boto3
import pandas as pd
from io import StringIO
import io

def handler(event, context):
    # Log the event argument for debugging and for use in local development.
    print(json.dumps(event))

    # extract s3 file from event

    record = event['Records'][0]
    s3_bucket = record['s3']['bucket']['name']
    s3_key = record['s3']['object']['key']

    print(s3_bucket)
    print(s3_key)


    s3 = boto3.client('s3')
    bucket_name = s3_bucket
    api_file_key = s3_key

    # read file saved via api
    api_object = s3.get_object(Bucket=bucket_name, Key=api_file_key)
    api_file_data = pd.read_csv(StringIO(api_object['Body'].read().decode('utf-8')))

    print(api_file_data.head())

    # read static file 
    
    static_file_key = 'static/static_crash_report.csv'

    static_object = s3.get_object(Bucket=bucket_name, Key=static_file_key)
    static_file_data = pd.read_csv(StringIO(static_object['Body'].read().decode('utf-8')))

    print(static_file_data.head())


#    rename static file column name
    static_file_data = static_file_data.rename(columns={'Report Number': 'report_number'})

    merged_data=pd.merge(api_file_data, static_file_data, on='report_number', how='inner')

    # print merged_data
    merged_data.head()

    merged_bucket_name = 'merged-data-infra-637423608110'
    merged_file_key = 'output/merged.csv'

    with io.StringIO() as csv_buffer:
        merged_data.to_csv(csv_buffer, index=False)

        response = s3.put_object(
            Bucket=merged_bucket_name, Key=merged_file_key, Body=csv_buffer.getvalue()
        )

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")



    return {}