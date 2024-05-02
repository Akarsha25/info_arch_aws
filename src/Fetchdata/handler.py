import json

import datetime
import requests
import csv
import boto3
import os


def handler(event, context):
    # Log the event argument for debugging and for use in local development.
    print(json.dumps(event))
    print("fetching data")

    # # Get current date and date 7 days ago
    # current_date = datetime.datetime.now().date()
    # date_7_days_ago = current_date - datetime.timedelta(days=7)

    # # Format dates as strings
    # date1 = current_date.strftime("%Y-%m-%d")
    # date2 = date_7_days_ago.strftime("%Y-%m-%d")

    date1 = "2024-01-01"
    date2 = "2022-01-01"

    # API URL
    api_url = f"https://data.montgomerycountymd.gov/resource/mmzv-x632.csv?$where=crash_date_time between '{date2}T00:00:00' and '{date1}T23:59:59'"


    print(f"Invoking api {api_url}")
    # Send request to API
    response = requests.get(api_url)

    # Check if request was successful
    if response.status_code == 200:
        # Write data to CSV file
        print(response.text)
        file_format = f"/tmp/output_api_{date1}_to_{date2}.csv"
        with open(file_format, 'w', newline='') as csvfile:
            csvfile.write(response.text)

        # Upload CSV file to AWS S3 bucket
        s3 = boto3.client('s3')
        bucket_name = 'info-arch-datafiles'
        folder_path = f'dynamic/{date1}/'
        s3.upload_file(file_format, bucket_name, os.path.join(folder_path, file_format))

        print("File uploaded successfully to S3 bucket.")
    else:
        print("Failed to fetch data from API.")
