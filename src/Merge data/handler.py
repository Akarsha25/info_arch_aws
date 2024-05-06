import boto3
import json

def handler(event, context):
    try:
        # Initialize S3 clients
        s3 = boto3.client('s3')

        # Define bucket names and file keys
        source_bucket = 'info-arch-datafiles'
        target_bucket = 'info-arch-infra-bucket2-637423608110'
        file1_key = 'output_api_2024-01-01_to_2022-01-01.csv'  # Replace with your file keys
        file2_key = 'Crash_Reporting_-_Non-Motorists_Data_20240502.csv'

        # Download file contents
        file1_obj = s3.get_object(Bucket=source_bucket, Key=file1_key)
        file2_obj = s3.get_object(Bucket=source_bucket, Key=file2_key)

        # Read file contents
        file1_content = file1_obj['Body'].read().decode('utf-8')
        file2_content = file2_obj['Body'].read().decode('utf-8')

        # Merge file contents
        merged_content = file1_content + '\n' + file2_content

        # Upload merged file
        merged_key = 'merged_file.txt'  # Adjust the merged file key as needed
        s3.put_object(Bucket=target_bucket, Key=merged_key, Body=merged_content)

        return {
            'statusCode': 200,
            'body': json.dumps('Files merged and uploaded successfully!')
        }
    except Exception as e:
        # Log the error
        print(f"An error occurred: {str(e)}")
        # Return error response
        return {
            'statusCode': 500,
            'body': json.dumps('An error occurred while processing the request.')
        }
