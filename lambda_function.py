import boto3
import gzip
import json
import urllib.parse 

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    print("Lambda invoked.")
    
    # Extract bucket and file key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Decode the key 
    key = urllib.parse.unquote_plus(key)
    
    print(f"Bucket: {bucket}")
    print(f"Key: {key}")

    # Download the file and read its content
    download_path = '/tmp/{}'.format(key.split('/')[-1])
    print(f"Downloading file to: {download_path}")
    s3_client.download_file(bucket, key, download_path)
    
    with gzip.open(download_path, 'rt') as f:
        data = json.load(f)
    
    print("File contents read successfully.")
    
    # Transform JSON to NDJSON
    ndjson_content = '\n'.join(json.dumps(item) for item in data)
    print("Transformation to NDJSON done.")
    
    # Define the output path
    output_key = 'transformed/' + key.split('/')[-1].replace('.json.gz', '.ndjson')
    output_path = '/tmp/transformed.ndjson'
    print(f"Writing transformed content to: {output_path}")

    with open(output_path, 'w') as f:
        f.write(ndjson_content)
    
    # Upload the transformed content to the different folder in the same S3 bucket
    print(f"Uploading transformed content to S3 bucket: {bucket} and key: {output_key}")
    s3_client.upload_file(output_path, bucket, output_key)
    
    # Delete the original file from S3 bucket
    print(f"Deleting original file from S3 bucket: {bucket} with key: {key}")
    s3_client.delete_object(Bucket=bucket, Key=key)

    print("Lambda execution completed.")
    
    return {
        'statusCode': 200,
        'body': json.dumps('File processed successfully!')
    }
