import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Lambda function to process S3 object created events.
    Parses the contents of single-line files uploaded to S3.
    """
    try:
        # Process each record in the event
        for record in event['Records']:
            bucket_name = record['s3']['bucket']['name']
            object_key = record['s3']['object']['key']

            logger.info(f"Processing file: s3://{bucket_name}/{object_key}")

            # Download the file content
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            file_content = response['Body'].read().decode('utf-8').strip()

            # Parse the single line (assuming it's a simple text file with one line)
            parsed_data = parse_single_line(file_content)

            # Process the parsed data (example: log it)
            logger.info(f"Parsed data: {parsed_data}")

            # You can add more processing here, like storing to DynamoDB, sending to SNS, etc.

    except Exception as e:
        logger.error(f"Error processing S3 event: {str(e)}")
        raise e

    return {
        'statusCode': 200,
        'body': json.dumps('File processed successfully')
    }

def parse_single_line(line: str) -> dict:
    """
    Parse a single line of text. This is a simple example.
    Modify this function based on your file format.
    """
    # Example: assume the line is JSON
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        # If not JSON, treat as plain text
        return {'content': line}
