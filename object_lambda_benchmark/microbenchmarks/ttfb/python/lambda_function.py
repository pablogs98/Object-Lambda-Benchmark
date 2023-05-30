import boto3
import requests


def lambda_handler(event, context):
    s3_client = boto3.client('s3')

    # Object Lambda
    try:
        object_get_context = event["getObjectContext"]
        request_route = object_get_context["outputRoute"]
        request_token = object_get_context["outputToken"]
        s3_url = object_get_context["inputS3Url"]
        # Get object from S3
        response = requests.get(s3_url)
        obj = response.content

        s3_client.write_get_object_response(
            Body=obj,
            RequestRoute=request_route,
            RequestToken=request_token)
    # Lambda
    except KeyError:
        bucket = event['bucket']
        key = event['object_key']

        obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        if key == 'data/10mb.txt' or key == 'data/100mb.txt':
            s3_client.put_object(Bucket=bucket, Key='results/ttfb_lambda_result', Body=body)
            return 200
        else:
            return "a" * 1024
