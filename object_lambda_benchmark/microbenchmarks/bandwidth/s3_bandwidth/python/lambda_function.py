import json
import boto3
import time


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    t0 = time.time()
    a = s3.get_object(Bucket='pablo-data', Key='bandwith-benchmark-object')['Body'].read()
    t1 = time.time()
    print(a)
    total_time = t1 - t0

    try:
        object_get_context = event["getObjectContext"]
        print(object_get_context)
        request_route = object_get_context["outputRoute"]
        request_token = object_get_context["outputToken"]

        # Write object back to S3 Object Lambda
        s3.write_get_object_response(
            Body=json.dumps({'total_time': total_time}),
            RequestRoute=request_route,
            RequestToken=request_token)

        return {'status_code': 200}
    except KeyError:
        return {'total_time': total_time}
