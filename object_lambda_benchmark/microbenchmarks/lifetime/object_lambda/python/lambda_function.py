import uuid
import boto3
import os


def lambda_handler(event, context):
    object_get_context = event["getObjectContext"]
    request_route = object_get_context["outputRoute"]
    request_token = object_get_context["outputToken"]

    if not os.path.isfile("/tmp/instance_id"):
        with open("/tmp/instance_id", 'w') as iid_file:
            instance_id = uuid.uuid4().hex
            iid_file.write(instance_id)
    else:
        with open("/tmp/instance_id", 'r') as iid_file:
            instance_id = iid_file.read()

    # Write object back to S3 Object Lambda
    s3 = boto3.client('s3')
    s3.write_get_object_response(
        Body=instance_id,
        RequestRoute=request_route,
        RequestToken=request_token)

    return {'status_code': 200}
