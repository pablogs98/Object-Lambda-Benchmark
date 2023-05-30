import json
import uuid
import boto3
import os
import time


def lambda_handler(event, context):
    if not os.path.isfile("/tmp/instance_id"):
        with open("/tmp/instance_id", 'w') as iid_file:
            instance_id = uuid.uuid4().hex
            iid_file.write(instance_id)
    else:
        with open("/tmp/instance_id", 'r') as iid_file:
            instance_id = iid_file.read()

    with open('/proc/self/cgroup', 'r') as f:
        lines = f.readlines()
        for line in lines:
            if "cpu,cpuacct:" in line:
                instance_root_id = line.replace("cpu,cpuacct:", "").split('/')[1]

    time.sleep(20)

    try:
        object_get_context = event["getObjectContext"]
        print(object_get_context)
        request_route = object_get_context["outputRoute"]
        request_token = object_get_context["outputToken"]
        print(request_token)
        print(request_route)

        # Write object back to S3 Object Lambda
        s3 = boto3.client('s3')
        s3.write_get_object_response(
            Body=json.dumps({'instance_id': instance_id, 'instance_root_id': instance_root_id}),
            RequestRoute=request_route,
            RequestToken=request_token)

        return {'status_code': 200}
    except KeyError:
        return {'instance_id': instance_id, 'instance_root_id': instance_root_id}
