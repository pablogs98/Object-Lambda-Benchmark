import boto3
import grpc
import json

import time_server_pb2_grpc, time_server_pb2


def lambda_handler(event, context):
    # Get Object Lambda params
    try:
        params = json.loads(event['userRequest']['headers']['X-Function-Params'])
        time_server_address = params["time_server_address"]
        function_id = params["function_id"]
    # Get Lambda params
    except KeyError:
        time_server_address = event["time_server_address"]
        function_id = event["function_id"]

    # Register cold start latency
    with grpc.insecure_channel(f'{time_server_address}:50051') as channel:
        stub = time_server_pb2_grpc.TimeStub(channel)
        stub.WriteTime(time_server_pb2.TimeRequest(id=function_id))

    try:
        object_get_context = event["getObjectContext"]
        request_route = object_get_context["outputRoute"]
        request_token = object_get_context["outputToken"]

        # Write object back to S3 Object Lambda
        s3 = boto3.client('s3')
        s3.write_get_object_response(
            Body="",
            RequestRoute=request_route,
            RequestToken=request_token)
    except KeyError:
        pass

    return {"statusCode": 200}
