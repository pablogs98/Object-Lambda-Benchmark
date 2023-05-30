import json
import time

import boto3
import grpc
import time_server_pb2_grpc, time_server_pb2


def lambda_handler(event, context):
    response = b'a' * 1024 * 1024 * 10

    object_get_context = event["getObjectContext"]
    request_route = object_get_context["outputRoute"]
    request_token = object_get_context["outputToken"]
    params = json.loads(event['userRequest']['headers']['X-Function-Params'])
    function_id = params["function_id"]
    execution_id = params["execution_id"]
    time_server_address = params['time_server_address']
    n_workers = params['n_workers']

    s3 = boto3.client('s3')
    s3.put_object(Bucket="pablo-data", Key=f'{execution_id}/{function_id}')

    while len(s3.list_objects(Bucket="pablo-data", Prefix=f'{execution_id}/')['Contents']) != n_workers:
        time.sleep(2)

    with grpc.insecure_channel(f'{time_server_address}:50051') as channel:
        stub = time_server_pb2_grpc.TimeStub(channel)

        # Timestamp 2: function starts here (start time, t1 - t0)
        stub.WriteTime(time_server_pb2.TimeRequest(id=function_id))

        # s3.get_object(Bucket="pablo-data", Key="data/1mb.txt")['Body'].read()

        # Timestamp 3: S3 object read time (t2 - t1)
        stub.WriteTime(time_server_pb2.TimeRequest(id=function_id))

        # Write object back to S3 Object Lambda.
        s3.write_get_object_response(
            Body=response,
            RequestRoute=request_route,
            RequestToken=request_token)

        # Timestamp 4: function ends
        stub.WriteTime(time_server_pb2.TimeRequest(id=function_id))
        time.sleep(10)
        print("Done!")
    return response
