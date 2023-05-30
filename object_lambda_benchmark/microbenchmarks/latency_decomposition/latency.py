import json
import os
import uuid

import boto3
import grpc
from lithops import FunctionExecutor

import time_server_pb2_grpc, time_server_pb2
from object_lambda_benchmark.microbenchmarks.latency_decomposition.map import _map
from object_lambda_benchmark.utils.utils import ObjectLambdaFunction, LambdaFunction
import time

function_classes = [LambdaFunction, ObjectLambdaFunction]
prefixes = ['lambda', 'object_lambda']
m_size = 2048
runtime = "python3.8"
handler = "lambda_function.lambda_handler"
zip_path = "/python/deployment-package.zip"
repetitions = 1
time_server_address = "18.222.138.118"

object_lambda_times = {'t_start': [], 't_return': [], 't_get_object': [], 't_write_get_object_response': []}

n_calls = [100, 500, 1000]
n_functions = 50

s3 = boto3.client('s3')
with grpc.insecure_channel(f'{time_server_address}:50051') as channel:
    stub = time_server_pb2_grpc.TimeStub(channel)
    for n in n_calls:
        # create function
        ol_funct = ObjectLambdaFunction(function_name=f"pablo-latency-decomposition-{runtime.replace('.', '')}",
                                        memory=m_size)
        ol_funct.create_function(handler, runtime, 'pablo-execution-role', f'.{zip_path}',
                                 'pablo-data-ap')

        time.sleep(5)

        for r in range(repetitions):
            print(f"Repetition {r}.")
            unique_id = uuid.uuid4().hex

            # call Object Lambda, get time decomposition
            function_ids = [f"object_lambda-{unique_id}-{worker_id}" for worker_id in range(n)]
            print(function_ids)
            params = [{'function_ids': function_ids[n * int(len(function_ids) / n_functions): (n + 1) * int(
                len(function_ids) / n_functions)],
                       's3ol_access_point_arn': ol_funct.s3ol_access_point,
                       'time_server_address': time_server_address,
                       'execution_id': unique_id,
                       'n_workers': n,
                       'aws_environment_variables': {'AWS_SECRET_ACCESS_KEY': os.environ['AWS_SECRET_ACCESS_KEY'],
                                                     'AWS_ACCESS_KEY_ID': os.environ['AWS_ACCESS_KEY_ID']}} for n in
                      range(n_functions)]

            f_exec = FunctionExecutor(runtime_memory=8192, log_level='debug', runtime='pablogs98-lithops-aws:04')
            futures = f_exec.map(_map, params)
            futures.wait()
            time.sleep(10)
            for f_id in function_ids:
                try:
                    with open(f'timestamp-{f_id}.txt', 'r') as times_file:
                        try:
                            lines = times_file.readlines()
                            timestamps = [float(l) for l in lines]
                            t_start = timestamps[1] - timestamps[0]
                            t_write_get_object_response = timestamps[3] - timestamps[2]
                            t_return = timestamps[4] - timestamps[3] - 10

                            object_lambda_times['t_start'].append(t_start)
                            object_lambda_times['t_write_get_object_response'].append(t_write_get_object_response)
                            object_lambda_times['t_return'].append(t_return)
                        except:
                            print(f"Incomplete data in file timestamp-{f_id}.txt: {timestamps}")
                except FileNotFoundError:
                    print(f"File timestamp-{f_id}.txt not found. Skip.")

        with open(f"latency_decomposition_object_lambda_{n}_calls.json", "w") as file:
            json.dump(object_lambda_times, file)
