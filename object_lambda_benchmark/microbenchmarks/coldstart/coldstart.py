import json
from datetime import datetime
from pathlib import Path
import grpc

from object_lambda_benchmark.utils.utils import ObjectLambdaFunction, LambdaFunction
import time
import uuid
from object_lambda_benchmark.utils.time_server import time_server_pb2, time_server_pb2_grpc

time_server_address = "0.0.0.0"
memory_sizes = [256, 1024, 4096]
runtimes = ["python3.8", "nodejs14.x"]
runtimes_no_number = ["python", "node"]

handlers = ["lambda_function.lambda_handler",
            "index.handler"]
repetitions = 10

zip_paths = ["python/deployment-package.zip",
             "nodejs/deployment-package.zip"]

for r, runtime in enumerate(runtimes):
    for m_size in memory_sizes:
        object_lambda_times = []
        lambda_times = []
        for n in range(repetitions):
            function_id = uuid.uuid4().hex
            print(f"--------------------------------------------------------------------------------------\n"
                  f"Running {runtime} coldstart benchmark. Memory size = {m_size}. Repetition {n}.")

            # Define functions
            try:
                olambda_function = ObjectLambdaFunction(function_name=f"pablo-coldstart-{runtimes_no_number[r]}",
                                                        memory=m_size)
                lambda_function = LambdaFunction(function_name=f"pablo-coldstart-{runtimes_no_number[r]}",
                                                 memory=m_size)

                # Creates both Lambda function and Object Lambda endpoint
                olambda_function.create_function(handlers[r], runtime, 'pablo-execution-role', f'{zip_paths[r]}',
                                                 'pablo-data-ap')
                time.sleep(5)

                with grpc.insecure_channel(f'{time_server_address}:50051') as channel:
                    stub = time_server_pb2_grpc.TimeStub(channel)

                    # Write Object Lambda init timestamp to timeserver here!
                    stub.WriteTime(time_server_pb2.TimeRequest(id=f'object_lambda_{function_id}'))
                    olambda_function.invoke_function(params={'time_server_address': time_server_address,
                                                             'function_id': f'object_lambda_{function_id}'})

                    olambda_function.delete_function()
                    lambda_function.create_function(handlers[r], runtime, 'pablo-execution-role', f'{zip_paths[r]}',
                                                    'pablo-data-ap')

                    # Write Lambda init timestamp to timeserver here!
                    stub.WriteTime(time_server_pb2.TimeRequest(id=f'lambda_{function_id}'))
                    lambda_function.invoke_function(params={'time_server_address': time_server_address,
                                                            'function_id': f'lambda_{function_id}'})
                    lambda_function.delete_function()

            except Exception as e:
                print(f"An error occured: {e}. Skipping.")

            # Get cold start times from the file created by the time server
            try:
                with open(f'timestamp-object_lambda_{function_id}.txt', 'r') as times_file:
                    times = times_file.readlines()
                    object_lambda_coldstart = float(times[1]) - float(times[0])
                    object_lambda_times.append(object_lambda_coldstart)

                with open(f'timestamp-lambda_{function_id}.txt', 'r') as times_file:
                    times = times_file.readlines()
                    lambda_coldstart = float(times[1]) - float(times[0])
                    lambda_times.append(lambda_coldstart)
            except Exception:
                print("An error occurred, skipping measurement.")

            print(
                f"Done! Object Lambda coldstart time = {object_lambda_coldstart}s. "
                f"Lambda coldstart time = {lambda_coldstart}s\n"
                f"--------------------------------------------------------------------------------------\n")

        result = {'object_lambda_coldstart_times': object_lambda_times,
                  'lambda_coldstart_times': lambda_times}

        now = datetime.now()
        Path(f"./results/{now.day}/{now.hour}").mkdir(parents=True, exist_ok=True)

        with open(f"./results/{now.day}/{now.hour}/{runtime.replace('.', '')}-{m_size}.json", 'w') as results_file:
            json.dump(result, results_file)
