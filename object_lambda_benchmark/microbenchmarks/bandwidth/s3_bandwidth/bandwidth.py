import concurrent.futures
import time
import json

import boto3

from object_lambda_benchmark.utils.utils import LambdaFunction, ObjectLambdaFunction

memory_sizes = [4096]
object_size = 200 * 1024 * 1024
runtime = 'python3.8'
handler = "lambda_function.lambda_handler"
zip_path = "python/deployment-package.zip"

for m_size in memory_sizes:
    results_dict = {'lambda': {}, 'object_lambda': {}}

    object_lambda_funct = ObjectLambdaFunction(function_name=f"pablo-bandwidth-{runtime.replace('.', '')}",
                                               memory=m_size)
    lambda_funct = LambdaFunction(function_name=f"pablo-bandwidth-{runtime.replace('.', '')}")

    object_lambda_funct.create_function(handler, runtime, 'pablo-execution-role',
                                        f'{zip_path}', 'pablo-data-ap')
    time.sleep(5)

    bandwidths = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        args = [lambda_funct] * 100
        results = executor.map(lambda x: x.invoke_function(), args)

        agg_bandwidth = 0
        for result in results:
            result = result['body']
            result = json.loads(result)
            read_time = float(result['total_time'])
            print(result)
            bandwidth = object_size / read_time
            agg_bandwidth += bandwidth
            bandwidths.append(bandwidth)
        results_dict['lambda']['bandwidths'] = bandwidths
        results_dict['lambda']['aggregate_bandwidth'] = agg_bandwidth

    bandwidths = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        args = [object_lambda_funct] * 100
        results = executor.map(lambda x: x.invoke_function(), args)

        agg_bandwidth = 0
        for result in results:
            result = result['body']
            result = json.loads(result)
            print(result)
            read_time = float(result['total_time'])
            bandwidth = object_size / read_time
            agg_bandwidth += bandwidth
            bandwidths.append(bandwidth)
        results_dict['object_lambda']['bandwidths'] = bandwidths
        results_dict['object_lambda']['aggregate_bandwidth'] = agg_bandwidth

    with open(f"bandwidth-{runtime.replace('.', '')}-{m_size}.json", 'w') as results_file:
        json.dump(results_dict, results_file)
    object_lambda_funct.delete_function()
