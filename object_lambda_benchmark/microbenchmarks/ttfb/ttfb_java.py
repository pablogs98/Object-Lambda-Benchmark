import time
from datetime import datetime
from pathlib import Path
from statistics import mean, stdev
import json

from object_lambda_benchmark.utils.aws_request import s3_get
from object_lambda_benchmark.utils.utils import LambdaFunction, ObjectLambdaFunction

memory_sizes = [512, 1024, 4096]
object_sizes = ['1kb', '10kb', '100kb', '1mb', '10mb', '100mb']
n_executions = 10

runtimes = ['java11']
runtimes_no_number = ['java']
lambda_functions = [f'pablo-ttfb-{runtime}' for runtime in runtimes_no_number]
zip_paths = ['java/lambda/deployment-package.zip', 'java/object_lambda/deployment-package.zip']
handlers = ["org.example.TTFB::handleRequest", "org.example.TTFB::handleRequest"]

for f, function_ in enumerate(lambda_functions):
    lambda_function = LambdaFunction(function_name=function_)
    olambda_function = ObjectLambdaFunction(function_name=f"{function_}_object_lambda")

    # Java requires two different functions
    lambda_function.create_function(handler=handlers[0], runtime=runtimes[f], role='pablo-execution-role',
                                    zip_file_path=zip_paths[0], s3_access_point='pablo-data-ap')

    olambda_function.create_function(handler=handlers[1], runtime=runtimes[f], role='pablo-execution-role',
                                     zip_file_path=zip_paths[1], s3_access_point='pablo-data-ap')

    for m_size in memory_sizes:
        # Update memory
        time.sleep(10)
        olambda_function.memory = m_size
        olambda_function.update_function()
        lambda_function.memory = m_size
        lambda_function.update_function()
        print(f"Updated lambdas' settings!")

        for object_size in object_sizes:
            times: dict = {'s3_object_lambda_times': [], 's3_object_lambda_mean_time': None,
                           's3_object_lambda_std_dev': None,
                           'lambda_times': [], 'lambda_mean_time': None, 'lambda_std_dev': None}
            object_key = f'data/{object_size}.txt'
            for n in range(n_executions):
                print("\n-------------------------------------------------------------------"
                      f"\nMemory size: {m_size}. Object size: {object_size}. Execution no. {n}"
                      "\n-------------------------------------------------------------------")
                # Lambda

                try:
                    results = lambda_function.invoke_function(params={'bucket': 'pablo-data', 'object_key': object_key})
                    ttfb = results['total_time']
                    if object_size == '10mb' or object_size == '100mb':
                        results = s3_get('pablo-data', 'results/ttfb_lambda_result', 'us-east-2')
                        ttfb += results['ttfb']
                    times['lambda_times'].append(ttfb)

                    # Object Lambda
                    results = olambda_function.invoke_function(key=object_key)
                    ttfb = results['ttfb']
                    times['s3_object_lambda_times'].append(ttfb)
                except Exception as e:
                    print(f"An error occurred: {e}. Skipping.")

            now = datetime.now()
            Path(f"./results/{now.day}/{now.hour}").mkdir(parents=True, exist_ok=True)

            with open(f'/results/{now.day}/{now.hour}/ttfb_{runtimes_no_number[f]}_{object_size}_{m_size}mb.json', 'w') as results_file:
                times['lambda_mean_time'] = mean(times['lambda_times'])
                times['lambda_std_dev'] = stdev(times['lambda_times'])
                times['s3_object_lambda_mean_time'] = mean(times['s3_object_lambda_times'])
                times['s3_object_lambda_std_dev'] = stdev(times['s3_object_lambda_times'])
                json.dump(times, results_file)
    # Delete all
    lambda_function.delete_function()
    olambda_function.delete_function()
