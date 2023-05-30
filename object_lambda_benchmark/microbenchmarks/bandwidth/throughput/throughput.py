import time
import json

from object_lambda_benchmark.utils.utils import LambdaFunction, ObjectLambdaFunction

memory_sizes = [4096]
part_size = 100 * 1024 * 1024
n_functions = [100, 250, 1000]

runtime = 'python3.8'
handler = "lambda_function.lambda_handler"
zip_path = "python/deployment-package.zip"
repetitions = 1

object_lambda_funct = ObjectLambdaFunction(function_name=f"pablo-throughput-{runtime.replace('.', '')}")
lambda_funct = LambdaFunction(function_name=f"pablo-throughput-{runtime.replace('.', '')}")

# Create both
object_lambda_funct.create_function(handler, runtime, 'pablo-execution-role',
                                    f'{zip_path}', 'pablo-data-ap')
time.sleep(5)

for n_func in n_functions:
    for m_size in memory_sizes:
        results_dict = {'object_lambda_times': [], 'lambda_times': []}
        for r in range(repetitions):
            object_lambda_funct.memory = m_size
            object_lambda_funct.update_function()
            time.sleep(5)

            # Object Lambda
            t0 = time.time()
            results = object_lambda_funct.map(keys=['data/throughput_test_object_10GB.txt'] * n_func,
                                              params=[{'part_number': n, 'part_size': part_size} for n in
                                                      range(n_func)])
            object_lambda_time = max([result['total_time'] for result in results])
            t1 = time.time()

            print(f"Client time: {t1 - t0}. Max time (all invocations): {object_lambda_time}")
            results_dict['object_lambda_times'].append(object_lambda_time)

        with open(f"throughput-{runtime.replace('.', '')}-{m_size}-n_functions-{n_func}.json", 'w') as results_file:
            json.dump(results_dict, results_file)
object_lambda_funct.delete_function()
