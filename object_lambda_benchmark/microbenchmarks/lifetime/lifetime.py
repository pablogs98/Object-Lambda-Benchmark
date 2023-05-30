import json

from object_lambda_benchmark.utils.utils import ObjectLambdaFunction, LambdaFunction
import time

function_classes = [LambdaFunction, ObjectLambdaFunction]
prefixes = ['lambda', 'object_lambda']
memory_sizes = [256, 4096]
sleep_times = [5, 60]
runtimes = ["python3.8", "java11", "nodejs14.x"]
handlers = ["lambda_function.lambda_handler",
            "org.example.Lifetime::handleRequest",
            "index.handler"]

zip_paths = ["/python/deployment-package.zip",
             "/java/deployment-package.zip",
             "/nodejs/deployment-package.zip"]


def lifetime_benchmark(function_class, memory_sizes, runtime, handler, zip_path, prefix, sleep_times):
    for sleep_time in sleep_times:
        for m_size in memory_sizes:
            results = {}
            print(f"--------------------------------------------------------------------------------------\n"
                  f"Running {prefix}, {runtime} lifetime benchmark. Memory size = {m_size}.")
            funct = function_class(function_name=f"pablo-{prefix}-lifetime-{runtime.replace('.', '')}", memory=m_size)
            funct.create_function(handler, runtime, 'pablo-execution-role',
                                  f'{prefix}{zip_path}', 'pablo-data-ap')
            time.sleep(5)
            try:
                while len(results.keys()) < 25:
                    instance_id = funct.invoke_function()['body']
                    if instance_id in results.keys():
                        results[instance_id]['last_time'] = time.time()
                        print(f"New instance id = {instance_id}")
                    else:
                        results[instance_id] = {'init_time': time.time(), 'last_time': None}
                    time.sleep(sleep_time)
            except Exception:
                with open(f"{runtime.replace('.', '')}-{prefix}-{m_size}-sleep-{sleep_time}.json", 'w') as results_file:
                    json.dump(results, results_file)
                funct.delete_function()
                raise

            print(f"Done!\n"
                  f"--------------------------------------------------------------------------------------\n")
            with open(f"{runtime.replace('.', '')}-{prefix}-{m_size}-sleep-{sleep_time}.json", 'w') as results_file:
                json.dump(results, results_file)


for f, function_class in enumerate(function_classes):
    for r, runtime in enumerate(runtimes):
        lifetime_benchmark(function_class, memory_sizes, runtime, handlers[r], zip_paths[r], prefixes[f],
                           sleep_times=sleep_times)
