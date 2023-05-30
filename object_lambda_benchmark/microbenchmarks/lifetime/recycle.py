import json

from object_lambda_benchmark.utils.utils import ObjectLambdaFunction, LambdaFunction
import time

function_classes = [LambdaFunction, ObjectLambdaFunction]
prefixes = ['lambda', 'object_lambda']
memory_sizes = [256, 1024, 4096]
runtimes = ["python3.8"]
handlers = ["lambda_function.lambda_handler"]
zip_paths = ["/python/deployment-package.zip"]
repetitions = 10


def recycle_benchmark(function_class, memory_sizes, runtime, handler, zip_path, prefix):
    for m_size in memory_sizes:
        sleep_times = []
        for r in range(repetitions):
            print(f"--------------------------------------------------------------------------------------\n"
                  f"Running {prefix}, {runtime} recycle benchmark. Memory size = {m_size}.")
            sleep_time = 60  # seconds
            while True:
                funct = function_class(function_name=f"pablo-{prefix}-recycle-{runtime.replace('.', '')}",
                                       memory=m_size)
                funct.create_function(handler, runtime, 'pablo-execution-role',
                                      f'{prefix}{zip_path}', 'pablo-data-ap')
                time.sleep(5)
                instance_id_1 = funct.invoke_function(key='a')['body']
                time.sleep(sleep_time)
                instance_id_2 = funct.invoke_function(key='a')['body']
                funct.delete_function()

                if instance_id_1 != instance_id_2:
                    print(f"Instance id 1 != instance id 2: {instance_id_1} is not {instance_id_2}")
                    break
                else:
                    print(
                        f"Instance id 1 == instance id 2. Current max time for instance {instance_id_1}: {sleep_time}")
                    sleep_time += 60  # increments of 60s
            print(f"Done!\n"
                  f"--------------------------------------------------------------------------------------\n")
        with open(f"recycle-{runtime.replace('.', '')}-{prefix}-{m_size}.json", 'w') as results_file:
            json.dump({'sleep_times': sleep_times}, results_file)


for f, function_class in enumerate(function_classes):
    for r, runtime in enumerate(runtimes):
        recycle_benchmark(function_class, memory_sizes, runtime, handlers[r], zip_paths[r], prefixes[f])
