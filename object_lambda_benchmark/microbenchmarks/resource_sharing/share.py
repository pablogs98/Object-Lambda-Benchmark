import concurrent.futures
import json
from object_lambda_benchmark.utils.utils import ObjectLambdaFunction, LambdaFunction
import time

memory_sizes = [256]
runtimes = ["python3.8"]
handlers = ["lambda_function.lambda_handler"]
zip_paths = ["python/deployment-package.zip"]


def share_benchmark(memory_sizes, runtime, handler, zip_path):
    for m_size in memory_sizes:
        vm_ids = {'object_lambda': {}, 'lambda': {}}
        instance_ids = {'object_lambda': {}, 'lambda': {}}
        pairs = {'object_lambda': [], 'lambda': []}
        print(f"--------------------------------------------------------------------------------------\n"
              f"Running {runtime} share benchmark. Memory size = {m_size}.")
        object_lambda_funct = ObjectLambdaFunction(
            function_name=f"pablo-share-{runtime.replace('.', '')}",
            memory=m_size)
        lambda_funct = LambdaFunction(function_name=f"pablo-share-{runtime.replace('.', '')}")

        object_lambda_funct.create_function(handler, runtime, 'pablo-execution-role',
                                            f'{zip_path}', 'pablo-data-ap')
        time.sleep(5)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1000) as executor:
            args = [lambda_funct] * 1000
            results = executor.map(lambda x: x.invoke_function(), args)

            for result in results:
                result = result['body']
                result = json.loads(result)
                vm_id = str(result['instance_root_id'])
                instance_id = str(result['instance_id'])

                try:
                    vm_ids['lambda'][vm_id] += 1
                except:
                    vm_ids['lambda'][vm_id] = 1

                try:
                    instance_ids['lambda'][instance_id] += 1
                except:
                    instance_ids['lambda'][instance_id] = 1

                pairs['lambda'].append((vm_id, instance_id))

        with concurrent.futures.ThreadPoolExecutor(max_workers=1000) as executor:
            args = [object_lambda_funct] * 1000
            results = executor.map(lambda x: x.invoke_function(), args)
            for result in results:
                result = result['body']
                result = json.loads(result)
                vm_id = str(result['instance_root_id'])
                instance_id = str(result['instance_id'])

                try:
                    vm_ids['object_lambda'][vm_id] += 1
                except:
                    vm_ids['object_lambda'][vm_id] = 1

                try:
                    instance_ids['object_lambda'][instance_id] += 1
                except:
                    instance_ids['object_lambda'][instance_id] = 1

                pairs['object_lambda'].append((vm_id, instance_id))

        object_lambda_funct.delete_function()

        print(f"Done!\n"
              f"--------------------------------------------------------------------------------------\n")

        with open(f"share-{runtime.replace('.', '')}-{m_size}.json", 'w') as results_file:
            json.dump({'vm_ids': vm_ids, 'instance_ids': instance_ids, 'pairs': pairs}, results_file)


for r, runtime in enumerate(runtimes):
    share_benchmark(memory_sizes, runtime, handlers[r], zip_paths[r])
