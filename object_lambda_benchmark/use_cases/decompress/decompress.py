import json

from object_lambda_benchmark.utils.utils import ObjectLambdaFunction, LambdaFunction
import time
import uuid

runtime = "python3.8"
handler = "lambda_function.lambda_handler"
repetitions = 10

zip_paths = ["object_lambda/python/deployment-package.zip", "lambda/python/deployment-package.zip"]

lambda_configurations = [('1kb.zip', 2048), ('10kb.zip', 2048), ('100kb.zip', 2048), ('1mb.zip', 2048), ('10mb.zip', 2048),
                         ('100mb.zip', 2048), ('1gb.zip', 2048)]

object_lambda_configurations = [('1kb.zip', 2048, 1e-6), ('10kb.zip', 2048, 1e-5), ('100kb.zip', 2048, 1e-4),
                                ('1mb.zip', 2048, 1e-3), ('10mb.zip', 2048, 1e-2), ('100mb.zip', 2048, 1e-1),
                                ('1gb.zip', 2048, 1)]

bucket = 'pablo-data'
results_key = 'results/decompress_results.txt'

for c in range(len(lambda_configurations)):
    object_lambda_function = ObjectLambdaFunction(function_name=f"pablo-decompress-object-lambda-python",
                                                  memory=object_lambda_configurations[c][1])
    lambda_function = LambdaFunction(function_name=f"pablo-decompress-lambda-python",
                                     memory=lambda_configurations[c][1])

    # Creates both Lambda function and Object Lambda endpoint
    object_lambda_function.create_function(handler, runtime, 'pablo-execution-role', f'{zip_paths[0]}',
                                           'pablo-data-ap')
    lambda_function.create_function(handler, runtime, 'pablo-execution-role', f'{zip_paths[1]}',
                                    'pablo-data-ap')
    time.sleep(5)

    try:
        object_lambda_function.invoke_function(params={})
    except:
        pass

    try:
        lambda_function.invoke_function(params={})
    except:
        pass

    object_lambda_times = []
    lambda_times = []

    key = lambda_configurations[c][0]
    for n in range(repetitions):
        function_id = uuid.uuid4().hex
        print(f"--------------------------------------------------------------------------------------\n"
              f"Running {runtime} decompress. Key={key}. Repetition {n}.")

        # Object Lambda
        t0 = time.time()
        try:
            object_lambda_function.invoke_function(key=f"data/{key}")
        except Exception as e:
            print(f"An error occured: {e}. Skipping.")
        t1 = time.time()
        object_lambda_time = t1 - t0
        object_lambda_times.append(object_lambda_time)

        # Lambda
        t0 = time.time()
        print('10mb.zip' == key or '100mb.zip' == key or '1gb.zip' == key)
        try:
            if '10mb.zip' == key or '100mb.zip' == key or '1gb.zip' == key:
                lambda_function.invoke_and_s3_get(
                    params={'object_key': f"data/{key}", 'bucket': bucket, 's3_results_key': results_key},
                    s3_results_key=results_key)
            else:
                lambda_function.invoke_function(
                    params={'object_key': f"data/{key}", 'bucket': bucket, 's3_results_key': results_key})
        except Exception as e:
            print(f"An error occured: {e}. Skipping.")
        t1 = time.time()
        lambda_time = t1 - t0
        lambda_times.append(lambda_time)

        print(f"Done! Object Lambda  time = {object_lambda_time}s. "
              f"Lambda  time = {lambda_time}s\n"
              f"--------------------------------------------------------------------------------------\n")

    result = {'object_lambda_times': object_lambda_times,
              'lambda_times': lambda_times}

    with open(f"decompress_{key.replace('.', '')}_{runtime.replace('.', '')}.json", 'w') as results_file:
        json.dump(result, results_file)

    object_lambda_function.delete_function()
    lambda_function.delete_function()
