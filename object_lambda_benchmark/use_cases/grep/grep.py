import json

from object_lambda_benchmark.utils.aws_request import s3_get
from object_lambda_benchmark.utils.utils import ObjectLambdaFunction, LambdaFunction
import time
import uuid

runtime = "python3.8"
handler = "lambda_function.lambda_handler"
repetitions = 10

zip_paths = ["object_lambda/python/deployment-package.zip", "lambda/python/deployment-package.zip"]

lambda_configurations = [('HDFS-1kb.log', 128), ('HDFS-10kb.log', 128), ('HDFS-100kb.log', 128),
                         ('HDFS-1mb.log', 128), ('HDFS-10mb.log', 128),
                         ('HDFS-100mb.log', 512), ('HDFS-1gb.log', 3072)]

object_lambda_configurations = [('HDFS-1kb.log', 128), ('HDFS-10kb.log', 128),
                                ('HDFS-100kb.log', 128),
                                ('HDFS-1mb.log', 128), ('HDFS-10mb.log', 128),
                                ('HDFS-100mb.log', 256),
                                ('HDFS-1gb.log', 1536)]

bucket = 'pablo-data'
results_key = 'results/grep_results.txt'
grep_string = "warn"

for c in range(len(lambda_configurations)):
    object_lambda_function = ObjectLambdaFunction(function_name=f"pablo-grep-object-lambda-python",
                                                  memory=object_lambda_configurations[c][1])
    lambda_function = LambdaFunction(function_name=f"pablo-grep-lambda-python",
                                     memory=lambda_configurations[c][1])

    # Creates both Lambda function and Object Lambda endpoint
    object_lambda_function.create_function(handler, runtime, 'pablo-execution-role', f'{zip_paths[0]}', 'pablo-data-ap')
    lambda_function.create_function(handler, runtime, 'pablo-execution-role', f'{zip_paths[1]}', 'pablo-data-ap')
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
              f"Running {runtime} grep. Key={key}. Repetition {n}.")

        # Object Lambda
        t0 = time.time()
        try:
            res = object_lambda_function.invoke_function(key=f"data/{key}", params={'grep_string': grep_string})
            print(f"Results length: {len(res['body'])}")
        except Exception as e:
            print(f"An error occured: {e}. Skipping.")
        t1 = time.time()
        object_lambda_time = t1 - t0
        object_lambda_times.append(object_lambda_time)

        # Lambda
        t0 = time.time()
        try:
            res = lambda_function.invoke_function(
                params={'object_key': f"data/{key}", 'bucket': bucket, 's3_results_key': results_key,
                        'grep_string': grep_string})
            if res['body'] == "200":
                print("Results bigger than 6MB. Fetching from storage.")
                res = s3_get(bucket=bucket, key=results_key, region="us-east-2")
            print(f"Results length: {len(res['body'])}")
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

    with open(f"grep_{key.replace('.', '')}_{runtime.replace('.', '')}.json", 'w') as results_file:
        json.dump(result, results_file)

    object_lambda_function.delete_function()
    lambda_function.delete_function()
