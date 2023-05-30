import json
import os
import pickle
import time
import csv
import lithops
import uuid

from worker import worker
from object_lambda_benchmark.utils.utils import ObjectLambdaFunction


class ObjectLambdaMap:
    def __init__(self, map_function, function_name, bucket, do_l_counts):
        self.map_function = map_function
        self.function_name = function_name
        self.bucket = bucket
        self.do_l_counts = do_l_counts
        self.executor = lithops.FunctionExecutor(runtime_memory=1024, log_level='debug',
                                                 runtime='pablogs98-lithops-aws:04')

        # Create object lambda
        self.object_lambda_function = ObjectLambdaFunction(function_name=function_name, memory=768)
        # self.object_lambda_function.create_function(handler='lambda_function.lambda_handler',
        #                                             runtime='python3.8',
        #                                             role='pablo-execution-role',
        #                                             zip_file_path='python/deployment-package.zip',
        #                                             s3_access_point='pablo-data-ap')
        print(self.object_lambda_function.s3ol_access_point)

    def run(self, keys, n_workers, function_params, sync=False):
        execution_id = uuid.uuid4().hex
        n_functions = len(keys)

        params = [{'bucket': self.bucket,
                   'keys': ['a'] * n_functions,
                   'function_params': function_params,
                   'function_name': self.function_name,
                   'do_l_counts': self.do_l_counts,
                   'n_workers': n_workers,
                   'worker_id': n,
                   'execution_id': execution_id,
                   'sync': sync,
                   'aws_environment_variables': {'AWS_SECRET_ACCESS_KEY': os.environ['AWS_SECRET_ACCESS_KEY'],
                                                 'AWS_ACCESS_KEY_ID': os.environ['AWS_ACCESS_KEY_ID']},
                   's3ol_access_point_arn': 'arn:aws:s3-object-lambda:us-east-2:786929956471:accesspoint/pablo-object-lambda-reducer-ap'}
                  for n in range(n_workers)]

        futures = self.executor.map(self.map_function, params)
        return futures.get_result()


keys = ['ghtorrent-2019-02-04', 'ghtorrent-2019-03-11', 'ghtorrent-2019-04-15',
        'ghtorrent-2019-05-20']
n_workers = len(keys)
n_functions = 16
s3_chunk_size = 4 * 1024 * 1024
count_window2 = 100000
count_window1 = 100
repetitions = 10
do_l_counts = True
bucket = 'pablo-data'

function_name = 'pablo-object-lambda-reducer'
_map = ObjectLambdaMap(map_function=worker, function_name=function_name, bucket=bucket, do_l_counts=do_l_counts)

times = []
for r in range(repetitions):
    function_params = [{'count_window': count_window1,
                        'function_params': [{'s3_chunk_size': s3_chunk_size,
                                             'count_window': count_window2}] * n_functions,
                        's3ol_access_points': [
                                                  'arn:aws:s3-object-lambda:us-east-2:786929956471:accesspoint/pablo-object-lambda-map-ap'] * n_functions,
                        'keys': [f'ghtorrent/{keys[f]}_part{str(n).zfill(2)}.csv' for n in range(1, n_functions + 1)]}
                       for f in range(n_workers)]
    t0 = time.time()
    results = _map.run(keys, 1, function_params)[0]

    result = results[0]
    data_size = results[1]
    l_counts = results[2]
    t1 = time.time()
    if do_l_counts:
        with open('l_counts.pickle', 'wb') as f:
            pickle.dump(l_counts, f)
            print(l_counts)
    with open('result.pickle', 'wb') as f:
        pickle.dump(result, f)
        print(result)

    times.append(t1 - t0)

    print(f"Execution time = {t1 - t0}. Total data size={data_size * 2}.")

with open('word_count_results.json', 'w') as f:
    json.dump({'times': times}, f)
