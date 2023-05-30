import json
import os
import pickle
import time

import boto3
import lithops
import uuid

from object_lambda_benchmark.use_cases.word_count.pywren.map import map_
from object_lambda_benchmark.use_cases.word_count.pywren.reduce1 import reduce as reduce1
from object_lambda_benchmark.use_cases.word_count.pywren.reduce2 import reduce as reduce2

bucket = 'pablo-data'
keys = ['ghtorrent-2019-02-04', 'ghtorrent-2019-03-11', 'ghtorrent-2019-04-15',
        'ghtorrent-2019-05-20']

repetitions = 10
n_keys_per_reducer = 16
n_reducers = len(keys)
n_workers = n_keys_per_reducer * n_reducers

results_keys = ['ghtorrent-2019-02-04'] * n_keys_per_reducer + [
    'ghtorrent-2019-03-11'] * n_keys_per_reducer + ['ghtorrent-2019-04-15'] * n_keys_per_reducer + [
                   'ghtorrent-2019-05-20'] * n_keys_per_reducer

ok_keys = [f'ghtorrent/{key}_part{str(n).zfill(2)}.csv' for n in range(1, n_keys_per_reducer + 1) for key in keys]
execution_id = uuid.uuid4().hex

params = [{'bucket': bucket,
           'key': ok_keys[n],
           'n_workers': n_workers,
           'worker_id': n % n_keys_per_reducer,
           'execution_id': execution_id,
           'sync': False,
           'results_key': results_keys[n],
           'aws_environment_variables': {'AWS_SECRET_ACCESS_KEY': os.environ['AWS_SECRET_ACCESS_KEY'],
                                         'AWS_ACCESS_KEY_ID': os.environ['AWS_ACCESS_KEY_ID']}, } for n in
          range(n_workers)]

times = []
for r in range(repetitions):
    try:
        executor = lithops.FunctionExecutor(runtime_memory=1024, log_level='debug',
                                            runtime='pablogs98-lithops-aws:04')
        t0 = time.time()
        futures = executor.map(map_, params)
        futures.wait()
        futures = executor.map(reduce1,
                               [{'execution_id': execution_id, 'n_workers': n_keys_per_reducer, 'worker_id': n,
                                 'results_key': keys[n],
                                 'aws_environment_variables': {
                                     'AWS_SECRET_ACCESS_KEY': os.environ['AWS_SECRET_ACCESS_KEY'],
                                     'AWS_ACCESS_KEY_ID': os.environ['AWS_ACCESS_KEY_ID']}} for n
                                in
                                range(n_reducers)])
        futures.wait()

        t01 = time.time()
        print(f"F. time = {t01 - t0}")
        future = executor.call_async(reduce2, {'execution_id': execution_id, 'n_workers': n_reducers})
        result = future.result()
        t1 = time.time()
        total_time = t1 - t0
        times.append(total_time)
        print(result)
        print(f"Execution time = {total_time}s")
        s3 = boto3.client('s3')
        map_time = pickle.loads(s3.get_object(Bucket=bucket, Key=f'times/{execution_id}_map_time')['Body'].read())
        reduce1_time = pickle.loads(
            s3.get_object(Bucket=bucket, Key=f'times/{execution_id}_reduce1_time')['Body'].read())
        reduce2_time = pickle.loads(
            s3.get_object(Bucket=bucket, Key=f'times/{execution_id}_reduce2_time')['Body'].read())
        process_time = map_time + reduce1_time + reduce2_time
        io_time = total_time - process_time
        print(f"Process time = {process_time}s. I/O time = {io_time}s.")

        s3.delete_objects(Bucket='pablo-data',
                          Delete={'Objects': [{'Key': f'results/{execution_id}_{n}'} for n in range(n_workers)]})
        time.sleep(10)
    except:
        pass

with open('word_count_results_pywren.json', 'w') as f:
    json.dump({'times': times}, f)
