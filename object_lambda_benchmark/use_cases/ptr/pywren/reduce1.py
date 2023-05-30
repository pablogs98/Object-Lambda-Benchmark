import pickle
import time

import boto3
import pandas as pd


def reduce(execution_id, worker_id, n_workers, results_key, aws_environment_variables):
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_environment_variables['AWS_ACCESS_KEY_ID'],
                      aws_secret_access_key=aws_environment_variables['AWS_SECRET_ACCESS_KEY']
                      )

    data = s3.get_object(Bucket='pablo-data', Key=f'results/{execution_id}_{results_key}_0')['Body'].read()
    total_time = 0

    t0 = time.time()
    result: pd.Series = pickle.loads(data)
    t1 = time.time()
    total_time += (t1 - t0)

    for n in range(1, n_workers):
        data = s3.get_object(Bucket='pablo-data', Key=f'results/{execution_id}_{results_key}_{n}')['Body'].read()
        t0 = time.time()
        count = pickle.loads(data)
        result = result.add(count, fill_value=0)
        t1 = time.time()
        total_time += (t1 - t0)

    s3.put_object(Bucket='pablo-data', Key=f'results/{execution_id}_{worker_id}', Body=pickle.dumps(result))

    if worker_id == 0:
        s3.put_object(Bucket='pablo-data', Key=f'times/{execution_id}_reduce1_time',
                      Body=pickle.dumps(total_time))
