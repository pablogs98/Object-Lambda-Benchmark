import pickle
import boto3
import pandas as pd
import time


def reduce(execution_id, n_workers):
    s3 = boto3.client('s3')
    data = s3.get_object(Bucket='pablo-data', Key=f'results/{execution_id}_0')['Body'].read()
    total_time = 0
    t0 = time.time()
    result: pd.Series = pickle.loads(data)
    t1 = time.time()
    total_time += (t1 - t0)

    for n in range(1, n_workers):
        data = s3.get_object(Bucket='pablo-data', Key=f'results/{execution_id}_{n}')['Body'].read()
        t0 = time.time()
        count = pickle.loads(data)
        result = result.add(count, fill_value=0)
        t1 = time.time()
        total_time += (t1 - t0)

    t0 = time.time()
    result = result.sort_values(ascending=False)
    t1 = time.time()
    total_time += (t1 - t0)

    s3.put_object(Bucket='pablo-data', Key=f'times/{execution_id}_reduce2_time',
                  Body=pickle.dumps(total_time))
    return result
