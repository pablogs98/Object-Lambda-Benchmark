import pickle
import time

import pandas as pd

import boto3


def map_(bucket, key, results_key, n_workers, worker_id, execution_id, sync, aws_environment_variables):
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_environment_variables['AWS_ACCESS_KEY_ID'],
                      aws_secret_access_key=aws_environment_variables['AWS_SECRET_ACCESS_KEY']
                      )

    # Synchronize functions
    if sync:
        s3.put_object(Bucket=bucket, Key=f'{execution_id}/{worker_id}', Body="")

        while len(s3.list_objects(Bucket=bucket, Prefix=f'{execution_id}/')) != n_workers:
            time.sleep(1)

        s3.delete_object(Bucket=bucket, Key=f'{execution_id}/{worker_id}')

    sr = pd.read_csv(f's3://{bucket}/{key}', sep=',', usecols=[5], header=None, engine='c').squeeze().astype("category")
    t0 = time.time()
    counts = sr.value_counts(dropna=True)
    t1 = time.time()
    s3.put_object(Bucket='pablo-data', Key=f'results/{execution_id}_{results_key}_{worker_id}',
                  Body=pickle.dumps(counts))

    if worker_id == 0:
        s3.put_object(Bucket='pablo-data', Key=f'times/{execution_id}_map_time',
                      Body=pickle.dumps(t1 - t0))
