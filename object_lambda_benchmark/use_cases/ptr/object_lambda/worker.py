import json
import pickle
import time

import aioboto3
import boto3
import pandas as pd
import asyncio

from aiobotocore.response import StreamingBody


async def word_count(s3ol_access_point, keys, params, do_l_counts=False):
    t0 = time.time()

    async def _wc(key, params):
        l_counts = []
        count = pd.Series(dtype='float64')
        data_size = 0
        session = aioboto3.Session()
        async with session.client("s3") as s3:
            event_system = s3.meta.events

            def _add_header(request, **kwargs):
                request.headers.add_header('X-Function-Params', json.dumps(params))

            event_system.register_first('before-sign.s3.GetObject', _add_header)
            response = await s3.get_object(Bucket=s3ol_access_point, Key=key)
            stream = response['Body']
            stream = StreamingBody(raw_stream=stream, content_length=None)
            try:
                pending = b''
                async for chunk in stream.iter_chunks(1024):
                    data_size += len(chunk)
                    lines = (pending + chunk).split(b'\r\r')
                    for line in lines[:-1]:
                        data = line.split(b'\r\r')[0]
                        dc = pickle.loads(data)
                        count = count.add(dc, fill_value=0)
                        if do_l_counts:
                            l_counts.append((dc, time.time() - t0))
                    pending = lines[-1]
                if pending:
                    data = pending.split(b'\r\r')[0]
                    dc = pickle.loads(data)
                    count = count.add(dc, fill_value=0)
                    if do_l_counts:
                        l_counts.append((dc, time.time() - t0))
            except Exception as e:
                print(e)
            finally:
                stream.close()

        return count, data_size, l_counts

    return await asyncio.gather(*[_wc(key, params[k]) for k, key in enumerate(keys)])


def worker(bucket, keys, function_params, function_name, n_workers, worker_id, execution_id, sync,
           s3ol_access_point_arn, aws_environment_variables, do_l_counts):
    s3 = boto3.client('s3')

    print(f"Worker {worker_id} started!")

    print("AWS variables check: ")
    print(aws_environment_variables)

    # Synchronize functions
    if sync:
        s3.put_object(Bucket=bucket, Key=f'{execution_id}/{worker_id}')

        while len(s3.list_objects(Bucket=bucket, Prefix=f'{execution_id}/')) != n_workers:
            time.sleep(1)

    results = asyncio.run(
        word_count(s3ol_access_point_arn, keys, function_params, do_l_counts))

    result = pd.Series(dtype='float64')
    data_size = 0
    l_counts = []
    for r, ds, lc in results:
        result = result.add(r, fill_value=0)
        data_size += ds
        l_counts.append(lc)
    result = result.sort_values(ascending=False)

    return result, data_size, l_counts
