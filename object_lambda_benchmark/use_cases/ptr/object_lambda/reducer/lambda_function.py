import json
import asyncio
import aioboto3
import aiostream
import boto3
from aiobotocore.response import StreamingBody
from botocore.config import Config

LAMBDA_ERROR_CODE = 500


def iter_over_async(ait, loop):
    ait = ait.__aiter__()

    async def get_next():
        try:
            obj = await ait.__anext__()
            return False, obj
        except StopAsyncIteration:
            return True, None

    while True:
        done, obj = loop.run_until_complete(get_next())
        if done:
            break
        yield obj


async def stream_generator(s3ol_access_point, key, params):
    session = aioboto3.Session()
    print(f"Created generator on S3OL-AP: {s3ol_access_point}, key: {key}, params: {params}")

    def _add_header(request, **kwargs):
        request.headers.add_header('X-Function-Params', json.dumps(params))

    async with session.client("s3") as s3:
        event_system = s3.meta.events
        event_system.register_first('before-sign.s3.GetObject', _add_header)

        response = await s3.get_object(Bucket=s3ol_access_point, Key=key)
        stream = response['Body']
        stream = StreamingBody(raw_stream=stream, content_length=None)
        try:
            pending = b''
            async for chunk in stream.iter_chunks(1024):
                lines = (pending + chunk).split(b'\r\r')
                for line in lines[:-1]:
                    yield line.split(b'\r\r')[0]
                pending = lines[-1]
            if pending:
                yield pending.split(b'\r\r')[0]
        finally:
            stream.close()

        print(f"Finished {key}!")


def _get_s3_client():
    config = Config(signature_version='s3v4', s3={
        'payload_signing_enabled': False
    })
    return boto3.client('s3', verify=False, config=config)


def lambda_handler(event, context):
    object_get_context = event["getObjectContext"]
    request_route = object_get_context["outputRoute"]
    request_token = object_get_context["outputToken"]

    params = json.loads(event['userRequest']['headers']['X-Function-Params'])
    count_window = int(params["count_window"])
    s3ol_access_points = params["s3ol_access_points"]
    keys = params["keys"]
    function_params = params["function_params"]

    s3 = _get_s3_client()

    generators = map(stream_generator, s3ol_access_points, keys, function_params)

    stream = aiostream.stream.merge(*generators)
    loop = asyncio.get_event_loop()

    file_obj = Reducer(iter_over_async(stream, loop), count_window=count_window)

    s3.write_get_object_response(
        Body=file_obj,
        RequestRoute=request_route,
        RequestToken=request_token)


class Reducer:
    def __init__(self, source_stream, count_window=100):
        self.stream = source_stream
        self.count_window = count_window

    def read(self):
        return self.stream.__next__()

    def __iter__(self):
        for chunk in self.stream:
            yield chunk + b'\r\r'

        print("Finished all!")
