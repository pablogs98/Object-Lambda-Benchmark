import json

import boto3

s3 = boto3.client('s3')

params = {'buffer_size': 128}


def _add_header(request, **kwargs):
    request.headers.add_header('X-Function-Params', json.dumps(params))


event_system = s3.meta.events
event_system.register_first('before-sign.s3.GetObject', _add_header)

resp = s3.get_object(Bucket='arn:aws:s3-object-lambda:us-east-2:786929956471:accesspoint/pablo-streaming-nop-python-ap',
                     Key='txt/1mb.txt')
r = resp['Body'].read()
print(r)
