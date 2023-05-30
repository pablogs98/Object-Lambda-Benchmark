import re
from io import StringIO

import boto3


def lambda_handler(event, context):
    bucket = event['bucket']
    object_key = event['object_key']
    s3_results_key = event['s3_results_key']
    grep_string = event['grep_string']

    s3 = boto3.client('s3')
    object_ = s3.get_object(Bucket=bucket, Key=object_key)['Body'].read().decode('utf-8')

    regex = re.compile(rf'\b{grep_string}\b', re.I)
    found_lines = ""

    lines = object_.split('\n')
    for line in lines:
        line = line.split('\n')[0]
        if regex.search(line):
            found_lines = found_lines + line + '\n'

    if len(found_lines) <= 6 * 1024 * 1024:
        return found_lines
    else:
        s3.put_object(Bucket=bucket, Key=s3_results_key, Body=found_lines)
        return 200
