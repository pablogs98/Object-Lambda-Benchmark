import zlib
import boto3


def lambda_handler(event, context):
    bucket = event['bucket']
    object_key = event['object_key']
    s3_results_key = event['s3_results_key']

    s3 = boto3.client('s3')
    object_ = s3.get_object(Bucket=bucket, Key=object_key)['Body'].read()

    decompressed_object = zlib.decompress(object_)

    if len(decompressed_object) <= 6 * 1024 * 1024:
        return decompressed_object
    else:
        s3.put_object(Bucket=bucket, Key=s3_results_key, Body=decompressed_object)
        return 200
