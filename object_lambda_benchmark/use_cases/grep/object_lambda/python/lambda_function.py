import json
import contextlib
import traceback
import requests
import boto3
from botocore.config import Config
import re

class Grep:
    def __init__(self, source_stream, grep_string):
        self.body = source_stream
        self.grep_string = grep_string

    def read(self, size=None):
        return self.body.read(size)

    def __iter__(self):
        r = self.read(1048576)
        pending = ""
        found_lines = ""
        regex = re.compile(rf'\b{self.grep_string}\b', re.I)
        while r:
            pending += r.decode('utf-8')
            lines = pending.split('\n')
            for line in lines[:-1]:
                line = line.split('\n')[0]
                if regex.search(line):
                    found_lines = found_lines + line + '\n'
            pending = lines[-1]
            if found_lines != "":
                yield found_lines
                found_lines = ""
            r = self.read(1048576)
        if pending:
            line = pending.split('\n')[0]
            if self.grep_string in line:
                found_lines = found_lines + line + '\n'

            if found_lines != "":
                yield found_lines


LAMBDA_ERROR_CODE = 500


def _if_not_none(value, f):
    if value is not None:
        return f(value)


def _get_s3_client():
    config = Config(signature_version='s3v4', s3={
        'payload_signing_enabled': False
    })
    return boto3.client('s3', verify=False, config=config)


def lambda_handler(event, context):
    object_get_context = event["getObjectContext"]
    request_route = object_get_context["outputRoute"]
    request_token = object_get_context["outputToken"]
    s3_url = object_get_context["inputS3Url"]

    params = json.loads(event['userRequest']['headers']['X-Function-Params'])
    grep_string = params["grep_string"]

    s3 = _get_s3_client()
    try:
        r = requests.get(s3_url, stream=True)
        if r.status_code != 200:
            s3.write_get_object_response(StatusCode=r.status_code, RequestRoute=request_route,
                                         RequestToken=request_token)
            return

    except requests.exceptions.RequestException as e:
        # An error on our side
        print(e)
        traceback.print_exc()
        s3.write_get_object_response(StatusCode=LAMBDA_ERROR_CODE, RequestRoute=request_route,
                                     RequestToken=request_token)
        return
    with contextlib.closing(r.raw) as file_obj:
        decompressed_file_obj = Grep(file_obj, grep_string)

        all_headers = {
            "ErrorMessage": r.headers.get("x-amz-fwd-error-message"),
            "ContentLanguage": r.headers.get("Content-Language"),
            "MissingMeta": r.headers.get("x-amz-missing-meta"),
            "ObjectLockLegalHoldStatus": r.headers.get("x-amz-object-lock-legal-hold"),
            "ReplicationStatus": r.headers.get("x-amz-replication-status"),
            "RequestCharged": r.headers.get("x-fwd-header-x-amz-request-charged"),
            "Restore": r.headers.get("x-amz-restore"),
            "ServerSideEncryption": r.headers.get("x-amz-server-side-encryption"),
            "SSECustomerAlgorithm": r.headers.get("x-amz-server-side-encryption-customer-algorithm"),
            "SSEKMSKeyId": r.headers.get("x-amz-server-side-encryption-aws-kms-key-id"),
            "SSECustomerKeyMD5": r.headers.get("x-amz-server-side-encryption-customer-key-MD5"),
            "StorageClass": r.headers.get("x-amz-storage-class"),
            "TagCount": _if_not_none(r.headers.get("x-amz-tagging-count"), int),
            "VersionId": r.headers.get("x-amz-version-id")
        }

        headers = {k: v for k, v in all_headers.items() if v is not None}

        metadata = {k[11:]: v for k, v in r.headers.items() if k.startswith("x-amz-meta-")}

        s3.write_get_object_response(
            Body=decompressed_file_obj,
            RequestRoute=request_route,
            RequestToken=request_token,
            Metadata=metadata,
            **headers)
