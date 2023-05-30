import contextlib
import json
import requests
import boto3
from botocore.config import Config

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
    # Object Lambda
    try:
        object_get_context = event["getObjectContext"]
        request_route = object_get_context["outputRoute"]
        request_token = object_get_context["outputToken"]
        s3_url = object_get_context["inputS3Url"]

        params = json.loads(event['userRequest']['headers']['X-Function-Params'])
        part_number = params["part_number"]
        part_size = params["part_size"]

        range0 = part_number * part_size
        range1 = ((part_number + 1) * part_size) - 1

        range_header = f"bytes={range0}-{range1}"

        s3 = _get_s3_client()
        r = requests.get(s3_url, headers={"Range": range_header}, stream=True)

        with contextlib.closing(r.raw) as file_obj:
            file_obj = _Buffer(file_obj)

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
                Body=file_obj,
                RequestRoute=request_route,
                RequestToken=request_token,
                Metadata=metadata,
                **headers)

    # Get Lambda params
    except KeyError:
        pass


class _Buffer:
    def __init__(self, source_stream, chunk_size=4096):
        self.body = source_stream
        self.chunk_size = chunk_size
        self.object_size = 0

    def read(self, size=None):
        return self.body.read(size)

    def __iter__(self):
        r = self.read(self.chunk_size)
        self.object_size += self.chunk_size
        while r:
            yield r
            self.object_size += self.chunk_size
            r = self.read(self.chunk_size)
        print(self.object_size)
