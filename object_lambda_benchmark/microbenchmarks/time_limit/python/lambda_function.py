import contextlib
import time
import traceback
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
    object_get_context = event["getObjectContext"]
    request_route = object_get_context["outputRoute"]
    request_token = object_get_context["outputToken"]
    s3_url = object_get_context["inputS3Url"]

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

        print("Called write get object response!")


class _Buffer:
    def __init__(self, source_stream, chunk_size=4096 * 1024):
        self.body = source_stream
        self.chunk_size = chunk_size
        self.current_byte = 0

    def read(self, size=None):
        return self.body.read(size)

    def __iter__(self):
        r = self.read(self.chunk_size)
        self.current_byte += self.chunk_size
        while r:
            time.sleep(60)  # Artificial delay. Check whether it will allow Object Lambda to finish or not
            print(f"Yield bytes {self.current_byte - self.chunk_size} to {self.current_byte}")
            yield r
            r = self.read(self.chunk_size)
            self.current_byte += self.chunk_size
