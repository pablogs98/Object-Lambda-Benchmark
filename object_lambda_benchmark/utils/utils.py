import re
import asyncio

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, ParamValidationError

from object_lambda_benchmark.utils.aws_request import s3ol_get, lambda_invoke, s3_get


class LambdaFunction:
    def __init__(self, function_name, memory=512, timeout=60, region="us-east-2", aws_environment_variables=None):
        self.function_name = function_name.replace("_", "-")
        regex = re.compile(r"^[a-zA-Z0-9\-]*$")
        if not regex.match(self.function_name):
            raise Exception("function_name does not match pattern ^[a-zA-Z0-9\\-]*$")
        self.memory = memory
        self.timeout = timeout
        self.region = region
        self.account_id = boto3.client('sts').get_caller_identity().get('Account')
        self.aws_environment_variables = aws_environment_variables

    def get_boto3_config(self):
        return Config(region_name=self.region)

    def create_function(self, handler, runtime, role, zip_file_path, *args, **kwargs):
        client = boto3.client("lambda", config=self.get_boto3_config())
        with open(zip_file_path, 'rb') as z:
            try:
                response = client.create_function(FunctionName=self.function_name,
                                                  Code={'ZipFile': z.read()},
                                                  Handler=handler,
                                                  Runtime=runtime,
                                                  Role=f"arn:aws:iam::{self.account_id}:role/{role}",
                                                  Timeout=self.timeout,
                                                  MemorySize=self.memory)
                return response['FunctionArn']
            except client.exceptions.ResourceConflictException:
                print("Function exists. Removing and trying again...")
                self.delete_function()
                # Don't want to use the subclasses' method.
                return LambdaFunction.create_function(self, handler, runtime, role, zip_file_path)

    def delete_function(self):
        client = boto3.client("lambda", config=self.get_boto3_config())
        try:
            client.delete_function(FunctionName=self.function_name)
        except client.exceptions.ResourceNotFoundException as e:
            print("Lambda function not found")

    def invoke_function(self, params: dict = "", *args, **kwargs):
        return asyncio.run(lambda_invoke(self.function_name, self.region, params, self.aws_environment_variables))

    def invoke_and_s3_get(self, *args, **kwargs):
        invoke_result = asyncio.run(self.invoke_function(*args, **kwargs))
        s3_results_key = kwargs.get("s3_results_key")
        result = asyncio.run(s3_get("pablo-data", s3_results_key, self.region, self.aws_environment_variables))
        result["ttfb"] += invoke_result["total_time"]
        result["total_time"] += invoke_result["total_time"]
        return result

    def update_function(self):
        client = boto3.client("lambda", config=self.get_boto3_config())
        client.update_function_configuration(FunctionName=self.function_name,
                                             MemorySize=self.memory,
                                             Timeout=self.timeout)

    def map(self, keys, bucket=None, params=None, verbose=True):
        if params is None:
            params = []
        for key in keys:
            params.append({'object_key': key, 'bucket': bucket})

        async def _map():
            return await asyncio.gather(
                *[lambda_invoke(self.function_name, self.region, param, self.aws_environment_variables, verbose=verbose)
                  for param in params])

        invoke_results = asyncio.run(_map())

        return invoke_results


class ObjectLambdaFunction(LambdaFunction):
    def __init__(self, function_name, memory=512, timeout=60, region="us-east-2", s3ol_access_point_arn=None,
                 aws_environment_variables=None):
        super().__init__(function_name, memory, timeout, region, aws_environment_variables)
        self.s3ol_access_point = s3ol_access_point_arn

    def create_function(self, handler, runtime, role, zip_file_path, s3_access_point):
        function_arn = super().create_function(handler, runtime, role, zip_file_path)
        s3control = boto3.client('s3control', config=self.get_boto3_config())

        configuration = {
            "SupportingAccessPoint": f"arn:aws:s3:{self.region}:{self.account_id}:accesspoint/{s3_access_point}",
            "TransformationConfigurations": [
                {
                    "Actions": ["GetObject"],
                    "ContentTransformation": {
                        "AwsLambda": {
                            "FunctionArn": function_arn,
                        }
                    }
                }
            ]
        }

        response = None
        try:
            response = s3control.create_access_point_for_object_lambda(AccountId=self.account_id,
                                                                       Name=f"{self.function_name}-ap",
                                                                       Configuration=configuration)
        except ClientError as e:
            if "AccessPointAlreadyOwnedByYou" in str(e):
                print("S3 Object Lambda access point already exists. Removing and tryign again...")
                s3control.delete_access_point_for_object_lambda(AccountId=self.account_id,
                                                                Name=f"{self.function_name}-ap")
            else:
                raise e
        except ParamValidationError as e:
            if "FunctionArn, value: None" in str(e):
                pass
            else:
                raise e

        self.s3ol_access_point = f"{self.function_name}-ap-{self.account_id}"

        return function_arn, response

    def delete_function(self):
        super().delete_function()
        s3control = boto3.client('s3control', config=self.get_boto3_config())
        try:
            s3control.delete_access_point_for_object_lambda(AccountId=self.account_id,
                                                            Name=f"{self.function_name}-ap")
        except ClientError as e:
            if "NoSuchAccessPoint" in str(e):
                print("Cannot delete S3 Object Lambda access point. It does not exist.")
            else:
                raise e

        self.s3ol_access_point = None

    def invoke_function(self, params: dict = None, key=None):
        if self.s3ol_access_point is None:
            raise Exception(
                "Cannot invoke Object Lambda: S3 Object Lambda Endpoint ARN (s3ol_access_point_arn) is None.")

        return asyncio.run(s3ol_get(self.s3ol_access_point, key, self.region, params, self.aws_environment_variables))

    def map(self, keys, params=None, verbose=True, *args, **kwargs):
        async def _map():
            if params is None:
                results = asyncio.gather(
                    *[s3ol_get(bucket=self.s3ol_access_point,params=params, key=key, region=self.region,
                               aws_environment_variables=self.aws_environment_variables, verbose=verbose) for key in keys])
            else:
                results = asyncio.gather(
                        *[s3ol_get(bucket=self.s3ol_access_point, params=params[k], key=key, region=self.region,
                                   aws_environment_variables=self.aws_environment_variables, verbose=verbose) for k, key in
                          enumerate(keys)])

            return await results

        return asyncio.run(_map())

    def create_access_point(self, name, bucket):
        try:
            s3control = boto3.client('s3control', config=self.get_boto3_config())
            s3control.create_access_point(AccountId=self.account_id,
                                          Name=name,
                                          Bucket=bucket)
        except ClientError as e:
            if "AccessPointAlreadyOwnedByYou" in str(e):
                print("Access point already exists.")
            else:
                raise e
