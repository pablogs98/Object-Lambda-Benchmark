import time

from object_lambda_benchmark.utils.utils import ObjectLambdaFunction


def _map(time_server_address, function_ids, execution_id, aws_environment_variables, s3ol_access_point_arn, n_workers):
    params = [
        {'n_workers': n_workers, 'function_id': f_id, 'time_server_address': time_server_address,
         'execution_id': execution_id} for f, f_id in enumerate(function_ids)]
    ol_funct = ObjectLambdaFunction(function_name=f"pablo-latency-decomposition-python38",
                                    aws_environment_variables=aws_environment_variables,
                                    s3ol_access_point_arn=s3ol_access_point_arn)
    warm_params = [
        {'function_id': 'ignore', 'time_server_address': time_server_address} for _ in range(len(function_ids))]
    try:
        ol_funct.map(keys=['a'] * len(function_ids), params=warm_params)
    except:
        pass
    time.sleep(30)

    ol_funct.map(keys=['txt/10mb.txt'] * len(function_ids), params=params)
