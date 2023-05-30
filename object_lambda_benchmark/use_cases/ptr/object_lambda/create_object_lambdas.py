from object_lambda_benchmark.utils.utils import ObjectLambdaFunction

object_lambda_function = ObjectLambdaFunction(function_name='pablo-object-lambda-reducer', memory=512)
object_lambda_function.create_function(handler='lambda_function.lambda_handler',
                                       runtime='python3.8',
                                       role='pablo-execution-role',
                                       zip_file_path='reducer/deployment-package.zip',
                                       s3_access_point='pablo-data-ap')

object_lambda_function = ObjectLambdaFunction(function_name='pablo-object-lambda-map', memory=2048)
object_lambda_function.create_function(handler='lambda_function.lambda_handler',
                                       runtime='python3.8',
                                       role='pablo-execution-role',
                                       zip_file_path='word_counter/deployment-package.zip',
                                       s3_access_point='pablo-data-ap')
