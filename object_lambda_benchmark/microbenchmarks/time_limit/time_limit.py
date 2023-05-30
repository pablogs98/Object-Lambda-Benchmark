import json
import time

from object_lambda_benchmark.utils.utils import LambdaFunction, ObjectLambdaFunction

object_lambda_func = ObjectLambdaFunction("pablo-time-limit", timeout=600)
object_lambda_func.create_function("lambda_function.lambda_handler", "python3.8", 'pablo-execution-role',
                                   f'./python/deployment-package.zip', 'pablo-data-ap')
time.sleep(5)

# Call Object Lambda. See if the response is OK even though we added an artificial delay
result = object_lambda_func.invoke_function(key="data/10mb.txt")
try:
    result = json.loads(result['body'])
    print(result)
except:
    print("Stream did not finish, 60s limit reached")
