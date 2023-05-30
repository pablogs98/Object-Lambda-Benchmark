import time
import json

from object_lambda_benchmark.utils.utils import ObjectLambdaFunction

KiB = 1024
MiB = KiB * KiB

memory_sizes = [4096]
buffer_sizes = [1 * MiB]
object_sizes = ['100mb']
n_executions = 1

results: dict = {'s3_object_lambda_times': []}

# Change these to run any runtime
runtimes = ['python3.8']
runtimes_no_number = ['python']
lambda_functions = [f'pablo-ttfb-{runtime}' for runtime in runtimes_no_number]
zip_paths = [f'{runtime}/deployment-package.zip' for runtime in runtimes_no_number]
handlers = ["lambda_function.lambda_handler"]

for i in range(len(lambda_functions)):
    olambda_function = ObjectLambdaFunction(function_name=lambda_functions[i], timeout=180)
    olambda_function.create_function(handler=handlers[i], runtime=runtimes[i], role='pablo-execution-role',
                                     zip_file_path=zip_paths[i], s3_access_point='pablo-data-ap')
    time.sleep(5)

    for b_size in buffer_sizes:
        for m, m_size in enumerate(memory_sizes):
            olambda_function.memory = m_size
            olambda_function.update_function()
            time.sleep(10)
            print(f"Updated lambdas' settings!")

            for o_size in object_sizes:
                for n in range(n_executions):
                    print("\n-----------------------------------------------------------------------------------"
                          f"\nBuffer size: {b_size}. Memory size: {m_size}. File size: {o_size}. Execution no. {n}"
                          "\n-----------------------------------------------------------------------------------")

                    # S3 Object Lambda
                    result = olambda_function.invoke_function(params={'buffer_size': b_size}, key=f'txt/{o_size}.txt')
                    print("Sleep 30s, time to manage Wireshark...")
                    time.sleep(20)

                    t_s3olambda = result['ttfb']

                    print(f"S3 Object Lambda done. TTFB: {t_s3olambda}s")

                    results['s3_object_lambda_times'].append(t_s3olambda)

                with open(
                        f'ttfb_stream_object_lambda_{runtimes_no_number[i]}_{m_size}mb_buffer{b_size}_object_size{o_size}.json',
                        'w') as f:
                    json.dump(results, f)

                results['s3_object_lambda_times'] = []
