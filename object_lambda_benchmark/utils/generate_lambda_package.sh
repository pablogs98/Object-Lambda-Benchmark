pip3 install --target ./package -r requirements.txt
cd package
zip -r ../deployment-package.zip . -9
cd ..
zip -g deployment-package.zip lambda_function.py -9
zip -g deployment-package.zip time_server_pb2.py -9
zip -g deployment-package.zip time_server_pb2_grpc.py -9
zip -g deployment-package.zip -r time_server -9
