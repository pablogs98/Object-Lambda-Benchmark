import sys
from concurrent import futures

import grpc

import time_server_pb2_grpc
import time


class TimestampRecorder(time_server_pb2_grpc.TimeServicer):

    def WriteTime(self, request, context):
        with open(f"timestamp-{request.id}.txt", 'a') as timestamp_file:
            timestamp = time.time()
            timestamp_file.write(f"{timestamp}\n")
            print(f"Received WriteTime RPC from id {request.id}! Timestamp written: {timestamp}")
        return time_server_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()


def serve(address, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    time_server_pb2_grpc.add_TimeServicer_to_server(
        TimestampRecorder(), server)
    server.add_insecure_port(f'{address}:{port}')
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    address = sys.argv[1]
    port = sys.argv[2] if len(sys.argv) >= 4 else 50051
    serve(address, port)
