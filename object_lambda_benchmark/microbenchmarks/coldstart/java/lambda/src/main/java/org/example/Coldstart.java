package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.LinkedHashMap;


public class Coldstart {

    public String handleRequest(LinkedHashMap<String, String> event, Context context) {
        String timeServerAddress = event.get("time_server_address");
        String functionId = event.get("function_id");

        LambdaLogger logger = context.getLogger();

        String target = timeServerAddress + ":50051";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        TimeGrpc.TimeStub stub = TimeGrpc.newStub(channel);
        TimeServer.TimeRequest request = TimeServer.TimeRequest.newBuilder().setId(functionId).build();

        StreamObserver<Empty> responseObserver = new StreamObserver<>() {

            @Override
            public void onNext(Empty value) {

            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        };


        stub.writeTime(request, responseObserver);

        // log execution details
        logger.log("target: " + target);
        logger.log("function_id: " + functionId);

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return "200 OK";
    }
}
