package org.example;

import com.amazonaws.services.lambda.runtime.events.S3ObjectLambdaEvent;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.WriteGetObjectResponseRequest;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.ByteArrayInputStream;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Coldstart {
    public void handleRequest(S3ObjectLambdaEvent event, Context context) throws Exception {
        // gRPC
        String params_raw = event.getUserRequest().getHeaders().get("X-Function-Params");

        GsonBuilder builder = new GsonBuilder();
        Gson gson = builder.create();

        Map params = gson.fromJson(params_raw, Map.class);

        String timeServerAddress = params.get("time_server_address").toString();
        String functionId = params.get("function_id").toString();

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

        AmazonS3 s3Client = AmazonS3Client.builder().build();
        var bodyStream = new ByteArrayInputStream(new byte[]{2, 0, 0});

        // Stream the bytes back to the caller.
        s3Client.writeGetObjectResponse(new WriteGetObjectResponseRequest()
                .withRequestRoute(event.outputRoute())
                .withRequestToken(event.outputToken())
                .withInputStream(bodyStream));
    }


}
