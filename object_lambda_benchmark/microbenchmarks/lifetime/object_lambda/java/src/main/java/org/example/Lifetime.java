package org.example;

import com.amazonaws.services.lambda.runtime.events.S3ObjectLambdaEvent;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.WriteGetObjectResponseRequest;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class Lifetime {
    public void handleRequest(S3ObjectLambdaEvent event, Context context) throws Exception {
        AmazonS3 s3Client = AmazonS3Client.builder().build();

        String instanceId;
        try {
            BufferedReader iidFile
                    = new BufferedReader(new FileReader("/tmp/instance_id"));
            instanceId = iidFile.readLine();
        } catch (IOException e) {
            BufferedWriter iidFile = new BufferedWriter(new FileWriter("/tmp/instance_id"));
            instanceId = UUID.randomUUID().toString().replace("-", "");
            iidFile.write(instanceId);
        }
        var bodyStream = new ByteArrayInputStream(instanceId.getBytes(StandardCharsets.UTF_8));

        // Stream the bytes back to the caller.
        s3Client.writeGetObjectResponse(new WriteGetObjectResponseRequest()
                .withRequestRoute(event.outputRoute())
                .withRequestToken(event.outputToken())
                .withInputStream(bodyStream));
    }
}