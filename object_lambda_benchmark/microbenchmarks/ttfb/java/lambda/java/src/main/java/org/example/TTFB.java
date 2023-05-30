package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Arrays;


// Lambda only version
public class TTFB {

    public String handleRequest(LinkedHashMap<String, String> event, Context context) {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
        // LambdaLogger logger = context.getLogger();
        String response = "200 OK";

        String bucket = event.get("bucket");
        String key = event.get("object_key");
        S3Object object = s3.getObject(bucket, key);

        try {
            byte[] all = object.getObjectContent().readAllBytes();
            if (key.equals("data/10mb.txt") || key.equals("data/100mb.txt")) {
                s3.putObject(bucket, "results/ttfb_lambda_result", new String(all));
            } else {
                char[] chars = new char[1024];
                Arrays.fill(chars, 'a');
                return new String(chars);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }
}