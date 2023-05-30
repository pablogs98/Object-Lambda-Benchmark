package org.example;

import com.amazonaws.services.lambda.runtime.Context;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.UUID;


public class Lifetime {

    public String handleRequest(LinkedHashMap<String, String> event, Context context) throws IOException {
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
        return instanceId;
    }
}
