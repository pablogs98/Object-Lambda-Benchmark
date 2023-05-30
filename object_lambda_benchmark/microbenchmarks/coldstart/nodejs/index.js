const AWS = require('aws-sdk');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

exports.handler = async function (event) {
    const PROTO_PATH = './time_server.proto';

    // Suggested options for similarity to existing grpc.load behavior
    var packageDefinition = protoLoader.loadSync(
        PROTO_PATH,
        {
            keepCase: true,
            longs: String,
            enums: String,
            defaults: true,
            oneofs: true
        });
    var timeServer = grpc.loadPackageDefinition(packageDefinition);
    // The protoDescriptor object has the full package hierarchy

    const {getObjectContext} = event;

    var timeServerAddress;
    var functionId;

    if (getObjectContext !== undefined) {
        const params = JSON.parse(event.userRequest.headers["X-Function-Params"])
        timeServerAddress = params.time_server_address;
        functionId = params.function_id;
    } else {
        timeServerAddress = event.time_server_address;
        functionId = event.function_id;
    }

    // Get coldstart here
    const client = new timeServer.Time(timeServerAddress + ':50051', grpc.credentials.createInsecure());
    var timeServerParams = {
        id: functionId,
    };

    function callbackFunction() {
    }

    client.writeTime(timeServerParams, callbackFunction);

    if (getObjectContext !== undefined) {
        const s3 = new AWS.S3();

        const {outputRoute, outputToken, _} = getObjectContext;

        await s3.writeGetObjectResponse({
            RequestRoute: outputRoute,
            RequestToken: outputToken,
            Body: "200",
        }).promise();
    }

    return {statusCode: 200};
};