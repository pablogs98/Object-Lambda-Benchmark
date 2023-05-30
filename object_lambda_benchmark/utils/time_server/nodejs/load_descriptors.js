const PROTO_PATH = '../protos/time_server.proto';
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

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
console.log(timeServer)
// The protoDescriptor object has the full package hierarchy


const client = new timeServer.Time('127.0.0.1:50051', grpc.credentials.createInsecure());

console.log(client)

var params = {
    id: "a",
};

function callbackFunction() {

}

client.writeTime(params, callbackFunction);
