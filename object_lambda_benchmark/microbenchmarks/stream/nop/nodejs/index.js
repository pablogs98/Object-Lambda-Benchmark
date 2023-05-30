const {S3} = require('aws-sdk');
const axios = require('axios').default;

const http = require('http');
const https = require('https');

// This is needed to be able to configure highWaterMark in axios
class MyHttpAgent extends http.Agent {
    constructor(buffer_size) {
        super({});
        this.buffer_size = buffer_size;
    }

    createConnection(options, callback) {
        options.highWaterMark = this.buffer_size;
        // @ts-ignore
        return super.createConnection(options, callback);
    }
}

class MyHttpsAgent extends https.Agent {
    constructor(buffer_size) {
        super({});
        this.buffer_size = buffer_size;
    }

    createConnection(options, callback) {
        options.highWaterMark = this.buffer_size;
        // @ts-ignore
        return super.createConnection(options, callback);
    }
}

// --------------------------------------------------------------

exports.handler = async (event) => {
    const s3 = new S3();

    const {getObjectContext} = event;
    const {outputRoute, outputToken, inputS3Url} = getObjectContext;

    const params = JSON.parse(event.userRequest.headers["X-Function-Params"])
    const buffer_size = parseInt(params["buffer_size"])

    await axios({
        method: 'GET',
        url: inputS3Url,
        responseType: 'stream',
        httpAgent: new MyHttpAgent(buffer_size),
        httpsAgent: new MyHttpsAgent(buffer_size)
    }).then(
        response => response.data
    ).then(
        // Finally send the gzip-ed stream back to the client.
        stream => s3.writeGetObjectResponse({
            RequestRoute: outputRoute,
            RequestToken: outputToken,
            Body: stream,
            ContentType: "text/plain",
        }).promise()
    );

    // Gracefully exit the Lambda function.
    return {statusCode: 200};
}