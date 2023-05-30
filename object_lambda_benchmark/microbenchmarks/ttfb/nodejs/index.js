const {S3} = require('aws-sdk');
const axios = require('axios').default;

exports.handler = async (event) => {
    const s3 = new S3();

    const {getObjectContext} = event;
    if (getObjectContext === undefined) {
        var params = {
            'Bucket': event.bucket,
            'Key': event.object_key
        };

        const response = await s3.getObject(params).promise();
        const object_ = response.Body;
        // console.info("Event:\n" + JSON.stringify(event, null, 2))

        if (params.Key == "data/10mb.txt" || params.Key == "data/100mb.txt") {
            params.Body = object_;
            params.Key = "results/ttfb_lambda_result";
            s3.putObject(params);

        } else {
            return new Array(1024 + 1).join("a").toString();
        }
    } else {
        const {outputRoute, outputToken, inputS3Url} = getObjectContext;

        const {data} = await axios.get(inputS3Url, {responseType: 'arraybuffer'});

        await s3.writeGetObjectResponse({
            RequestRoute: outputRoute,
            RequestToken: outputToken,
            Body: data,
        }).promise();
    }

    return {statusCode: 200};
}