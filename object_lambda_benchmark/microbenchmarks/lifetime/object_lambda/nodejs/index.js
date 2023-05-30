const {S3} = require('aws-sdk');
const fs = require('fs')
const uuid = require('uuid')

exports.handler = async (event) => {
    const s3 = new S3();

    const {getObjectContext} = event;
    const {outputRoute, outputToken, _} = getObjectContext;

    var instanceId;

    try {
        instanceId = fs.readFileSync('/tmp/instance_id', 'utf8');
    } catch (err) {
        instanceId = uuid.v4();
        fs.writeFileSync('/tmp/instance_id', instanceId);
    }

    await s3.writeGetObjectResponse({
        RequestRoute: outputRoute,
        RequestToken: outputToken,
        Body: instanceId,
    }).promise();

    return {statusCode: 200};
}