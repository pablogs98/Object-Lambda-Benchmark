const fs = require('fs')
const uuid = require('uuid')

exports.handler = async function (event) {

    try {
        const instanceId = fs.readFileSync('/tmp/instance_id', 'utf8');
    } catch (err) {
        const instanceId = uuid.v4();
        fs.writeFileSync('/tmp/instance_id', instanceId);
    }

    return {instance_id: instanceId};
};