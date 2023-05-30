import uuid
import os


def lambda_handler(event, context):
    if not os.path.isfile('/tmp/instance_id'):
        with open('/tmp/instance_id', 'w') as iid_file:
            instance_id = uuid.uuid4().hex
            iid_file.write(instance_id)
    else:
        with open('/tmp/instance_id', 'r') as iid_file:
            instance_id = iid_file.read()

    return instance_id
