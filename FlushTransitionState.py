import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import decimal
from cStringIO import StringIO
import uuid
import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('transitions')
s3_client = boto3.client('s3')

# Convert "decimal" to json - from AWS examples
# http://docs.aws.amazon.com/amazondynamodb/latest/gettingstartedguide/GettingStarted.Python.03.html
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def empty_transition_table(keys):
    # empty the table here
    for key in keys:
        print("Deleting transition information for '{}'".format(key))
        response = table.delete_item(
            Key={
                'transitionLevels': key,
            }
        )
        print("{}".format(response))

    # return the number of records deleted
    return len(keys)

def make_key(from_level, to_level):
    f=from_level
    t=to_level

    if from_level is None:
        f = ""
    if to_level is None:
        t = ""

    return "{}/{}".format(f, t)

def get_transition_table():
    pe = "#from, #to, #tot"
    ean = { "#from": "levelFrom", "#to": "levelTo", "#tot": "count" }

    response = table.scan(
        ProjectionExpression=pe,
        ExpressionAttributeNames=ean,
        ConsistentRead=True
    )

    recs = []
    keys = []

    # get all the records
    # NB that dynamodb scans return "pages", which is why we have to repeatedly
    # call this in a while loop
    for i in response['Items']:
        row = { "from": i.get('levelFrom'), "to": i.get('levelTo'), "count": i['count'] }
        recs.append(row)
        keys.append(make_key(row['from'], row['to']))

    while 'LastEvaluatedKey' in response:
        response = table.scan(
            ProjectionExpression=pe,
            ExpressionAttributeNames=ean,
            ExclusiveStartKey=response['LastEvaluatedKey'],
            ConsistentRead=True
            )

        for i in response['Items']:
            row = { "from": i.get('levelFrom'), "to": i.get('levelTo'), "count": i['count'] }
            recs.append(row)
            keys.append(make_key(row['from'], row['to']))

    # return the records as an array of dictionaries (rows)
    # but also return the dynamodb primary keys of each - so we can remove them
    return keys, recs

def write_json_to_s3(json):
    # write the json string to s3
    print(json)
    bucket_name = "sp-codecombat-level-state"
    file_name = "transition_information.json"
    fake_handle = StringIO(json)
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=fake_handle.read())
    return True

def lambda_handler(event, context):
    keys, transition_table = get_transition_table()

    update = { 'update_time': datetime.datetime.utcnow().replace(microsecond=0).isoformat() + 'Z',
               'update_id' : str(uuid.uuid4()),
               'update_interval_secs': 60,
               'transitions' : transition_table }

    update_json = json.dumps(update, indent=2, cls=DecimalEncoder)

    write_json_to_s3(update_json)
    recs = empty_transition_table(keys)

    return "{}".format(update_json)
