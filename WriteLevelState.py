import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
import json
from cStringIO import StringIO
import uuid
import datetime

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

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('level-state')
s3_client = boto3.client('s3')

def get_level_states():
    print("get the level states");

    fe = Attr('playerCount').gt(0);
    pe = "#level, playerCount"
    ean = { "#level": "levelId", }
    esk = None

    levels = {}
    levels_found = 0

    response = table.scan(
        FilterExpression=fe,
        ProjectionExpression=pe,
        ExpressionAttributeNames=ean,
        ConsistentRead = True
    )

    for i in response['Items']:
        levels[i['levelId']] = i['playerCount']
        levels_found += 1

    while 'LastEvaluatedKey' in response:
        response = table.scan(
            ProjectionExpression=pe,
            FilterExpression=fe,
            ExpressionAttributeNames= ean,
            ExclusiveStartKey=response['LastEvaluatedKey'],
            ConsistentRead = True
            )

        for i in response['Items']:
            levels[i['levelId']] = i['playerCount']
            levels_found += 1

    print("{} level(s) found".format(levels_found))
    return levels

def write_level_states(json_levels):
    bucket_name = "sp-codecombat-level-state"
    file_name = "level_information.json"
    fake_handle = StringIO(json_levels)
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=fake_handle.read())

def lambda_handler(event, context):
    levels = get_level_states()
    # also add the meta information in here
    # update_time as iso8601
    # update_id as uuid
    # interval_secs 60
    update = { 'update_time': datetime.datetime.utcnow().replace(microsecond=0).isoformat() + 'Z',
               'update_id' : str(uuid.uuid4()),
               'update_interval_secs': 60,
               'level_player_counts' : levels }
    as_json = json.dumps(update, indent=2, cls=DecimalEncoder)
    print(as_json)
    write_level_states(as_json)
    return as_json
