import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
import json
import time
import os
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('player-state')
prune_duration_secs = int(os.getenv('DELETE_OLDER_THAN_SECS', '300')) # default to 5 minutes

def timenow_millis():
    return int(round(time.time() * 1000))

def clean_mia_player(player, expected_age):
    print("{} hasn't been seen for a while (since {}, time now {} (age {}s))- marking as MIA".format(player, expected_age, timenow_millis(), (timenow_millis()-expected_age)/1000))
    try:
        response = table.delete_item(
            Key={
                'playerId': player,
            },
            ConditionExpression="attribute_not_exists(lastUpdated) OR lastUpdated <= :timestamp",
            ExpressionAttributeValues={
                ':timestamp': expected_age
            }
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print("Player has returned - newer record exists in the table!")
        else:
            raise

def clean_mia_players():
    # delete player records with a last updated time of before now - DELETE_OLDER_THAN_SECS environment variable
    now = timenow_millis()
    prune_older_than = now - (prune_duration_secs * 1000)
    print("Removing records older than {} ({} seconds ago)".format(prune_older_than,prune_duration_secs))

    fe = Key('lastUpdated').lt(prune_older_than) & Attr('levelId').exists();
    pe = "#player, lastUpdated"
    ean = { "#player": "playerId", }
    esk = None

    players_pruned = 0

    response = table.scan(
        FilterExpression=fe,
        ProjectionExpression=pe,
        ExpressionAttributeNames=ean
    )

    for i in response['Items']:
        clean_mia_player(i['playerId'], i['lastUpdated'])
        players_pruned += 1

    while 'LastEvaluatedKey' in response:
        response = table.scan(
            ProjectionExpression=pe,
            FilterExpression=fe,
            ExpressionAttributeNames= ean,
            ExclusiveStartKey=response['LastEvaluatedKey']
            )

        for i in response['Items']:
            clean_mia_player(i['playerId'], i['lastUpdated'])
            players_pruned += 1

    return players_pruned

def lambda_handler(event, context):
    total = clean_mia_players()
    msg = "{} players MIA".format(total)
    print(msg)
    return msg
