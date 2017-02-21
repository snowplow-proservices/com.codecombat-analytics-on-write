from __future__ import print_function

import json
import boto3

print('Loading function')

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('level-state')
transitions_table = dynamodb.Table('transitions')

def get_level_changes(record_change):
    old_level = None
    new_level = None

    if 'OldImage' in record_change:
        if 'levelId' in record_change['OldImage']:
            old_level = record_change['OldImage']['levelId']["S"]

    if 'NewImage' in record_change:
        if 'levelId' in record_change['NewImage']:
            new_level = record_change['NewImage']['levelId']["S"]

    return (old_level, new_level)

def increment_level(level):
    print("increase player count in " + level + " by one")
    response = table.update_item(Key={'levelId': level}, UpdateExpression="set playerCount = if_not_exists(playerCount, :initial) + :val", ExpressionAttributeValues={':val': 1, ':initial' : 0 }, ReturnValues="UPDATED_NEW")

def decrement_level(level):
    print("reducing player count in " + level + " by one")
    response = table.update_item(Key={'levelId': level}, UpdateExpression="set playerCount = if_not_exists(playerCount, :initial) - :val", ExpressionAttributeValues={':val': 1, ':initial' : 1 }, ReturnValues="UPDATED_NEW")

def make_transition_key(from_level, to_level):
    f=from_level
    t=to_level

    if from_level is None:
        f = ""
    if to_level is None:
        t = ""

    return "{}/{}".format(f,t)

def write_transition(old_level, new_level):
    # write to the transitions table, bumping the count for this record
    if old_level != new_level:

        record_key = make_transition_key(old_level, new_level)
        print("transition key = {}".format(record_key))

        if old_level is not None and new_level is not None:
            # from / to a level
            response = transitions_table.update_item(
                Key={'transitionLevels': record_key},
                UpdateExpression="set #total = if_not_exists(#total, :initial) + :val, levelFrom = :from, levelTo = :to",
                ExpressionAttributeValues={':val': 1, ':initial': 0, ':from': old_level, ':to': new_level },
                ExpressionAttributeNames={'#total': 'count'},
                ReturnValues="UPDATED_NEW"
            )
        elif old_level is None and new_level is not None:
            # entered game
            response = transitions_table.update_item(
                Key={'transitionLevels': record_key},
                UpdateExpression="set #total = if_not_exists(#total, :initial) + :val, levelTo = :to", # there's no levelFrom in here (no attribute means it's null here)
                ExpressionAttributeValues={':val': 1, ':initial': 0, ':to': new_level },
                ExpressionAttributeNames={'#total': 'count'},
                ReturnValues="UPDATED_NEW"
            )
        elif new_level is None and old_level is not None:
            # exiting game
            response = transitions_table.update_item(
                Key={'transitionLevels': record_key},
             UpdateExpression="set #total = if_not_exists(#total, :initial) + :val, levelFrom = :from ", # there's no levelTo in here
                ExpressionAttributeValues={':val': 1, ':initial': 0, ':from': old_level },
                ExpressionAttributeNames={'#total': 'count'},
                ReturnValues="UPDATED_NEW"
            )
        else:
            raise ValueError("Unexpected error - level change does not meet transition criteria")

def lambda_handler(event, context):
    print(json.dumps(event, indent=2))

    for record in event['Records']:
        print("***")
        levels = get_level_changes(record["dynamodb"])
        old_level = levels[0]
        new_level = levels[1]

        if old_level is None:
            print("previous level is undefined!")
        else:
            print("previous level: " + old_level)

        if new_level is None:
            print("current level is undefined!")
        else:
            print("current level: " + new_level)

        if old_level == new_level:
            print("Level unchanged")
        else:
            write_transition(old_level, new_level)

            if old_level is not None:
                decrement_level(old_level)

            if new_level is not None:
                increment_level(new_level)


    return 'Successfully processed {} records.'.format(len(event['Records']))
