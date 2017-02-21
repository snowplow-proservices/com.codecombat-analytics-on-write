from __future__ import print_function

import json
import random
import time
import boto3
from botocore.exceptions import ClientError
import decimal
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime

print('Loading function')

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('player-state')

ENRICHMENT_KEY = "contexts_com_codecombat_level_context_1"


def timenow_millis():
    return int(round(time.time() * 1000))

def update_player_level(player, level, timestamp):
    # write the record to dynamodb, IFF the update time we have is newer than the one in the database
    print("Changing level for {} to {} (if older than {})".format(player,level,timestamp))
    try:
        response = table.update_item(
            Key={
                'playerId': player,
            },
            UpdateExpression="set levelId = :level, lastUpdated = :timestamp",
            ConditionExpression="attribute_not_exists(lastUpdated) OR lastUpdated <= :timestamp",
            ExpressionAttributeValues={
                ':level': level,
                ':timestamp': timestamp
            },
            ReturnValues="UPDATED_NEW"
        )
        print("Level changed")
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print("Level change ignored - a newer record exists in the table")
        else:
            raise

def get_records(update):
    data = []
    if "Records" in update:
        for record in update["Records"]:
            if "kinesis" in record and "data" in record['kinesis']:
                decoded_data = record['kinesis']['data'].decode('base64')
                data.append(decoded_data)
    return data

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    records = get_records(event)

    for record in records:
        snowplow_event_json = None

        try:
            snowplow_event_json = transform(record)
        except:
            print("Ignoring badly formatted record in stream (failed to parse with SP analytics SDK")
            continue

        if ENRICHMENT_KEY in snowplow_event_json:

            for user_level_context in snowplow_event_json[ENRICHMENT_KEY]:

                if 'user_id' in user_level_context and 'level_slug' in user_level_context:
                    player_id = user_level_context['user_id']
                    level_id = user_level_context['level_slug']

                    utc_dt = datetime.strptime(snowplow_event_json['collector_tstamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    timestamp = (utc_dt - datetime(1970, 1, 1)).total_seconds() * 1000 # seconds -> milliseconds
                    timestamp = int(timestamp) # remove fractional milliseconds (timestamp is now an integer)

                    # sometimes these values are null, ignore those
                    if player_id is None or level_id is None:
                        print("{} is missing a player id ({}) or level id ({})".format(ENRICHMENT_KEY, player_id, level_id))
                    else:
                        now = timenow_millis()
                        print("player name = {}\ncurrent level = {}\ntimestamp = {}\ntime now = {}".format(player_id, level_id, timestamp, now))
                        update_player_level(player_id, level_id, now)
                else:
                    print("{} in unexpected format - cannot find 'user_id' or 'level_slug'".format(ENRICHMENT_KEY))

        else:
            print("Ignoring Snowplow JSON event without {} key".format(ENRICHMENT_KEY))


        # user id
        # level slug
        #details = get_player_details(record)

        #playerId = details['playerId']
        #levelId = details['levelId']
        #timestamp = details['timestamp']

        #print("player name = {}\ncurrent level = {}\ntimestamp = {}".format(playerId, levelId, timestamp))

        #update_player_level(playerId, levelId, timestamp)

    return "Successfully processed {} Kinesis Records(s)".format(len(records))





# ############################################################################ #
# Snowplow Python Analytics SDK follows
# https://github.com/snowplow/snowplow-python-analytics-sdk
# ############################################################################ #

import re

LATITUDE_INDEX = 22
LONGITUDE_INDEX = 23

def convert_string(key, value):
    return [(key, value)]


def convert_int(key, value):
    return [(key, int(value))]


def convert_bool(key, value):
    if value == '1':
        return [(key, True)]
    elif value == '0':
        return [(key, False)]
    raise SnowplowEventTransformationException(["Invalid value {} for field {}".format(value, key)])


def convert_double(key, value):
    return [(key, float(value))]


def convert_tstamp(key, value):
    return [(key, value.replace(' ', 'T') + 'Z')]


def convert_contexts(key, value):
    return parse_contexts(value)


def convert_unstruct(key, value):
    return parse_unstruct(value)

# Ordered list of names of enriched event fields together with the function required to convert them to JSON
ENRICHED_EVENT_FIELD_TYPES = (
    ("app_id", convert_string),
    ("platform", convert_string),
    ("etl_tstamp", convert_tstamp),
    ("collector_tstamp", convert_tstamp),
    ("dvce_created_tstamp", convert_tstamp),
    ("event", convert_string),
    ("event_id", convert_string),
    ("txn_id", convert_int),
    ("name_tracker", convert_string),
    ("v_tracker", convert_string),
    ("v_collector", convert_string),
    ("v_etl", convert_string),
    ("user_id", convert_string),
    ("user_ipaddress", convert_string),
    ("user_fingerprint", convert_string),
    ("domain_userid", convert_string),
    ("domain_sessionidx", convert_int),
    ("network_userid", convert_string),
    ("geo_country", convert_string),
    ("geo_region", convert_string),
    ("geo_city", convert_string),
    ("geo_zipcode", convert_string),
    ("geo_latitude", convert_double),
    ("geo_longitude", convert_double),
    ("geo_region_name", convert_string),
    ("ip_isp", convert_string),
    ("ip_organization", convert_string),
    ("ip_domain", convert_string),
    ("ip_netspeed", convert_string),
    ("page_url", convert_string),
    ("page_title", convert_string),
    ("page_referrer", convert_string),
    ("page_urlscheme", convert_string),
    ("page_urlhost", convert_string),
    ("page_urlport", convert_int),
    ("page_urlpath", convert_string),
    ("page_urlquery", convert_string),
    ("page_urlfragment", convert_string),
    ("refr_urlscheme", convert_string),
    ("refr_urlhost", convert_string),
    ("refr_urlport", convert_int),
    ("refr_urlpath", convert_string),
    ("refr_urlquery", convert_string),
    ("refr_urlfragment", convert_string),
    ("refr_medium", convert_string),
    ("refr_source", convert_string),
    ("refr_term", convert_string),
    ("mkt_medium", convert_string),
    ("mkt_source", convert_string),
    ("mkt_term", convert_string),
    ("mkt_content", convert_string),
    ("mkt_campaign", convert_string),
    ("contexts", convert_contexts),
    ("se_category", convert_string),
    ("se_action", convert_string),
    ("se_label", convert_string),
    ("se_property", convert_string),
    ("se_value", convert_string),
    ("unstruct_event", convert_unstruct),
    ("tr_orderid", convert_string),
    ("tr_affiliation", convert_string),
    ("tr_total", convert_double),
    ("tr_tax", convert_double),
    ("tr_shipping", convert_double),
    ("tr_city", convert_string),
    ("tr_state", convert_string),
    ("tr_country", convert_string),
    ("ti_orderid", convert_string),
    ("ti_sku", convert_string),
    ("ti_name", convert_string),
    ("ti_category", convert_string),
    ("ti_price", convert_double),
    ("ti_quantity", convert_int),
    ("pp_xoffset_min", convert_int),
    ("pp_xoffset_max", convert_int),
    ("pp_yoffset_min", convert_int),
    ("pp_yoffset_max", convert_int),
    ("useragent", convert_string),
    ("br_name", convert_string),
    ("br_family", convert_string),
    ("br_version", convert_string),
    ("br_type", convert_string),
    ("br_renderengine", convert_string),
    ("br_lang", convert_string),
    ("br_features_pdf", convert_bool),
    ("br_features_flash", convert_bool),
    ("br_features_java", convert_bool),
    ("br_features_director", convert_bool),
    ("br_features_quicktime", convert_bool),
    ("br_features_realplayer", convert_bool),
    ("br_features_windowsmedia", convert_bool),
    ("br_features_gears", convert_bool),
    ("br_features_silverlight", convert_bool),
    ("br_cookies", convert_bool),
    ("br_colordepth", convert_string),
    ("br_viewwidth", convert_int),
    ("br_viewheight", convert_int),
    ("os_name", convert_string),
    ("os_family", convert_string),
    ("os_manufacturer", convert_string),
    ("os_timezone", convert_string),
    ("dvce_type", convert_string),
    ("dvce_ismobile", convert_bool),
    ("dvce_screenwidth", convert_int),
    ("dvce_screenheight", convert_int),
    ("doc_charset", convert_string),
    ("doc_width", convert_int),
    ("doc_height", convert_int),
    ("tr_currency", convert_string),
    ("tr_total_base", convert_double),
    ("tr_tax_base", convert_double),
    ("tr_shipping_base", convert_double),
    ("ti_currency", convert_string),
    ("ti_price_base", convert_double),
    ("base_currency", convert_string),
    ("geo_timezone", convert_string),
    ("mkt_clickid", convert_string),
    ("mkt_network", convert_string),
    ("etl_tags", convert_string),
    ("dvce_sent_tstamp", convert_tstamp),
    ("refr_domain_userid", convert_string),
    ("refr_device_tstamp", convert_tstamp),
    ("derived_contexts", convert_contexts),
    ("domain_sessionid", convert_string),
    ("derived_tstamp", convert_tstamp),
    ("event_vendor", convert_string),
    ("event_name", convert_string),
    ("event_format", convert_string),
    ("event_version", convert_string),
    ("event_fingerprint", convert_string),
    ("true_tstamp", convert_tstamp)
)


def transform(line, known_fields=ENRICHED_EVENT_FIELD_TYPES, add_geolocation_data=True):
    """
    Convert a Snowplow enriched event TSV into a JSON
    """
    return jsonify_good_event(line.split('\t'), known_fields, add_geolocation_data)


def jsonify_good_event(event, known_fields=ENRICHED_EVENT_FIELD_TYPES, add_geolocation_data=True):
    """
    Convert a Snowplow enriched event in the form of an array of fields into a JSON
    """
    if len(event) != len(known_fields):
        raise SnowplowEventTransformationException(
            ["Expected {} fields, received {} fields.".format(len(known_fields), len(event))]
        )
    else:
        output = {}
        errors = []
        if add_geolocation_data and event[LATITUDE_INDEX] != '' and event[LONGITUDE_INDEX] != '':
            output['geo_location'] = event[LATITUDE_INDEX] + ',' + event[LONGITUDE_INDEX]
        for i in range(len(event)):
            key = known_fields[i][0]
            if event[i] != '':
                try:
                    kvpairs = known_fields[i][1](key, event[i])
                    for kvpair in kvpairs:
                        output[kvpair[0]] = kvpair[1]
                except SnowplowEventTransformationException as sete:
                    errors += sete.error_messages
                except Exception as e:
                    errors += ["Unexpected exception parsing field with key {} and value {}: {}".format(
                        known_fields[i][0],
                        event[i],
                        repr(e)
                    )]
        if errors:
            raise SnowplowEventTransformationException(errors)
        else:
            return output


SCHEMA_PATTERN = re.compile(""".+:([a-zA-Z0-9_\.]+)/([a-zA-Z0-9_]+)/[^/]+/(.*)""")


def fix_schema(prefix, schema):
    """
    Create an Elasticsearch field name from a schema string
    """
    match = re.match(SCHEMA_PATTERN, schema)
    if match:
        snake_case_organization = match.group(1).replace('.', '_').lower()
        snake_case_name = re.sub('([^A-Z_])([A-Z])', '\g<1>_\g<2>', match.group(2)).lower()
        model = match.group(3).split('-')[0]
        return "{}_{}_{}_{}".format(prefix, snake_case_organization, snake_case_name, model)
    else:
        raise SnowplowEventTransformationException([
            "Schema {} does not conform to regular expression {}".format(schema, SCHEMA_PATTERN)
        ])


def parse_contexts(contexts):
    """
    Convert a contexts JSON to an Elasticsearch-compatible list of key-value pairs
    For example, the JSON

    {
      "data": [
        {
          "data": {
            "unique": true
          },
          "schema": "iglu:com.acme/unduplicated/jsonschema/1-0-0"
        },
        {
          "data": {
            "value": 1
          },
          "schema": "iglu:com.acme/duplicated/jsonschema/1-0-0"
        },
        {
          "data": {
            "value": 2
          },
          "schema": "iglu:com.acme/duplicated/jsonschema/1-0-0"
        }
      ],
      "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0"
    }

    would become

    [
      ("context_com_acme_duplicated_1", [{"value": 1}, {"value": 2}]),
      ("context_com_acme_unduplicated_1", [{"unique": true}])
    ]
    """
    my_json = json.loads(contexts)
    data = my_json['data']
    distinct_contexts = {}
    for context in data:
        schema = fix_schema("contexts", context['schema'])
        inner_data = context['data']
        if schema not in distinct_contexts:
            distinct_contexts[schema] = [inner_data]
        else:
            distinct_contexts[schema].append(inner_data)
    output = []
    for key in distinct_contexts:
        output.append((key, distinct_contexts[key]))
    return output


def parse_unstruct(unstruct):
    """
    Convert an unstructured event JSON to a list containing one Elasticsearch-compatible key-value pair
    For example, the JSON

    {
      "data": {
        "data": {
          "key": "value"
        },
        "schema": "iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1"
      },
      "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0"
    }

    would become

    [
      (
        "unstruct_com_snowplowanalytics_snowplow_link_click_1", {
          "key": "value"
        }
      )
    ]
    """
    my_json = json.loads(unstruct)
    data = my_json['data']
    schema = data['schema']
    if 'data' in data:
        inner_data = data['data']
    else:
        raise SnowplowEventTransformationException(["Could not extract inner data field from unstructured event"])
    fixed_schema = fix_schema("unstruct_event", schema)
    return [(fixed_schema, inner_data)]

class SnowplowEventTransformationException(Exception):
    def __init__(self, error_messages):
        self.error_messages = error_messages
        self.message = '\n'.join(error_messages)

    def __str__(self):
        return repr(self.error_messages)
