import argparse
import boto3
import json
import uuid

__ACTION_GET = 'get'
__ACTION_GET_TAGS = 'get_tags'
__ACTION_SET_DEFAULT_TAGS = 'set_tags_d'

__DEFAULT_BUCKET = 'TrachyFamily'
__DEFAULT_REGION = 'us-east-1'
__DEFAULT_TABLE_NAME = 'Photo'

__DEFAULT_TAG_LIST = [
    {'Key': 'processed', 'Value': 'False'},
    {'Key': 'people', 'Value': ''},
    {'Key': 'location', 'Value': ''},
    {'Key': 'date', 'Value': ''},
    {'Key': 'meta', 'Value': ''},
    {'Key': 'photoId', 'Value': ''}
]

s3 = boto3.resource('s3')
trachy_bucket = s3.Bucket(__DEFAULT_BUCKET)

def get_photo(photo_reference):
    for resource in trachy_bucket.objects.filter(Prefix=photo_reference):
        print('Retrieving photo from {}'.format(photo_reference))
        r = resource.Object()
        #print(r.__dict__)
        pic = r.get()
        print(pic)

def get_tags_for_photo(photo_reference):
    client = boto3.client('s3')
    photo_tag_resp = client.get_object_tagging(
        Bucket=__DEFAULT_BUCKET,
        Key=photo_reference
    )

    return photo_tag_resp.get('TagSet')

def apply_default_tag_set(photo_reference):
    client = boto3.client('s3')
    client.put_object_tagging(
        Bucket=__DEFAULT_BUCKET,
        Key=photo_reference,
        Tagging={
            'TagSet': __DEFAULT_TAG_LIST
        }
    )

def dynamo_scan_test():
    client = boto3.client('dynamodb', region_name=__DEFAULT_REGION)
    documents = client.scan(
        TableName=__DEFAULT_TABLE_NAME,
        Limit=1
    )
    print(json.dumps(documents))

def put_dynamo_record(dynamo_photo):
    dynamodb = boto3.resource('dynamodb', region_name=__DEFAULT_REGION)
    table = dynamodb.Table(__DEFAULT_TABLE_NAME)
    response = table.put_item(
        Item={
            'photoId': dynamo_photo.photo_id,
            'people': dynamo_photo.people,
            'location': dynamo_photo.location,
            'processed': bool(dynamo_photo.processed),
            'meta': dynamo_photo.meta,
            's3Location': dynamo_photo.s3_location
        }
    )

def get_dynamo_record_by_s3_location(s3_location):
    client = boto3.client('dynamodb', region_name=__DEFAULT_REGION)
    documents = client.scan(
        TableName=__DEFAULT_TABLE_NAME,
        Limit=1,
        FilterExpression='s3Location = :s3Location',
        ExpressionAttributeValues={
            ':s3Location': {
                "S": s3_location
            }
        }

    )
    print(json.dumps(documents))


def get_dynamo_record_by_id():
    client = boto3.client('dynamodb', region_name=__DEFAULT_REGION)
    document = client.get_item(
        TableName=__DEFAULT_TABLE_NAME,
        Key={
            'photoId': {
                'S': 'b47b05c4-a355-404e-b3b0-6b6f62c0a899'
            }
        }
    )
    return document.get('Item')


class DynamoPhoto:
    def __init__(self, people, location, processed, meta, s3_location, photo_id=None):
        self.people = people
        self.location = location
        self.processed = processed
        self.meta = meta
        self.s3_location = s3_location
        if photo_id is not None:
            self.photo_id = photo_id
        else:
            # This is a new DynamoPhoto getting created
            self.photo_id = str(uuid.uuid4())

#for resource in trachy_bucket.objects.filter(Prefix='Photos/Photos from James iPhone/September 2014 dump'):
#    print(resource.key)
#    r = resource.Object()
#    r.put(Tagging='processed=False&testTag2=boo')
    #pic = r.get()

    #print(str(pic['ContentLength']))

    #f = open('temp.jpg', 'wb')
    #f.write(pic['Body'].read())

    #f.close()

#for bucket in s3.buckets.all():
#    print(bucket.name)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gathering arguments')
    parser.add_argument('-a', action='store', dest='action', required=False, default=__ACTION_GET,
                        help='The action to perform, default={}. Options = {}, {}, {}'.format(
                            __ACTION_GET, __ACTION_GET, __ACTION_GET_TAGS, __ACTION_SET_DEFAULT_TAGS))
    parser.add_argument('-p', action='store', dest='photo', required=False,
                        help='The photo to take action on - if the action requires specifying a photo')
    parser.add_argument('-t', '--test_mode', action='store_true',
                        help='Do not connect to a server, just ask for commands')
    args = parser.parse_args()

    if args.photo is None:
        args.photo = 'Photos/Photos from James iPhone/September 2014 dump/2014-09-20 15.39.40.jpg'

    if args.test_mode:
        get_dynamo_record_by_s3_location(args.photo)
    elif args.action == __ACTION_GET:
        get_photo(args.photo)
    elif args.action == __ACTION_GET_TAGS:
        print(json.dumps(get_tags_for_photo(args.photo)))
    elif args.action == __ACTION_SET_DEFAULT_TAGS:
        apply_default_tag_set(args.photo)
