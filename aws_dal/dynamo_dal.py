import argparse
import boto3
import json
import uuid

__ACTION_PUT_PHOTO = 'put'
__ACTION_GET_BY_S3 = 'get_by_s3'
__ACTION_GET_RECORD_BY_ID = 'get_by_id'
__ACTION_GET_NEXT_PHOTO = 'next'

__DEFAULT_REGION = 'us-east-1'
__DEFAULT_TABLE_NAME = 'Photo'


class DynamoService():
    def __init__(self, table_name='Photo', region='us-east-1'):
        self.region = region
        self.table_name = table_name

    def get_next_unprocessed_photo(self):
        client = boto3.client('dynamodb', region_name=self.region)
        documents = client.scan(
            TableName=self.table_name,
            Limit=1
        )
        print(json.dumps(documents))

    def put_dynamo_record(self, dynamo_photo):
        dynamodb = boto3.resource('dynamodb', region_name=self.region)
        table = dynamodb.Table(self.table_name)
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

    def get_dynamo_record_by_s3_location(self, s3_location):
        client = boto3.client('dynamodb', region_name=self.region)
        documents = client.scan(
            TableName=self.table_name,
            Limit=1,
            FilterExpression='s3Location = :s3Location',
            ExpressionAttributeValues={
                ':s3Location': {
                    "S": s3_location
                }
            }

        )
        print(json.dumps(documents))


    def get_dynamo_record_by_id(self):
        client = boto3.client('dynamodb', region_name=self.region)
        document = client.get_item(
            TableName=self.table_name,
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
    parser.add_argument('-a', action='store', dest='action', required=False, default=__ACTION_GET_RECORD_BY_ID,
                        help='The action to perform, default={}. Options = {}, {}, {}'.format(
                            __ACTION_GET_RECORD_BY_ID, __ACTION_GET_BY_S3,
                            __ACTION_GET_RECORD_BY_ID, __ACTION_PUT_PHOTO,
                            __ACTION_GET_NEXT_PHOTO))
    parser.add_argument('-p', action='store', dest='photo', required=False,
                        help='The photo to take action on - if the action requires specifying a photo')
    parser.add_argument('-t', '--test_mode', action='store_true',
                        help='Do not connect to a server, just ask for commands')
    args = parser.parse_args()

    if args.photo is None:
        args.photo = 'Photos/Photos from James iPhone/September 2014 dump/2014-09-20 15.39.40.jpg'

    ds = DynamoService()
    if args.action == __ACTION_GET_RECORD_BY_ID:
        ds.get_dynamo_record_by_id()
    elif args.action == __ACTION_GET_BY_S3:
        ds.get_dynamo_record_by_s3_location(args.photo)
    elif args.action == __ACTION_PUT_PHOTO:
        ds.put_dynamo_record(DynamoPhoto(
            'Teri,Lauren',
            'Zoo',
            'False',
            'what a cool day',
            args.photo
        ))
    elif args.action == __ACTION_GET_NEXT_PHOTO:
        print('hi')
