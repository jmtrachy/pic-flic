import argparse
import boto3
import json

__ACTION_GET = 'get'
__ACTION_GET_TAGS = 'get_tags'
__ACTION_SET_DEFAULT_TAGS = 'set_tags_d'

class S3Service():
    __DEFAULT_TAG_LIST = [
        {'Key': 'processed', 'Value': 'False'},
        {'Key': 'people', 'Value': ''},
        {'Key': 'location', 'Value': ''},
        {'Key': 'date', 'Value': ''},
        {'Key': 'meta', 'Value': ''},
        {'Key': 'photoId', 'Value': ''},
        {'Key': 'printWorth', 'Value': ''}
    ]

    def __init__(self, bucket='TrachyFamily'):
        self.s3 = boto3.resource('s3')
        self.bucket_name = bucket
        self.bucket = self.s3.Bucket(bucket)

    def get_photo(self, photo_reference):
        for resource in self.bucket.objects.filter(Prefix=photo_reference):
            print('Retrieving photo from {}'.format(photo_reference))
            r = resource.Object()
            # print(r.__dict__)
            pic = r.get()
            print(pic)

    def get_tags_for_photo(self, photo_reference):
        client = boto3.client('s3')
        photo_tag_resp = client.get_object_tagging(
            Bucket=self.bucket_name,
            Key=photo_reference
        )

        return photo_tag_resp.get('TagSet')

    def apply_default_tag_set(self, photo_reference):
        client = boto3.client('s3')
        client.put_object_tagging(
            Bucket=self.bucket_name,
            Key=photo_reference,
            Tagging={
                'TagSet': S3Service.__DEFAULT_TAG_LIST
            }
        )

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gathering arguments')
    parser.add_argument('-a', action='store', dest='action', required=False, default=__ACTION_GET,
                        help='The action to perform, default={}. Options = {}, {}, {}'.format(
                            __ACTION_GET, __ACTION_GET, __ACTION_GET_TAGS, __ACTION_SET_DEFAULT_TAGS))
    parser.add_argument('-p', action='store', dest='photo', required=False,
                        help='The photo to take action on - if the action requires specifying a photo')
    args = parser.parse_args()

    if args.photo is None:
        args.photo = 'Photos/Photos from James iPhone/September 2014 dump/2014-09-20 15.39.40.jpg'

    aws_service = S3Service()
    if args.action == __ACTION_GET:
        aws_service.get_photo(args.photo)
    elif args.action == __ACTION_GET_TAGS:
        print(json.dumps(aws_service.get_tags_for_photo(args.photo)))
    elif args.action == __ACTION_SET_DEFAULT_TAGS:
        aws_service.apply_default_tag_set(args.photo)