import boto3
import configparser

config = configparser.ConfigParser()
config.read('dl.cfg')

s3 = boto3.resource(
    's3',
    aws_access_key_id=config["AWS"]["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=config["AWS"]["AWS_SECRET_ACCESS_KEY"]
).Bucket(
    'datalakebucketjsb'
)

objects = s3.Object('udacity/capstone/states/states.parquet')

print(
    objects.content_length
)
