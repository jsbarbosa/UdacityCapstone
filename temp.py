import os
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

files = [
    os.path.join(
        path, file
    )
    for path, __, files in os.walk('output/immigration.parquet')
    for file in files
]

for path in files:
    name = os.path.join(
        'udacity/capstone/immigration/',
        path
    ).replace('output/', '')
    print(name)
    with open(path, 'rb') as file:
        s3.put_object(
            Body=file.read(),
            Key='test.crc'
        )
    break
