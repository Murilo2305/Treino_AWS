import boto3
import time 
import json 
from datetime import datetime
import os
from dotenv import load_dotenv
import csv

load_dotenv()

bucket = os.getenv("bucket")

client = boto3.client(

    "s3",
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"),
    aws_session_token= os.getenv("aws_session_token")

) 

paginator = client.get_paginator('list_objects_v2')

while True:

    for page in paginator.paginate(Bucket = bucket,Prefix = "raw/"):        

        for obj in page["Contents"]:

            chave = obj["Key"]

            response = client.get_object(Bucket= bucket,Key=chave)
            file_content = response['Body'].read().decode('utf-8')
            j = json.loads(file_content)
            print(f"{chave}: " + str(j))


    time.sleep(5)