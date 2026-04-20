import boto3
import time 
import json 
from datetime import datetime
import os
from dotenv import load_dotenv
import csv
from botocore.exceptions import ClientError
from statistics import mean

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

    try:

        client.head_object(Bucket = bucket, Key = "trusted/trustedAntena.csv")
            
    except ClientError as e:

        headers = ["antena","datetime","MvelSend","MvelRecv","PvelSend","PvelRecv","Pcons","Pcpu","Pram","Mcons","Mcpu","Mram","Tcons"]

        if e.response['Error']['Code'] == "404":
                    
            with open ("trustedAntena.csv","w",newline="") as f:
                        
                writer=csv.writer(f)
                writer.writerow(headers)

        client.upload_file("trustedAntena.csv",bucket,"trusted/trustedAntena.csv")
        
    if os.path.exists("trustedAntena.csv"):

        os.remove("trustedAntena.csv")

    antena = "a1113"
    registros = []

    for page in paginator.paginate(Bucket = bucket,Prefix = "raw/"):        

        for obj in page["Contents"]:

            chave = obj["Key"]

            if antena in chave:

                response = client.get_object(Bucket= bucket,Key=chave)
                registros.append({"nome": antena,"conteudo": response})
            
    registros = sorted(registros, key = lambda x: x["conteudo"]["LastModified"], reverse=True)

    data = {

        "antena": antena,
         "cpu": [],
         "ram": [],
         "velSend": [],
         "velRecv": [],
         "cons": []

    }

    for registro in registros:

        print(f"{registro["nome"]} : {registro["conteudo"]["LastModified"]}")
        file_content = registro["conteudo"]['Body'].read().decode('utf-8')
        j = json.loads(file_content)
        
        if len(data["cpu"]) < 3:

            data["cpu"].append(j["cpu"])
            data["ram"].append(j["ram"])
            data["velSend"].append(j["ByteSend"])
            data["velRecv"].append(j["ByteRecv"])
            data["cons"].append(j["cons"])

        else:

            break

        print(data)


    csvLinha = [antena,datetime.now().strftime("%Y-%m-%d_%H-%M"),mean(data["velSend"])/1000000,mean(data["velRecv"])/1000000,max(data["velSend"])/1000000,max(data["velRecv"])/1000000,round(max(data["cons"]),2),round(max(data["cpu"]),2),round(max(data["ram"]),2),round(mean(data["cons"]),2),round(mean(data["cpu"]),2),round(mean(data["ram"])),sum(data["cons"])]

    print(csvLinha)


    csvS3 = client.get_object(Bucket = bucket, Key = "trusted/trustedAntena.csv")
    dados = csvS3["Body"].read().decode('utf-8')

    with open ("trustedAntena.csv","w",newline="") as f:
                        
        f.write(dados)

    with open ("trustedAntena.csv","a",newline="") as f:
                        
        writer=csv.writer(f)
        writer.writerow(csvLinha)

    client.upload_file("trustedAntena.csv",bucket,"trusted/trustedAntena.csv")
    
    os.remove("trustedAntena.csv")

    time.sleep(180)