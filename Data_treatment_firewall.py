import boto3
import time 
import json 
from datetime import datetime
import os
from dotenv import load_dotenv
import csv
from botocore.exceptions import ClientError
from statistics import mean
from statistics import mode

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

        client.head_object(Bucket = bucket, Key = "trusted/trustedFirewall.csv")
            
    except ClientError as e:

        headers = ["datetime","MvelSend","MvelRecv","PvelSend","PvelRecv","Pcpu","Pram","Mcpu","Mram","Mropins","Mdropouts","Pdropins","Pdropouts","MActiveConnections","PActiveConnections","TopBlock"]

        if e.response['Error']['Code'] == "404":
                    
            with open ("trustedFirewall.csv","w",newline="") as f:
                        
                writer=csv.writer(f)
                writer.writerow(headers)

        client.upload_file("trustedFirewall.csv",bucket,"trusted/trustedFirewall.csv")
        
    if os.path.exists("trustedFirewall.csv"):

        os.remove("trustedFirewall.csv")

    registros = []

    for page in paginator.paginate(Bucket = bucket,Prefix = "raw/"):        

        for obj in page["Contents"]:

            chave = obj["Key"]

            if "f" in chave:

                response = client.get_object(Bucket= bucket,Key=chave)
                registros.append({"conteudo": response})
            
    registros = sorted(registros, key = lambda x: x["conteudo"]["LastModified"], reverse=True)

    data = {

         "cpu": [],
         "ram": [],
         "velSend": [],
         "velRecv": [],
         "dropins": [],
         "dropouts": [],
         "activeSessions": [],
         "topblock" : []

    }

    for registro in registros:

        file_content = registro["conteudo"]['Body'].read().decode('utf-8')
        j = json.loads(file_content)
        
        if len(data["cpu"]) < 3:

            data["cpu"].append(j["cpu"])
            data["ram"].append(j["ram"])
            data["velSend"].append(j["ByteSend"])
            data["velRecv"].append(j["ByteRecv"])
            data["dropins"].append(j["dropins"])
            data["dropouts"].append(j["dropouts"])
            data["activeSessions"].append(j["ActiveSessions"])
            data["topblock"].append(j["topBlock"])

        else:

            break

        print(data)


    csvLinha = [datetime.now().strftime("%Y-%m-%d_%H-%M"),mean(data["velSend"])/1000000,mean(data["velRecv"])/1000000,max(data["velSend"])/1000000,max(data["velRecv"])/1000000,round(max(data["cpu"]),2),round(max(data["ram"]),2),round(mean(data["cpu"]),2),round(mean(data["ram"])),round(mean(data["dropins"])),round(mean(data["dropouts"])),round(max(data["dropins"])),round(max(data["dropouts"])),round(mean(data["activeSessions"])),round(max(data["activeSessions"])),mode(data["topblock"])]

    #headers = ["datetime","MvelSend","MvelRecv","PvelSend","PvelRecv","Pcpu","Pram","Mcpu","Mram","Mropins","Mdropouts","Pdropins","Pdropouts","ActiveConnections","TopBlock"]

    print(csvLinha)


    csvS3 = client.get_object(Bucket = bucket, Key = "trusted/trustedFirewall.csv")
    dados = csvS3["Body"].read().decode('utf-8')

    with open ("trustedFirewall.csv","w",newline="") as f:
                        
        f.write(dados)

    with open ("trustedFirewall.csv","a",newline="") as f:
                        
        writer=csv.writer(f)
        writer.writerow(csvLinha)

    client.upload_file("trustedFirewall.csv",bucket,"trusted/trustedFirewall.csv")
    
    os.remove("trustedFirewall.csv")

    time.sleep(180)