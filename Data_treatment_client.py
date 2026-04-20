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

        print("Erro")


    res = client.get_object(Bucket = bucket, Key = "trusted/trustedAntena.csv")
    csvAntenas = res["Body"].read().decode("utf-8")

    with open("csvAntenas.csv","w", newline = "") as f:

        f.write(csvAntenas)

    antenas = []
    registro = []

    with open("csvAntenas.csv","r") as f:

        leitor = csv.reader(f)

        next(leitor)

        for row in leitor:

            if (row[0] in antenas) == False:

                antenas.append(row[0])

            registro.append(row)

    os.remove("csvAntenas.csv")

    registro = sorted(registro, key = lambda x: x[1], reverse=True)

    dados = []

    for antena in antenas:

        for registr in registro:

            if registr[0] == antena:

                dados.append({

                    "antena" : antena,
                    "send" : registr[4],
                    "recv" : registr[5],
                    "cpu" : registr[7],
                    "prop": ((float(registr[7])/float(registr[4])) + (float(registr[7])/float(registr[5])))/2

                })

                break

    dados = sorted(dados,key = lambda x: x["prop"], reverse=True)

    headers = ["datetime","Rank_antenas","cpu_por_rank","send_por_rank","recv_por_rank","PvelSend","PvelRecv","Pdropins","Pdropouts","ActiveSessions","Topblock"]

    try:

        client.head_object(Bucket = bucket, Key = "client/client.csv")
                
    except ClientError as e:

        with open("client.csv","w", newline= "") as f:

            writer=csv.writer(f)
            writer.writerow(headers)
        
        client.upload_file("client.csv",bucket,"client/client.csv")

        os.remove("client.csv")

    res = client.get_object(Bucket = bucket, Key = "client/client.csv")

    with open("client.csv","w", newline= "") as f:

        f.write(res["Body"].read().decode("utf-8"))

    rankAntenas = ""
    rankCpu = ""
    rankSend = ""
    rankRecv = ""

    for antena in dados:
        rankAntenas += f"{antena["antena"]};"
        rankCpu += f"{antena["cpu"]};"
        rankSend += f"{antena["send"]};"
        rankRecv += f"{antena["recv"]};"

    result = client.get_object(Bucket = bucket, Key = "trusted/trustedFirewall.csv")

    with open("trustedFirewall.csv","w", newline= "") as f:

        f.write(result["Body"].read().decode("utf-8"))

    linha_firewall = []

    with open("trustedFirewall.csv","r",newline="") as f:

        leitor = csv.reader(f)
        linha_firewall = list(leitor)[-1]

    os.remove("trustedFirewall.csv")

    csvline = [datetime.now().strftime("%Y-%m-%d_%H-%M"),rankAntenas,rankCpu, rankSend, rankRecv,linha_firewall[3],linha_firewall[4],linha_firewall[11],linha_firewall[12],linha_firewall[14],linha_firewall[15]]

    with open ("client.csv","a",newline= "") as f:

        writer = csv.writer(f)
        writer.writerow(csvline)

    client.upload_file("client.csv",bucket,"client/client.csv")

    os.remove("client.csv")

    time.sleep(180)