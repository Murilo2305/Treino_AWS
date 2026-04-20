import psutil
import boto3
import time
import random 
import json 
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

client = boto3.client(

    "s3",
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"),
    aws_session_token= os.getenv("aws_session_token")

)

while True:

    ByteSend1 = psutil.net_io_counters().bytes_sent
    ByteRecv1 = psutil.net_io_counters().bytes_recv

    time.sleep(5)

    ByteSend2 = psutil.net_io_counters().bytes_sent
    ByteRecv2 = psutil.net_io_counters().bytes_recv

    cpu = psutil.cpu_percent(interval=1, percpu=False)
    ram = psutil.virtual_memory().percent

    activeSessions = len(psutil.pids())
    dropins = psutil.net_io_counters().dropin
    dropouts = psutil.net_io_counters().dropout

    ips = {"123.456.789.10","123.456.789.11","123.456.789.12"}

    dados = {

        "ByteSend" : round((ByteSend2 - ByteSend1)/5),
        "ByteRecv" : round((ByteRecv2 - ByteRecv1)/5),
        "cpu" : cpu,
        "ram" : ram,
        "ActiveSessions": activeSessions,
        "dropins" : dropins,
        "dropouts" : dropouts,
        "topBlock" : random.choice(tuple(ips))

    }

    data = datetime.now()
    nome = "f_"+data.strftime("%Y-%m-%d_%H-%M")+".json"

    with open(nome,"w") as f:
        json.dump(dados,f,indent=4)

    client.upload_file(nome,os.getenv("bucket"),"raw/"+nome)

    print(dados)

    os.remove(nome)

    time.sleep(54)
