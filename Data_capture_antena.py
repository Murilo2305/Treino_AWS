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

idAntena = "a1234"

while True:

    ByteSend1 = psutil.net_io_counters().bytes_sent
    ByteRecv1 = psutil.net_io_counters().bytes_recv

    time.sleep(5)

    ByteSend2 = psutil.net_io_counters().bytes_sent
    ByteRecv2 = psutil.net_io_counters().bytes_recv

    cons = random.randint(1,20)

    cpu = psutil.cpu_percent(interval=1, percpu=False)
    ram = psutil.virtual_memory().percent

    dados = {

        "ByteSend" : round((ByteSend2 - ByteSend1)/5),
        "ByteRecv" : round((ByteRecv2 - ByteRecv1)/5),
        "cpu" : cpu,
        "ram" : ram,
        "cons" : cons

    }

    data = datetime.now()
    nome = idAntena+"_"+data.strftime("%Y-%m-%d_%H-%M")+".json"

    with open(nome,"w") as f:
        json.dump(dados,f,indent=4)

    client.upload_file(nome,os.getenv("bucket"),"raw/"+nome)

    os.remove(nome)

    time.sleep(54)
