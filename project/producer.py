import time
import json
import csv
from kafka import KafkaProducer
import pandas as pd


data = pd.read_csv("test_data.csv")

def send_data_to_kafka(topic='raw-climate-data'):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    for index, record in data.iterrows():
        producer.send(topic, value=record.to_dict())
        print(f"Sent: {record.to_dict()}")
        time.sleep(0.2)

send_data_to_kafka()
