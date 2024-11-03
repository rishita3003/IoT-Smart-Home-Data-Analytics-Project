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

    # with open(file_path, 'r') as file:
    #     reader = csv.DictReader(file)
    #     for row in reader:
    #         producer.send(topic, row)
    #         time.sleep(0.2)  # Simulate real-time data feed
    #         #print("Sent:", row)

send_data_to_kafka()
