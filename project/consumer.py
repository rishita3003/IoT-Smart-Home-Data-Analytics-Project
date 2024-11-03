import json
from kafka import KafkaConsumer
from model import ModelTrainer  # Import the ModelTrainer class with correct functionalities

def consume_data_from_kafka(model_trainer):
    consumer = KafkaConsumer(
        'raw-climate-data',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        value_deserializer=
        lambda m: json.loads(m.decode('utf-8')) if m else None
    )
    for message in consumer:
        print(message)
    # try:
    #     for message in consumer:
    #         data = message.value
    #         if data:
    #             try:
    #                 print(data)
    #                 #result = model_trainer.predict(data)  # Use predict method on the instance
    #                 #print("Processed:", result)
    #             except Exception as e:
    #                 print(f"Error processing data: {e}")
    #         else:
    #             print("Received empty or non-JSON data")
    # finally:
    #     consumer.close()

model_save_path = '/home/rishita/project'
trainer = ModelTrainer(None, model_save_path)  # Assuming ModelTrainer can handle None for data_path
trainer.load_model()  # Ensure the model is correctly loaded
consume_data_from_kafka(trainer)
