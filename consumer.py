from kafka import *
import json


consumer = KafkaConsumer('transaction',
                            bootstrap_servers=['127.0.0.1:9092'],
                            value_deserializer=lambda m: json.loads(m.decode('utf-8')))

while True:
    records = consumer.poll(1.0)

    for partition, record_list in records.items():
        for record in record_list:
            message = record.value
            print(f"Reçu un message: {message}")