import os
import json
import time
import pandas as pd 
import pandavro as pdx
from fastavro import reader
from kafka import KafkaProducer



topicName = 'test-topic-3'
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))



OUTPUT_PATH='{}/test.avro'.format(os.path.dirname(__file__))

df = pd.read_csv('C:\kafka\kafka_producer\oscar_age_male.csv', delimiter=',', sep="\\")
avro = pdx.to_avro(OUTPUT_PATH, df)
with open(OUTPUT_PATH, 'rb') as data:
    avro_reader = reader(data)
    for record in avro_reader:
        msg = producer.send(topicName, value=record)

        metadata = msg.get()
        print(metadata.topic)
        print(metadata.partition)

        







