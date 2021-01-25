import os, csv, json
from kafka import KafkaProducer

csvPath = '{}/oscar_age_male.csv'.format(os.path.dirname(__file__))
jsonPath = '{}/test.json'.format(os.path.dirname(__file__))

producer = KafkaProducer(bootstrap_servers = 'localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
topic = 'my-test-topic'

# Function to first convert csv to JSON
# Takes file paths as args
def convert_to_json(csvPath, jsonPath):

    data = {}

    with open(csvPath, encoding="utf-8") as csv_dt:
        csvReader = csv.DictReader(csv_dt)

        for rows in csvReader:
            key = rows['Index']
            data[key] = rows
    
    with open(jsonPath, 'w', encoding='utf-8') as json_dt:
        json_dt.write(json.dumps(data, indent=4))
        
    
    





convert_to_json(csvPath, jsonPath)









            









