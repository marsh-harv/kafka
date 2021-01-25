import os, csv, avro.schema, fastavro
from kafka import KafkaProducer

schema = {
    "type": "record",
    "namespace": "avro_schema_test",
    "name": "Test",
    "fields": [
        {"name": "Index", "type": "int"},
        {"name": "Year", "type": "int"},
        {"name": "Age", "type": "int"},
        {"name": "Name", "type": "string"},
        {"name": "Movie", "type": "string"}
    ]
}
output_loc = '{}/avro.avro'.format(os.path.dirname(__file__))
CSV = '{}/oscar_age_male.csv'.format(os.path.dirname(__file__))

with open(CSV) as data:
    reader = csv.reader(data, delimiter=',')
    

with open(output_loc, 'wb') as output:
    fastavro.writer(output, schema, reader)

with open(output_loc, 'rb') as av:
    avro_reader = fastavro.reader(av)
    for record in avro_reader:
        print(record)














            









