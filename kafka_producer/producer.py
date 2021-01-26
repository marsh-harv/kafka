import os, csv, avro.schema, json
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from kafka import KafkaProducer
from collections import namedtuple

output_loc = '{}/avro.avro'.format(os.path.dirname(__file__))
CSV = '{}/oscar_age_male.csv'.format(os.path.dirname(__file__))
fields = ("Index","Year", "Age", "Name", "Movie")
csv_record = namedtuple('csv_record', fields)

p = KafkaProducer(bootstrap_servers = 'localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def read_csv(path):
    with open(path, 'rU') as data:
        data.readline()
        reader = csv.reader(data, delimiter=",")
        for row in map(csv_record._make, reader):
            yield row

def parse_schema(path='{}/schema.avsc'.format(os.path.dirname(__file__))):
    with open(path, 'r') as data:
        return avro.schema.parse(data.read())

def serilialise_records_and_send(records, outpath=output_loc):
    schema = parse_schema()
    with open(outpath, 'w') as out:
        writer = DataFileWriter(out, DatumWriter(), schema)
        for record in records:
            record = dict((f, getattr(record, f)) for f in record._fields)
            writer.append(record)
            msg = p.send(topic='test', value=record)
            metadt = msg.get()
            print(metadt.topic)
            print(metadt.partition)

serilialise_records_and_send(read_csv(CSV))











            









