import os, csv, avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from kafka import KafkaProducer

CSV = '{}/oscar_age_male.csv'.format(os.path.dirname(__file__))

def read_csv(path):
    with open(path, 'rU') as data:
        reader = csv.DictReader(data)
        for row in reader:
            yield row

def parse_schema(path='{}/schema.avsc'.format(os.path.dirname(__file__))):
    with open(path, 'r') as data:
        return avro.schema.parse(data.read())

def serialize_records(records, outpath='{}/test.avro'.format(os.path.dirname(__file__))):
    schema = parse_schema()
    with open(outpath, 'wb') as out:
        writer = DataFileWriter(out, DatumWriter(), schema)
        for record in records:
            writer.append(record)

if __name__ == "__main__":
    serialize_records(read_csv(CSV))













            









