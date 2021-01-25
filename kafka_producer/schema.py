from schema_registry.client import SchemaRegistryClient, schema

client = SchemaRegistryClient(url="http://localhost:8081")

deployment_schema = {
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


avro_schema = schema.AvroSchema(deployment_schema)

schema_id = client.register('Test', avro_schema)
