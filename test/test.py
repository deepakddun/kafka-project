from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient

schema_registry_conf = {'url': "http://localhost:8081"}
sr = SchemaRegistryClient(schema_registry_conf)
subjects = sr.get_subjects()
for subject in subjects:
    schema = sr.get_latest_version(subject)
    print(schema.version)
    print(schema.schema_id)
    print(schema.subject)
    print(schema.schema.schema_str)

print(sr.get_latest_version(subject_name='person-value').schema.schema_str)


