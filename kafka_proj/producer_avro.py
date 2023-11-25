import os
import uuid
from pathlib import Path
from typing import List, Dict
from aiokafka import AIOKafkaProducer
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

import kafka_proj.kafka_helper as kafka_helper
from models.person import Person, Address


# kafka-console-consumer.bat --bootstrap-server http://localhost:9094 --topic person --property schema.registry.url=http://localhost:8081 --property print.key=true  --property print.value=true --key-deserializer org.apache.kafka.common.serialization.StringDeserializer
# --value-deserializer io.confluent.kafka.serializers.KafkaAvroDeserializer


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

    # Serve on_delivery callbacks from previous calls to produce()


# id: str = "test123"
# first_name: str = 'deepak'
# last_name: str = 'sharma'
# dob = datetime.date.today().isoformat()

# address1 = Address(
#     address_line1="Address Line 1",
#     address_line2="address line 2",
#     city="Menands",
#     state="NY",
#     zip=12345,
#     address_type=AddressType.residential.value
#
# )
#
# address2 = Address(
#     address_line1="Line 1",
#     address_line2="Line 2",
#     city="Albany",
#     state="NY",
#     zip=12345,
#     address_type=AddressType.residential.value
#
# )
# address2.model_dump(mode='python')
# user = Person(
#     id=id,
#     first_name=first_name,
#     last_name=last_name,
#     dob=dob,
#     address=[address1, address2]
#
# )


def address_to_dict(addresses: List[Address]):
    """
    Returns a dict representation of a User instance for serialization.

    Args:
        user (User): User instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with user attributes to be serialized.
        :param addresses:
    """

    address_dict = [address.model_dump() for address in addresses]
    return address_dict


def person_to_dict(person: Person, ctx):
    """
    Returns a dict representation of a User instance for serialization.

    Args:
        person (User): User instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with user attributes to be serialized.
    """
    person_dict = dict(id=person.id, first_name=person.first_name, last_name=person.last_name, dob=person.dob
                       , address=address_to_dict(person.address)
                       )
    return person_dict


def get_schema() -> str:
    path = os.path.realpath(os.path.dirname(__file__))
    print(path)
    with open(f"{path}\\schema\\person_nested.avsc") as f:
        schema_str = f.read()
    #print(schema_str)
    return schema_str


async def send_message(person: Person) -> uuid.UUID:
    conf = {'bootstrap.servers': 'localhost:9092'}
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    schema_registry_conf = {'url': "http://localhost:8081"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    topic: str = "person"
    schema_str = get_schema();
    string_serializer = StringSerializer('utf_8')
    # producer.poll(1)
    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     person_to_dict)

    await producer.start()
    id: uuid.UUID = uuid.uuid4()
    result = await producer.send(topic=topic,
                                 key=string_serializer(str(id)),
                                 value=avro_serializer(person, SerializationContext(topic, MessageField.VALUE)))

    #await producer.flush()
    await producer.stop()
    return id

    # producer.produce(topic=topic,
    #                  key=string_serializer(str(uuid.uuid4())),
    #                  value=avro_serializer(person, SerializationContext(topic, MessageField.VALUE)),
    #                  on_delivery=acked)

    # print("\nFlushing records...")
    # producer.flush()

# conf = {'bootstrap.servers': 'localhost:9092'}
#
# producer = Producer(conf)
# schema_registry_conf = {'url': "http://localhost:8081"}
# schema_registry_client = SchemaRegistryClient(schema_registry_conf)
# topic: str = "person"
#
# schema_str = kafka_helper.get_schema();
#
# string_serializer = StringSerializer('utf_8')
# producer.poll(1)
# avro_serializer = AvroSerializer(schema_registry_client,
#                                  schema_str,
#                                  person_to_dict)
#
#
# producer.produce(topic=topic,
#                  key=string_serializer(str(uuid.uuid4()),
#                  value=avro_serializer(user, SerializationContext(topic, MessageField.VALUE)),
#                  on_delivery=acked)

# print("\nFlushing records...")
# producer.flush()
