import datetime
import time
import uuid

import os
from typing import List

import fastavro
import confluent_kafka.serialization
from confluent_kafka import Producer
from confluent_kafka.avro import AvroConsumer
from pathlib import Path
import json

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from models.person import Person, Address, AddressType

# kafka-console-consumer.bat --bootstrap-server http://localhost:9094 --topic person --property schema.registry.url=http://localhost:8081 --property print.key=true  --property print.value=true --key-deserializer org.apache.kafka.common.serialization.StringDeserializer
# --value-deserializer io.confluent.kafka.serializers.KafkaAvroDeserializer
schema_path = Path("C:\\Users\\deepa\\KafkaFastApi\\schema\\person.avsc")

conf = {'bootstrap.servers': 'localhost:9092'
        }

producer = Producer(conf)

path = os.path.realpath(os.path.dirname(__file__))
print(path)
with open(f"{path}\\schema\\person_nested.avsc") as f:
    schema_str = f.read()

print(schema_str)

schema_registry_conf = {'url': "http://localhost:8081"}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

string_serializer = StringSerializer('utf_8')


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

    # Serve on_delivery callbacks from previous calls to produce()


producer.poll(0.0)

id: str = "test123"
first_name: str = 'deepak'
last_name: str = 'sharma'
dob = datetime.date.today().isoformat()

address1 = Address(
    address_line1="Address Line 1",
    address_line2="address line 2",
    city="Menands",
    state="NY",
    zip=12345,
    address_type=AddressType.residential.value

)

address2 = Address(
    address_line1="Line 1",
    address_line2="Line 2",
    city="Albany",
    state="NY",
    zip=12345,
    address_type=AddressType.residential.value

)
address2.model_dump(mode='python')
user = Person(
    id=id,
    first_name=first_name,
    last_name=last_name,
    dob=dob,
    address=[address1, address2]

)


def address_to_dict(addresses: List[Address]):
    """
    Returns a dict representation of a User instance for serialization.

    Args:
        user (User): User instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    # y = [{
    #     'address_line1': "Line 1",
    #     'address_line2': "Line 2",
    #     'city': 'Albany',
    #     'state': 'NY',
    #     'zip': 12345,
    #     'address_type': AddressType.residential.value
    # }]
    y = [address.model_dump() for address  in addresses]
    print(y)
    return y


def user_to_dict(user: Person, ctx):
    """
    Returns a dict representation of a User instance for serialization.

    Args:
        user (User): User instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    x = dict(id=user.id, first_name=user.first_name, last_name=user.last_name, dob=user.dob
             , address=address_to_dict(user.address)
             )
    return x


avro_serializer = AvroSerializer(schema_registry_client,
                                 schema_str,
                                 user_to_dict)

topic: str = "person"
producer.produce(topic=topic,
                 key=string_serializer(id),
                 value=avro_serializer(user, SerializationContext(topic, MessageField.VALUE)),
                 on_delivery=acked)

print("\nFlushing records...")
producer.flush()
