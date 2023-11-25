#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# A simple example demonstrating use of AvroDeserializer.

import argparse
import os
from typing import Dict, List

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition, AIOKafkaProducer
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField, SerializationError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from confluent_kafka.serialization import StringSerializer
from models.person import Person
import asyncio
# from confluent_kafka.schema_registry.



# class User:
#     """
#     User record
#
#     Args:
#         name (str): User's name
#
#         favorite_number (int): User's favorite number
#
#         favorite_color (str): User's favorite color
#     """
#
#     def __init__(self, id, first_name, middle_name, last_name, dob, address):
#         self.id = id
#         self.first_name = first_name
#         self.middle_name = middle_name
#         self.last_name = last_name
#         self.dob = dob
#         self.address = address


# def address_to_dict(addresses: List[Address]):
#     """
#     Returns a dict representation of a User instance for serialization.
#
#     Args:
#         user (User): User instance.
#
#         ctx (SerializationContext): Metadata pertaining to the serialization
#             operation.
#
#     Returns:
#         dict: Dict populated with user attributes to be serialized.
#         :param addresses:
#     """
#
#     address_dict = [address.model_dump() for address in addresses]
#     return address_dict


# def person_to_dict(person: Person, ctx):
#     """
#     Returns a dict representation of a User instance for serialization.
#
#     Args:
#         person (User): User instance.
#
#         ctx (SerializationContext): Metadata pertaining to the serialization
#             operation.
#
#     Returns:
#         dict: Dict populated with user attributes to be serialized.
#     """
#     person_dict = dict(id=person.id, first_name=person.first_name, last_name=person.last_name, dob=person.dob
#                        , address=address_to_dict(person.address)
#                        )
#     return person_dict


def dict_to_user(obj, ctx):
    """
    Converts object literal(dict) to a User instance.

    Args:
        obj (dict): Object literal(dict)

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """

    if obj is None:
        return None
    print(obj)
    return Person(id=obj['id'],
                  first_name=obj['first_name'],
                  middle_name=obj['middle_name'],
                  last_name=obj['last_name'],
                  dob=obj['dob'],
                  address=obj['address']
                  )


async def main():
    topic = "person"
    #  is_specific = args.specific == "true"

    schema = "person_nested.avsc"

    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/schema/{schema}") as f:
        schema_str = f.read()

    sr_conf = {'url': 'http://localhost:8081'}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str,
                                         dict_to_user)

    consumer_conf = {'bootstrap.servers': 'localhost:9092',
                     'group.id': 'test',
                     'auto.offset.reset': "latest",
                     }

    # consumer = AIOKafkaConsumer(bootstrap_servers='localhost:9092',
    #                             group_id='test',
    #                             auto_offset_reset='earliest'
    #                             )

    #consumer = AIOKafkaConsumer(*consumer_conf,)
    consumer = AIOKafkaConsumer(bootstrap_servers="localhost:9092",group_id="test",auto_offset_reset="latest",
                                enable_auto_commit=False
                                )
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')

    consumer.subscribe([topic])
    await consumer.start()
    while True:
        try:

            # SIGINT can't be handled when polling, limit timeout to 1 second.
            messages: Dict[TopicPartition, List[ConsumerRecord]] = await consumer.getmany(timeout_ms=1500)
            print("polling...")
            # print(msg.value())
            if messages is None:
                continue
            for message in messages.values():
                msg = message.pop()
                try:
                    user = avro_deserializer(msg.value, SerializationContext(msg.topic, MessageField.VALUE))

                    if user is not None:
                        # print("User record with key {}: id : {}\n"
                        #       "\tfirst Name: {}\n"
                        #       "\tlast name: {}\n"
                        #       .format(msg.key(), user.id , user.first_name, user.last_name))
                        print(
                            f'{user.id} , First Name = {user.first_name}, Last Name = {user.last_name} , Address = {user.address}'

                        )

                except SerializationError as e:
                    print(msg.value)
                    print("Inside Serialization Exception block")
                    print(e)
                    # send to dead letter queue
                    await producer.start()
                    result = await producer.send(topic="deadletterqueue",
                                                 value=msg.value)

                    await producer.flush()
                    #await producer.stop()


            await consumer.commit()
        except KeyboardInterrupt as li:
            print(li)
        except Exception as error:
            print(error)
            await consumer.commit()

    await consumer.stop()


if __name__ == '__main__':
    # parser = argparse.ArgumentParser(description="AvroDeserializer example")
    # parser0.add_argument('-b', dest="bootstrap_servers", required=True,
    #                     help="Bootstrap broker(s) (host[:port])")
    # parser.add_argument('-s', dest="schema_registry", required=True,
    #                     help="Schema Registry (http(s)://host[:port]")
    # parser.add_argument('-t', dest="topic", default="example_serde_avro",
    #                     help="Topic name")
    # parser.add_argument('-g', dest="group", default="example_serde_avro",
    #                     help="Consumer group")
    # parser.add_argument('-p', dest="specific", default="true",
    #                     help="Avro specific record")

    try:
        #     result = loop.run_until_complete(main())
        #    loop = asyncio.get_event_loop()
        # io.run(main())
        #   consumer_task = loop.create_task(main())
        # loop.run_until_complete(main())
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
        loop.run_until_complete(main())

    except KeyboardInterrupt as k:
        print("Hello World inside Keyboard Exception ")
    except Exception as e:
        print("Closing")
        print(e.with_traceback())
