import uuid
from typing import List

from kafka_proj.producer_avro import send_message
from models.person import Person, Address


async def copytokafkaobj(person: Person) -> uuid.UUID:
    result = await send_message(person)
    return result


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


def person_to_dict(user: Person, ctx):
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
    person_dict = dict(id=user.id, first_name=user.first_name, last_name=user.last_name, dob=user.dob
                       , address=address_to_dict(user.address)
                       )
    return person_dict
