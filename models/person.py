from pydantic import BaseModel, UUID4, Field
from uuid import UUID
from typing import Optional, List
from datetime import date
from enum import Enum


class AddressType(Enum):
    mailing = 'Mailing'
    residential = 'Residential'


class PhoneNumberType(Enum):
    cell_phone = 'Cell'
    mobile_phone = 'Mobile'


class PhoneNumber(BaseModel):
    area_code: int = Field(ge=99, le=1000)
    phone_number: int = Field(ge=999999, le=10000000)
    type: PhoneNumberType


class Address(BaseModel,use_enum_values=True):
    address_line1: str
    address_line2: Optional[str] = None
    city: str
    state: str
    zip: int
    address_type: AddressType


class Person(BaseModel):
    id: str
    first_name: str
    middle_name: Optional[str] = None
    last_name: str
    dob: str
    address: List[Address]
    # phones:list[PhoneNumber]
