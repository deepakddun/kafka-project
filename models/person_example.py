from models.person import Address, AddressType

address2 = Address(
    address_line1="Line 1",
    address_line2="Line 2",
    city="Albany",
    state="NY",
    zip=12345,
    address_type=AddressType.mailing.mailing.value

)
print((address2.model_dump()))