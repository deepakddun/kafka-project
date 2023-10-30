import fastavro
from fastavro import parse_schema
from fastavro.validation import validate_many

household_schema = {
    "namespace": "example.avro",
    "type": "record",
    "name": "Person",
    "fields": [
        {
            "name": "id",
            "type": "string"
        },

        {
            "name": "address",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "Address",
                    "fields": [
                        {
                            "name": "address_line1",
                            "type": "string"
                        }

                    ]
                }
            }
        }
    ]
}

records = [
    {
        "id": "123 Drive Street",
        "address": [
            {
                "address_line1": "no"
            }
        ]
    }
]

parsed_schema = parse_schema(household_schema)
validate_many(records, parsed_schema)
