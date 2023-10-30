import fastavro
from fastavro import writer, parse_schema

schema = {
    'doc': 'A weather reading.',
    'name': 'Weather',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'station', 'type': 'string'},
        {'name': 'addresses', 'type': {'type': 'array',
                                       'items': {
                                           'name': 'Address', 'type': 'record',
                                           'fields': [
                                               {
                                                   'name': 'address_line1',
                                                   'type': 'string'
                                               }
                                           ]
                                       }

                                       }
         }
    ],
}
parsed_schema = parse_schema(schema)

records = [{u'station': u'011990-99999', u'addresses': [{u'address_line1': u'Test'}]}]

fastavro.validate(records, parsed_schema)
with open('weather.avro', 'wb') as out:
    writer(out, parsed_schema, records)
