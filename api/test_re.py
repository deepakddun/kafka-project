import time

import requests
from faker import Faker
from datetime import datetime
import json

fake = Faker()
for i in range(1):
    # fake = Faker()
    data = {
        "id": str(i),
        "first_name": fake.name(),
        "last_name": fake.name(),
        "dob": datetime.now().strftime("%m-%d-%y"),
        "address": [
            {
                "address_line1": fake.building_number(),
                "address_line2": "Line 2",
                "city": fake.city(),
                "state": fake.country_code(),
                "zip": "12345",
                "address_type": "Residential"
            }

        ]
    }
    print(data)

    json_data = json.dumps(data).encode('utf8')
    res = requests.post(url="http://localhost:8000/register", data=json_data)
    time.sleep(2)
    if i == 5:
        res = requests.post(url="http://localhost:8000/register", data=json.dumps(f"Deepak {i}"))
        #time.sleep(100)
    print(res.status_code)


