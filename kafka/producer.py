import time

from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9094'
        }

producer = Producer(conf)


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


for i in range(10):
    producer.produce("ChildDetails", value="hello world", callback=acked)

    producer.poll(1)