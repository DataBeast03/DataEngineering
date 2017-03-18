from __future__ import division
import io, random, threading, logging, time

import avro.io
import avro.schema

from kafka.client   import KafkaClient
from kafka.consumer import KafkaConsumer
from kafka.producer import SimpleProducer


LOGIN_USERS = ['alice','bob','chas','dee','eve']
LOGIN_OPS = ['login','logout']

KAFKA_TOPIC = 'login-topic'

AVRO_SCHEMA_STRING = '''{
        "namespace": "kafka_lab.avro",
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "date",    "type": "string"},
            {"name": "time",    "type": "string"},
            {"name": "user",    "type": "string"},
            {"name": "op",      "type": "string"},
            {"name": "success", "type": "string"}
        ]
    }
    '''

class AvroSerDe:
    '''Serializes and deserializes data structures using Avro.'''
    def __init__(self, avro_schema_string):
        self.schema = avro.schema.parse(avro_schema_string)
        self.datum_writer = avro.io.DatumWriter(self.schema)
        self.datum_reader = avro.io.DatumReader(self.schema)

    def obj_to_bytes(self, obj):
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        self.datum_writer.write(obj, encoder)
        raw_bytes = bytes_writer.getvalue()
        return raw_bytes

    def bytes_to_obj(self, raw_bytes):
        bytes_reader = io.BytesIO(raw_bytes)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        obj = self.datum_reader.read(decoder)
        return obj

def get_login_event():
    return {
        'date'    : str(time.strftime('%F')),
        'time'    : str(time.strftime('%T')),
        'user'    : random.choice(LOGIN_USERS),
        'op'      : random.choice(LOGIN_OPS),
        'success' : str(bool(random.randint(0,1))) }

class Producer(threading.Thread):
    '''Produces users and publishes them to Kafka topic.'''
    daemon = True
    def run(self):
        avro_serde = AvroSerDe(AVRO_SCHEMA_STRING)
        client = KafkaClient('localhost:9092')
        producer = SimpleProducer(client)
        while True:
            # input generated avro data here
            raw_bytes = avro_serde.obj_to_bytes(get_login_event())
            producer.send_messages(KAFKA_TOPIC, raw_bytes)
            #time.sleep(1)

class Consumer(threading.Thread):
    '''Consumes users from Kafka topic.'''
    daemon = True
    def run(self):
        avro_serde = AvroSerDe(AVRO_SCHEMA_STRING)
        client = KafkaClient('localhost:9092')
        consumer = KafkaConsumer(KAFKA_TOPIC,
                                 group_id='my_group',
                                 bootstrap_servers=['localhost:9092'])
        attempts = 0.0
        success = 0.0
        failure = 0.0

        for message in consumer:
            user = avro_serde.bytes_to_obj(message.value)
            print '--> ' + str(user)
            if user['op'] == 'login':
                attempts += 1.0
                if user['success'] == 'True':
                    success += 1.0
                else:
                    failure += 1.0
            if attempts > 0:
                print "Success Rate {:.2}\n".format(success/attempts)
            if failure >= 2:
                print "{0} Failed Login Attempts !\n".format(failure)
                print "Threat Detected!"


def main():
    '''Starts producer and consumer threads.'''
    threads = [ Producer(), Consumer() ]
    for t in threads:
        t.run()
    time.sleep(5)

if __name__ == "__main__":
    main()




