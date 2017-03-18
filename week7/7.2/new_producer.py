'''
python new_producer.py
'''
import time
import simplejson as json

from kafka import SimpleProducer, KafkaClient

from gen_data import get_datum

# To send messages synchronously
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)

# Kafka topic
KAFKA_TOPIC = "my-topic"

for i in xrange(10): 
    raw_bytes = json.dumps(get_datum()).encode('utf-8')
    producer.send_messages(KAFKA_TOPIC, raw_bytes)
    time.sleep(1)