import io, random, threading, logging, time
import simplejson as json
from data_gen import get_datum

from pyspark.streaming.kafka import KafkaUtils
from kafka.client   import KafkaClient
from kafka.consumer import KafkaConsumer
from kafka.producer import SimpleProducer

KAFKA_TOPIC = 'amazon-topic'


# kafka pluin for spark streaming will only accept utf-8
# CAN'T SEND BINARY CODE

class Producer(threading.Thread):
    '''Produces users and publishes them to Kafka topic.'''
    daemon = True
    def run(self):
        # default settings: do not generate new user ids, upload and use existing ides
        gen = get_datum()
        client = KafkaClient('localhost:9092')
        producer = SimpleProducer(client)
        
        while True:
            raw_bytes = json.dumps(gen.next()).encode('utf-8')
            producer.send_messages(KAFKA_TOPIC, raw_bytes)
        time.sleep(1)

if __name__ == "__main__":

	p = Producer()
	p.run()