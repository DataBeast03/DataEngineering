'''
run with $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.4.1 new_consumer.py 
'''

import simplejson as json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

KAFKA_TOPIC = 'my-topic'

if __name__ == "__main__":
    sc = SparkContext(appName="SpeedLayer")
    ssc = StreamingContext(sc, 1)

    zkQuorum, topic = 'localhost:2181'
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {KAFKA_TOPIC: 1})

    # kvs.map(lambda x: (str(type(x)), len(x), x)).pprint()
    kvs.pprint()

    ssc.start()
    ssc.awaitTermination()
