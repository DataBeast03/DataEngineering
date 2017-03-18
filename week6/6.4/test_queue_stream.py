from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from pprint import pprint

import time
import random

print 'Initializing ssc'
ssc = StreamingContext(SparkContext(), batchDuration=1) # batchDuration is 1 second

print 'Initializing event_rdd_queue'
event_rdd_queue = []
for i in xrange(5):
    events = range(10) * 10
    event_rdd = ssc.sparkContext.parallelize(events)
    event_rdd_queue.append(event_rdd)
pprint(event_rdd_queue)

print 'Building DStream pipeline'
ds = ssc\
    .queueStream(event_rdd_queue) \
    .map(lambda event: (event, 1)) \
    .reduceByKey(lambda v1,v2: v1+v2)
ds.pprint()

print 'Starting ssc'
ssc.start()
time.sleep(6)

print 'Stopping ssc'
ssc.stop(stopSparkContext=True, stopGraceFully=True)