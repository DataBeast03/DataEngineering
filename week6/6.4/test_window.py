
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from pprint import pprint

import time

print 'Initializing ssc'
ssc = StreamingContext(SparkContext(), batchDuration=1)

print 'Initializing rdd_queue'
rdd_queue = []
for i in xrange(5): 
    rdd_data = xrange(1000)
    rdd = ssc.sparkContext.parallelize(rdd_data)
    rdd_queue.append(rdd)
pprint(rdd_queue)

print 'Creating queue stream'
ds = ssc\
    .queueStream(rdd_queue)\
    .map(lambda x: (x % 10, 1))\
    .window(windowDuration=4,slideDuration=2)\
    .reduceByKey(lambda v1,v2:v1+v2)
ds.pprint()

print 'Starting ssc'
ssc.start()
time.sleep(20)

print 'Stopping ssc'
ssc.stop(stopSparkContext=True, stopGraceFully=True)