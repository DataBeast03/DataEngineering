# Import modules.

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from pprint import pprint

import time

# Create the StreamingContext.

print 'Initializing ssc'
ssc = StreamingContext(SparkContext(), batchDuration=1)


# For testing create prepopulated QueueStream of streaming customer orders. 

print 'Initializing queue of customer transactions'
transaction_rdd_queue = []
for i in xrange(5): 
    transactions = [(customer_id, None) for customer_id in xrange(10)]
    transaction_rdd = ssc.sparkContext.parallelize(transactions)
    transaction_rdd_queue.append(transaction_rdd)
pprint(transaction_rdd_queue)

# Batch RDD of whether customers are good or bad. 

print 'Initializing bad customer rdd from batch sources'
# (customer_id, is_good_customer)
customers = [
        (0,True),
        (1,False),
        (2,True),
        (3,False),
        (4,True),
        (5,False),
        (6,True),
        (7,False),
        (8,True),
        (9,False) ]
customer_rdd = ssc.sparkContext.parallelize(customers)

# Join the streaming RDD and batch RDDs to filter out bad customers.
print 'Creating queue stream'
ds = ssc\
    .queueStream(transaction_rdd_queue)\
    .transform(lambda rdd: rdd.join(customer_rdd))\
    .filter(lambda (customer_id, (customer_data, is_good_customer)): is_good_customer)

ds.pprint()

ssc.start()
time.sleep(6)
ssc.stop()