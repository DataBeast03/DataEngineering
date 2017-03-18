Miniquiz
--------

1. What does *RDD* stand for?

Resilient Distributed Data Set
Resilient --> if a machine goes down, the missing data can be rebuilt 

2. Is `count()` a transformation or an action.

action:         will always return an object or print statement (the preperation)
transformation: creates an RDD, a function call on the data (the execution)


3. In `sc.parallelize(xrange(10)).filter(lambda x:x%2==0).collect()`
   where does `lambda` execute? On the driver or the executors? Also,
   where does `xrange(10)` execute?
   
driver:   'parallelize'
executor: 'lambda'
executor: 'xrange(10)'
   

4. In `sc.parallelize(xrange(10)).filter(lambda x:x == 0)` where does
   `lambda` execute? On the driver or the executors?

driver: 'lambda'

this function is written and then push up to the drive and it sits there until 
we call an action on it

5. Write a Spark job that squares all numbers from 0 to 99.

sc.parallelize(xrange(100)).map(lambda x: x**2).collect()

6. If I read a file that is 100 MB from S3: What is the default
   partition size? How many partitions will hold this file?

partition size: 4 , each partition in S3 holds 32MB


7. Bonus: How does `.top()` work?

