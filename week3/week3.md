Week 3: Map/Reduce and Spark
------------------------------------------

1. __Project Proposals__  
    - **Before class:** Prep 10 min proposal for a big data system of your own design 
        + Use homework from [2.3](https://github.com/zipfian/DSCI6007-student/blob/master/week2/2.3/lab.md) and [2.4](https://github.com/zipfian/DSCI6007-student/blob/master/week2/2.4/lab.ipynb)
    - **Resources:**
        + [lambda-architecture.net](http://lambda-architecture.net/)

2. __Hadoop MapReduce__ 
    - **Before class:** Read [Map-Reduce Intro](https://github.com/zipfian/DSCI6007-student/blob/master/week3/3.2/intro.md)
    - **Standard:** Process data using Hadoop MapReduce
        + Select appropriate key for shuffle/sort
        + Write mapper function
        + Compose map-only job
        + Use counters
        + Write reducer function
        + Write combiner function
        + Deploy mrjob to the cloud (i.e. AWS)
    - **Resources:**
        + [Parallel MapReduce in Python in Ten Minutes](https://mikecvet.wordpress.com/2010/07/02/parallel-mapreduce-in-python/)
        + [Writing an Hadoop MapReduce Program in Python](http://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/)

3. __Intro to Spark__
    - **Standard:** Use Spark to ingest, transform & write data
        + Create RDDs using parallelize, textFile
        + Save RDDs to file using saveAsTextFile
        + Explain the benefits of RDDs
        + Use common RDD transformations
        + Use common RDD actions
        + Use Double RDD actions
        + Name the processes that enable Spark
    - **Resources:**
        + [Resilient Distributed Datasets: A Fault-Tolerant Abstraction for
In-Memory Cluster Computing](https://www.cs.berkeley.edu/~matei/papers/2012/nsdi_spark.pdf)

4. __Spark II__
    - **Standard:** Use Spark to aggregate & process key-value pair
        + Explain how Pair RDDs differ from regular RDDs
        + Use Pair RDD transformations
        + Use cache, persist to save data in RDDs
        + Rebalance data between tasks
        + Setup and tear down tasks using mapPartitions 
        + Send data back from executors to the driver
        + Send data from drivers to executors using closures