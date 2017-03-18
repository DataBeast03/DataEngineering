Week 6: Serving Layer
------------------------------------------

1. __Batch Layer Level Assessment__  
    - **Before class:** Prep 10 min presentation showcasing your batch layer
    - **Standard:** Generalize batch layer method to new problem
        + Define a fact-based data model
        + Implement model in a serialization framework
        + Implement & justify vertical partitioning scheme
        + Generate batch views from these data

2. __Implementing the Serving Layer__
    - **Standard:** Implement a serving layer in Lambda Architecture
        + Tailor batch views to the queries they serve
        + Provide a new answer to the data-normalization versus
denormalization debate
        + Discuss the advantages of batch-writable, random-read, and no random-write databases
        + Contrast a Lambda Architecture solution with a fully incremental solution

3. __Queueing with Kafka__
    - **Standard:** Build queueing, streaming system with Kafka
        + Install and run Kafka
        + Review Kafka and Zookeeper configs
        + Create Kafka Topics
        + Write Kafka Producers
        + Write a Consumer that outputs to HDFS
        + Add topics for new kinds of sensor data
    - **Resources:**
        + [Kafka as Unix Pipes](http://logallthethings.com/2015/09/15/kafka-by-example-kafka-as-unix-pipes/)

4. __Micro-batch Stream Processing with Spark__