Week 7: Speed Layer
------------------------------------------

1. __Serving Layer Level Assessment__
    - **Before class:** Prep 10 min presentation showcasing your serving layer
    - **Standard:** Generalize serving layer to new problem
		+ Provide SparkSQL interface to batch layer

2. __Generating Realtime Views__
	- **Standard:** Develop speed layer for realtime pageviews
        + Enqueue page-views in Kafka
        + Dedupe and normalize using Spark Streaming
        + Store Pageviews over time in HBase
        + Expire data in HBase as appropriate
    - **Resources:**
        + [Spark Streaming with Kafka & HBase Example](http://henning.kropponline.de/2015/04/26/spark-streaming-with-kafka-hbase-example/)

3. __Data Engineering in Review__
    - **Standard:** Produce query-able Lambda Architecture
        + Integrate speed & serving layers

4. __Speed Layer Level Assessment__
    - **Before class:** Prep 10 min presentation showcasing your speed layer
    - **Standard:** Generalize speed layer to new problem
		+ Enable multi-consumer queues using Kafka
		+ Achieve exactly-one semantics in streaming
		+ Store realtime views in HBase
