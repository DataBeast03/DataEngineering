Week 4: Batch Layer
------------------------------------------

1. __Vertical Partitioning with HDFS__
    - **Standard:** Develop file schema & stage data for batch process
        + Define vertical partitions in HDFS
        + Write script to load and partition data
        + Horizontally scale ETL using Hadoop/Spark
    - **Resources:**
        + [The Small Files Problem](http://blog.cloudera.com/blog/2009/02/the-small-files-problem/)
        + [HDFS for the Batch Layer](https://dzone.com/articles/hdfs-batch-layer)
        + [How NOT to pull from S3 using Apache Spark](http://tech.kinja.com/how-not-to-pull-from-s3-using-apache-spark-1704509219)  
        _Note_: this article recommends using [AmazonS3Client](https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-s3/src/main/java/com/amazonaws/services/s3/AmazonS3Client.java) but [boto](https://github.com/boto/boto3) works just fine
2. __Intro to Hive__ - *Guest Instructor*: Asim Jalis
    - **Standard:** Use Hive for ETL
        + Identify use cases for Hive
        + Explain how Hive differs from RDBMS
        + Install Hive using Ambari
        + Create Hive Tables
        + Query Hive using CLI
3. __Distributed Machine Learning__
    - **Standard:** Employ MLlib to predict user behavior
        + Make movie recommendations with MLlib
        + Explain how ALS differs from SVD
        + Explain how ALS is done in Spark w/ MLlib
        + Use cross-validation on a parameter grid
    - **Resources:**
        + [Collaborative Filtering for Implicit Feedback Datasets](http://yifanhu.net/PUB/cf.pdf)
        + [Large-scale Parallel Collaborative Filtering for
the Netflix Prize](http://www.grappa.univ-lille3.fr/~mary/cours/stats/centrale/reco/paper/MatrixFactorizationALS.pdf)
4. __Generating Batch Views__
    - **Standard:** Develop a DAG for batch processing
        + Compare recomputation & incremental algorithms
        + Construct a batch layer to produce aggregate tables
        + Isolate batch jobs to maximize flexibility
        + Pick appropriate keys for batch views
    - **Resources:**
        + [HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm](http://research.google.com/pubs/archive/40671.pdf)
