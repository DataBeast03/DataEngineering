Quiz
====

1. If you have a consumer group with 8 consumer processes in it, how many
partitions should your topic have to fully utilize all the consumers?

<<<<<<< HEAD
    8 partitions 

2. If a consumer fails and restarts how does Kafka ensure that it does
not lose the messages that arrived while it was offline?
        Broker will hold on to the data, for up to 7 days, 
        as soon as the consumer is back online, the broker
        will pass messages to the consumer exactly where it
        left off last

3. Can the same producer write to multiple topics?
    Yes
       

4. Can the same consumer read from multiple topics?
    No
    Can think of each topic as a streaming table (from lambda project)
    

5. What is the benefit of increasing replication?
    not lossing data 

6. What is the benefit of increasing partitions?
    allows to spread out the data load in order to scale horizontally 
=======
2. If a consumer fails and restarts how does Kafka ensure that it does
not lose the messages that arrived while it was offline?

3. Can the same producer write to multiple topics?

4. Can the same consumer read from multiple topics?

5. What is the benefit of increasing replication?

6. What is the benefit of increasing partitions?
>>>>>>> 42ec54c5b49980d837de660dbe29f2ec2ab774c4
