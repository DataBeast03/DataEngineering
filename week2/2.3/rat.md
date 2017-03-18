Miniquiz
--------

Q: Suppose I write a 1-block HDFS file from a program running in a
DataNode. Where will HDFS write the 3 replicas of this block?

HDSF will write the other 3 replicas of the block in seperate engines/data nodes

What are the names of the following daemons?

Q: HDFS master daemon that manages the metadata?

Name Node

Q: HDFS master daemon that compacts the metadata? 

Secondary Name Node compacts the data


Q: HDFS master daemon that is a backup for the main master daemon?

Stand by Name Node 


Q: HDFS worker daemon?

Data Node 


Q: If I run the command `hadoop fs -ls` what directory will this list?

hadoop fs -ls will return the user home directory, the user on the hadoop cluster


I want to change the replication of a file using `setrep`. 

Q: What is the difference between `setrep 10 file1` and `setrep -w 10
file1` with the added `-w`?

I don't know

Q: Which one is faster?

I don't know 
