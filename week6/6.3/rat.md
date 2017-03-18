1. Of the four requirements for a serving layer, that it be:  
	- Batch writable
	- Scalable
<<<<<<< HEAD
	- Random reads    <-------------
	- Fault-tolerant  
	which of these is _not_ needed for the batch layer?  

2. True or False: batch views in the serving layer should always be normalized. 
         False, denormalized batch views may lower latency

3. Which serialization framework (Avro versus Parquet) is better for new data, which is better for batch views, and why?
        Avro for new data because avro stores data in a row format
        parquet for batch views, parquet stores data as columns and aggragations are a colum acton

4. How should data in batch views be organized?
        Sorted index
=======
	- Random reads
	- Fault-tolerant  
	which of these is _not_ needed for the batch layer?  

2. True or False: batch views in the serving layer should always be normalized.

3. Which serialization framework (Avro versus Parquet) is better for new data, which is better for batch views, and why?

4. How should data in batch views be organized?
>>>>>>> 42ec54c5b49980d837de660dbe29f2ec2ab774c4
