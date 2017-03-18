Lab
===============================

Partition the data in `s3://dsci/6007/data/SuperWebAnalytics/new_data/` into a master dataset as follows:

The unions within a graph schema provide a natural vertical partitioning scheme for a dataset.

    /data/
          person_property/
                          full_name/
                          gender/
                          location/
                          age/
          page_property/
                        page_views/
          equiv/
          page_view/
          page_link/

Write a batch job to vertically partition avro data according to the above scheme.  
*i.e.* Edges are partitioned according type. Properties are partitioned according to subtype.
1. It is important that your solution scale horizontally. Writing a Python script load the data into memory and write it into different local files is easy. Writing a script that will leverage Hadoop or Spark's distributed architecture is a little more challenging.
2. Don't forget about the small-files problem. 

***Hints*:**  
* MapReduce, whether implemented in Hadoop or in Spark, can only write to a single output directory at a time. As a result, it will be necessary to iterate over the dataset with filters in order to partition the data into different folders. For this it is recommended that you cache the data in an RDD.
* Spark's implementation of Avro is incomplete. It is therefore recommended that you use a different Avro library (*e.g.* [fastavro](https://pypi.python.org/pypi/fastavro/)). Libraries like this require a file object. If you are using Python, you may use [StringIO](https://docs.python.org/2/library/stringio.html#module-cStringIO) to solve this problem.
* Warning, be careful when dynamically generating filters. `lambda` in Python passes variables by reference, not by value, so you might find that you appear to be filtering on the same thing as you iterate through partitions. `def` saves a copy of the variable in the function.  
*e.g.* 
```python
rdd0 = sc.parallelize(range(10))
rdds = [rdd0.filter(lambda x: x%2 == i) for i in range(2)]
rdds[1].first() == rdds[0].first()
```
but
```python
def mod2_equals(i): return lambda x: x%2 == i
rdds = [rdd0.filter(mod2_equals(i)) for i in range(2)]
rdds[1].first() != rdds[0].first()
```
