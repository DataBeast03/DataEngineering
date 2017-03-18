Part 0: Spark Installation
--------------------------

Follow these steps for installing PySpark on your laptop.

1. Go to this [link](http://spark.apache.org/downloads.html). 

2. Select `Pre-built for Hadoop 2.4` or earlier under `Choose a
   package type:`. (Note: This is important. Versions after Hadoop 2.4
   have a bug and don't work with Amazon S3.)

3. Download the tar package for `spark-1.4.1-bin-hadoop1.tgz`. If you
   are not sure pick the latest version.

4. Make sure you are downloading the binary version, not the source
   version.

5. Unzip the file and place it at your home directory.

6. Include the following lines in your `~/.bash_profile` file on Mac
   (without the brackets).

   ```
   export SPARK_HOME=[FULL-PATH-TO-SPARK-FOLDER]
   export PYTHONPATH=[FULL-PATH-TO-SPARK-FOLDER]/python:$PYTHONPATH
   ```

7. Install py4j using `sudo pip install py4j`

8. Open a new terminal window.

9. Start ipython console and type `import pyspark as ps`. If this did
   not throw an error, then your installation was successful.

10. Start `ipython notebook` from the new terminal window.

11. If PySpark throws errors about Java you might need to download the
    newest version of the
    [JDK](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html).
    
    
Part 1: RDD and Spark Basics
----------------------------

Lets get familiar with the basics of Spark (PySpark). We will
be using Spark in local mode. 

1. Initiate a `SparkContext`. A `SparkContext` specifies where your
   cluster is, i.e. the resources for all your distributed
   computation. Specify your `SparkContext` as follows.
   
   ```python
   import pyspark as ps
   # Uses all 4 cores on your machine
   sc = ps.SparkContext('local[4]') 
   ```

2. Spark keeps your data in **Resilient Distributed Datasets (RDDs)**.
   **An RDD is a collection of data partitioned across machines**.
   Each group of records that is processed by a single thread (*task*) on a
   particular machine on a single machine is called a *partition*.

   Using RDDs Spark can process your data in parallel across
   the cluster. 
   
   You can create an RDD from a list, from a file or from an existing
   RDD.
   
   Lets create an RDD from a Python list.
   
   ```python
   list_rdd = sc.parallelize([1, 2, 3])
   ```
   
   Read an RDD in from a text file. **By default, the RDD will treat
   each line as an item and read it in as string.**
   
   ```python
   file_rdd = sc.textFile('https://s3-us-west-2.amazonaws.com/dsci/6007/data/toy_data.txt')
   ```

3. RDDs are lazy so they do not load the data from disk unless it is
   needed. Each RDD knows what it has to do when it is asked to
   produce data. In addition it also has a pointer to its parent RDD
   or a pointer to a file or a pointer to an in-memory list.

   When you use `take()` or `first()` to inspect an RDD does it load
   the entire file or just the partitions it needs to produce the
   results? Exactly. It just loads the partitions it needs.
 
   ```python
   file_rdd.first() # Views the first entry
   file_rdd.take(2) # Views the first two entries
   ```
    
4. If you want to get all the data from the partitions to be sent back
   to the driver you can do that using `collect()`. However, if your
   dataset is large this will kill the driver. Only do this when you
   are developing with a small test dataset.
   
   ```python
   file_rdd.collect()
   list_rdd.collect()
   ```

Part 2: Transformations and Actions
-----------------------------------

Use
<http://real-chart.finance.yahoo.com/table.csv?s=AAPL&g=d&ignore=.csv>
to download the most recent stock prices of AAPL, and save it to
`aapl.csv`.

        import urllib2
        url = 'http://real-chart.finance.yahoo.com/table.csv?s=AAPL&g=d&ignore=.csv'
        csv = urllib2.urlopen(url).read()
        with open('aapl.csv','w') as f: f.write(csv)        

Q: How many records are there in this CSV?

Q: Find the average *adjusted close* price of the stock. Also find the
min, max, variance, and standard deviation.

Q: Find the dates of the 3 highest adjusted close prices.

Q: Find the date of the 3 lowest adjusted close prices.

Q: Find the number of days on which the stock price fell, i.e. the
close price was lower than the open.

Q: Find the number of days on which the stock price rose.

Q: Find the number of days on which the stock price neither fell nor
  rose.

Q: To find out how much the stock price changed on a particular day,
convert the close and the open prices to natural log values using
`math.log()` and then take the difference between the close and the
open. This gives you the log change in the price. Find the 3 days on
which the price increased the most.

Q: The log change price lets you calculate the average change by
taking the average of the log changes. Calculate the average change in
log price over the entire range of prices.

Part 3: Extra Credit
--------------------

Q: Write a function that given a string date gives you the weekday.
Here is code that calculates the weekday for 2015/05/05. This returns
an integer. `0` is Monday, `1` is Tuesday, etc.

Q: Using this function calculate the weekday for all the stock prices,
and the log change in the price on that day. Convert the log change
back to percentage change. To convert log change to percentage take
use `percent_change = math.exp(log_change) - 1`.

Q: Does the price change more on some days and less on others?
