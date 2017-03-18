##Individual Assignment

###Objectives:

- Contrasting different file formats
- Understanding Partitioning
- Understanding Bucketing
- Understanding Skewed tables.

###Lab:

####File Formats

This lab assumes you have hive and its dependancies installed and running.   

- Step1: Download 100m movies data from
  <http://files.grouplens.org/datasets/movielens/ml-latest-small.zip>
- Step2: Load data into HDFS
- Create `<tablename>_txt` to load data as text file.
- Create `<tablename>_rc` to load data in rc format.
- Create `<tablename>_orc` to load data in orc format.
- Step3: Load the data into above tables from hdfs and note the
  timings. Which table took more time to load?
- Step4: How many movies with the tag `Action` are present in the
  list? Save a list of the titles and IDs of such movie to the HDFS.
  Contrast the timings. *Hint : Case sensitive?*

####Partitioning

- Step1: Review the data in `data/state-pops.csv`  
- Step2: Load it into HDFS.  
- Step3: Create table `states` in hive partitioned on `country`.  
- Step4: Query the description of the table. 
- Step5: Load states with data from HDFS.  
- Step6: Check the directory structure in HDFS.
- The data for each country should be in a separate folder.

####Bucketing

- Step1: Review data in `movies.csv`.
- Step2: Create table `movies1` **without** bucketing.    
- Step3: Create table `movies2` **with bucketing over movieID (4 buckets)**.  
- Step4: Load same data to both tables and notice difference in time.   
- Step5: Run `count(*)` command on both tables and notice difference in time.   
- Step6: Perform sampling on `movies2`.

