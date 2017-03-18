# HDFS Lab 1

HDFS Lab 1 consists of the following tasks:

- [Set up 4-node HDFS cluster on EMR](#Setup)
- [HDFS Load Data and Run WordCount](#LoadData)
- [Running Applications using Hortonworks Sandbox](#Application)

<a name="Setup"></a>

## Set up 4-node HDFS cluster on EMR

The first lab will be to create a 4-node cluster on Amazon's Elastic MapReduce
environment. This assumes that you have already created an account with Amazon
Web Services (http://aws.amazon.com/)

### Amazon Key Pairs

After logging into AWS, you need to create a key pair (a `pem` file)
to log into the cluster. 

**NOTE:** Use your `pem` file name instead of `emr_training` as you
follow this lab.

### Create EMR Cluster

Go to the main AWS page, click on the **EMR** item under the **Analytics**
section as shown below:

<img src="images/AWS_EMR.png">

The next step is to create an Elastic MapReduce cluster by clicking on **Create
cluster**.

<img src="images/Create_EMR.png">

On the **Quick cluster configuration** screen enter the following:

Item                 |Value
----                 |-----
Cluster name         |`HDFS_Training`
Applications         |Core Hadoop: Hadoop 2.6.0, Hive 1.0.0, and Pig 0.14.0
Number of Instances  |`4`
EC2 key pair         |`emr_training` (or a key you have previously created)

Press **Create cluster** to continue

<img src="images/EMR_Building.png">

The screen will refresh periodically as the cluster is being built.
Once the cluster has been built, you should be able to ssh into the cluster.
Click on the **SSH** link beside **Master public DNS:** as shown below:

<img src="images/EMR_SSH_Link.png">

A screen will appear providing the details on how to connect to the cluster:
<img src="images/EMR_SSH_Details.png">
Copy the *ssh* line as follows:

    ssh hadoop@ec2-54-186-203-34.us-west-2.compute.amazonaws.com -i ~/emr_training.pem

***NOTE:*** Replace the path to `emr_training.pem` with the path to your `pem` file.

Here is the output you should see.

    $ ssh hadoop@ec2-54-186-203-34.us-west-2.compute.amazonaws.com -i emr_training.pem
    Last login: Tue Aug 11 18:01:49 2015

           __|  __|_  )
           _|  (     /   Amazon Linux AMI
          ___|\___|___|

    https://aws.amazon.com/amazon-linux-ami/2015.03-release-notes/
    14 package(s) needed for security, out of 26 available
    Run "sudo yum update" to apply all updates.

    EEEEEEEEEEEEEEEEEEEE MMMMMMMM           MMMMMMMM RRRRRRRRRRRRRRR
    E::::::::::::::::::E M:::::::M         M:::::::M R::::::::::::::R
    EE:::::EEEEEEEEE:::E M::::::::M       M::::::::M R:::::RRRRRR:::::R
      E::::E       EEEEE M:::::::::M     M:::::::::M RR::::R      R::::R
      E::::E             M::::::M:::M   M:::M::::::M   R:::R      R::::R
      E:::::EEEEEEEEEE   M:::::M M:::M M:::M M:::::M   R:::RRRRRR:::::R
      E::::::::::::::E   M:::::M  M:::M:::M  M:::::M   R:::::::::::RR
      E:::::EEEEEEEEEE   M:::::M   M:::::M   M:::::M   R:::RRRRRR::::R
      E::::E             M:::::M    M:::M    M:::::M   R:::R      R::::R
      E::::E       EEEEE M:::::M     MMM     M:::::M   R:::R      R::::R
    EE:::::EEEEEEEE::::E M:::::M             M:::::M   R:::R      R::::R
    E::::::::::::::::::E M:::::M             M:::::M RR::::R      R::::R
    EEEEEEEEEEEEEEEEEEEE MMMMMMM             MMMMMMM RRRRRRR      RRRRRR

    [hadoop@ip-172-31-18-92 ~]$


You have now successfully connected to your EMR cluster.

We can test that we can list the files in the HDFS filesystem as follows:

    [hadoop@ip-172-31-18-92 ~]$ hadoop fs -ls /
    Found 7 items
    drwxrwxrwx   - hdfs   hadoop          0 2015-08-11 17:22 /benchmarks
    drwxr-xr-x   - hbase  hbase           0 2015-08-11 17:22 /hbase
    drwxr-xr-x   - hadoop hadoop          0 2015-08-11 17:22 /mnt
    drwxr-xr-x   - solr   solr            0 2015-08-11 17:22 /solr
    drwxrwxrwt   - hdfs   hadoop          0 2015-08-11 17:23 /tmp
    drwxr-xr-x   - hdfs   hadoop          0 2015-08-11 17:22 /user
    drwxr-xr-x   - hdfs   hadoop          0 2015-08-11 17:22 /var
    [hadoop@ip-172-31-18-92 ~]$


<a name="LoadData"></a>

## HDFS Load Data and Run WordCount

- Create directories
- Upload sample file
- Run WordCount on const.txt
- Terminal EMR Cluster

### Hadoop FS command

**Step 1:** Type `hadoop fs` to get a list of all the sub-commands
this has. 

**Step 2:** Type `hadoop fs -help` to get detailed information on the
sub-commands.

We are going to use the `hadoop fs` sub-commands extensively in this
exercise.

### Create directories

Create a directory in HDFS called `/data/movielens`. What sub-command
will do this for you?

Check that the directory exists using `hadoop fs -ls -R /data`
 
### Upload sample file

We will download some sample data to run a WordCount test against this. We will
download the U.S. Constitution text as the sample file to use.

**Step 1:** Use `wget` to download the Constitution from
<https://www.usconstitution.net/const.txt> on your EMR instance.

**Step 2:** Run `ls -l` to verify that it is in the current directory.

Note that this is not in HDFS yet. It is on your local file system.

**Step 3:** Put `const.txt` into `/data/movielens/const.txt` on HDFS.
What sub-command will do this for you?

**Step 4:** Run `hadoop fs -ls /data/movielens` to verify that `const.txt`
has been uploaded. It should appear in the output.

### Run WordCount on const.txt

**Step 1:** Run word count on this file in HDFS.

    hadoop jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar \
        wordcount /data/movielens/const.txt wordcount_output`

**Step 2:** View the output of the job in `wordcount_output` in HDFS.
What sub-command will do this for you?

**Step 3:** View the first few lines of the output. What sub-command will
do this for you?



### Use distcp to copy directories from S3 to HDFS

#### Download the dataset 'Movielens Latest'

- URL [http://grouplens.org/datasets/movielens/](http://grouplens.org/datasets/m
ovielens/)

#### Upload the file to S3

- Follow the instrutions under 'Create and Configure an Amazon S3
  Bucket' at this URL:
  <http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-plan-upload-s3.html>

- Click your new bucket and upload the Movielens zip file.

#### Copy file from S3 to HDFS using distcp

Use the DistCp tool to copy the MovieLens file from S3 to HDFS. 

    hadoop distcp s3://AWS_ACCESS_KEY:AWS_SECRET_KEY@BUCKET_NAME/ml-latest.zip /training

Replace the `AWS_ACCESS_KEY`, `AWS_SECRET_KEY`, and `BUCKET_NAME`
appropriately.

#### Bug: Keys Must Not Contain Forward Slash

If your AWS access key or secret key has a `/` character in it
`distcp` will not parse the s3 path correctly. 

The bug for this issue is still open at
<https://issues.apache.org/jira/browse/HADOOP-3733>. 

The workaround is to regenerated AWS keys until you get keys without
`/`.

#### A note on distcp:

"The distcp tool is useful for quickly prepping S3 for MapReduce jobs that use
S3 for input or for backing up the content of hdfs."

Apache DistCp is an open-source tool you can use to copy large amounts of data.
DistCp uses MapReduce to copy in a distributed manner
S3DistCp is an extension of DistCp that is optimized to work with AWS,
particularly Amazon S3.
More info: <http://wiki.apache.org/hadoop/AmazonS3>


### Terminate EMR Cluster

After completing this exercise, go into Amazon AWS and click on **Terminate** to
remove the cluster we have just built.

<a name="Application"></a>

## Running Applications using Hortonworks Sandbox

- Why use the sandbox instead of EMR
- Downloading and Installing Hortonworks Sandbox
- Use hadoop examples jar's grep, word-count, and wordmean
- Use use these to split movie reviews into positive and negative
- Run word count on the two sets of files
- See what words are most common in each set

### Why use the sandbox instead of EMR

Q: What is the Hortonworks Sandbox?

- Instead of running Hadoop on a cluster the Hortonworks Sandbox lets
  you run it locally in a virtual machine.

Q: Why would I need the sandbox? 

- While in production you will run a cluster on Amazon, for
  development and testing it is a lot easier to spin up a one-machine
  cluster locally. 

- It is a great training and learning environment for experimenting
  with new technologies.

- It is a great development environment where you can use small data
  sets to verify that your application logic is correct.

- Finally once everything is tested you can run your analysis on real
  data on EMR.
  
- Also this is (a lot) cheaper.

### Downloading and Installing Hortonworks Sandbox

The remainder of this lab will use a local copy of the Hortonworks Sandbox.
Please ensure you have **terminated** the EMR Cluster in Amazon AWS prior to
continuing this lab.

Go to the Hortonworks Sandbox download page - [http://hortonworks.com/products
/hortonworks-sandbox/#install](http://hortonworks.com/products/hortonworks-
sandbox/#install)

Download the latest version. This assumes that you have a local environment for
VirtualBox (free download from
[https://www.virtualbox.org/](https://www.virtualbox.org/)), VMware (Workstation
or Fusion), or Hyper-V to run the Virtual Machine. Download the appropriate VM
for your local environment (which is about 7GB). Instructions for importing the
Virtual Appliance are available on the Hortonworks Sandbox page. Once the
Sandbox has started, the screen should look like the following.

<img src="images/Sandbox.png">

Login to the Sandbox using the instructions for ssh on the screen.

### Use hadoop examples jar's grep,  word-count, and wordmean

**Step 1:** Download and uncompress Shakespeare's works.

Download the dataset 'Complete Works of William Shakespeare' from
<http://www.it.usyd.edu.au/~matty/Shakespeare/shakespeare.tar.gz> in
the sandbox instance.

    [root@sandbox ~]# wget http://www.it.usyd.edu.au/~matty/Shakespeare/shakespeare.tar.gz

Unzip the William Shakespeare data

    mkdir shakespeare
    cd shakespeare
    tar xvfz ../shakespeare.tar.gz

Make sure you have all the files.

    ls -l

**Step 2:** Create a direcgtory in HDFS called `/data/shakespeare`

**Step 3:** Verify using `hadoop fs -ls -R /data/shakespeare` that your upload worked.

**Note:** For the next three exercises we will be running the grep,
wordcount, and wordmean from the *hadoop-mapreduce-examples.jar* file.
This is located in the **/usr/hdp/<version>/hadoop-mapreduce**
directory. Please replace the `current` in the paths below with the
current Sandbox version you are running with.

#### Grep

Use this command to find the number of times that `the` occurs in
`/data/shakespeare/comedies`.

    yarn jar /usr/hdp/current/hadoop-mapreduce-client \
        /hadoop-mapreduce-examples.jar grep /data/shakespeare/comedies /data/grep_out \
        the

Look at the contents of `/data/grep_out` in HDFS to find out how many
times `the` occurred.

#### Exploring Examples

Run this command to get a list of all the sub-programs under
`hadoop-mapreduce-examples.jar`

    yarn jar /usr/hdp/current/hadoop-mapreduce-client \
        /hadoop-mapreduce-examples.jar

#### Word-Count

Run `wordcount` against `/data/shakespeare/comedies`. This calculates
the frequency of all the words.

Look at the output directory to verify that the command worked and
produced results.

#### Wordmean

Run `wordmean` against `/data/shakespeare/comedies`. This calculates
the average word length across all the words.
