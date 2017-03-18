Hive 2: Quiz
============

Q1: Suppose you have the following table definition:  

```sql
CREATE TABLE movies (
  title STRING, 
  rating STRING, 
  length DOUBLE)  
PARTITIONED BY (genre STRING);
```

What will the folder structure in HDFS look like for the movies table?  
<<<<<<< HEAD
/user/root/movies/genre=action/
/user/root/movies/genre=comdey/
/user/root/movies/genre=horror/
.
.
.
/user/root/movies/genre=sciencefiction/

Q2: What is a *SerDe*?
    SerDe is a short name for Serializer and Deserializer
    Using these Hive can read and write to any data source can read and write a
    sequence of records.

Q3: Where should you place the largest table in a `JOIN`?
    Largest tables in a join should come last
=======

Q2: What is a *SerDe*?

Q3: Where should you place the largest table in a `JOIN`?
>>>>>>> 42ec54c5b49980d837de660dbe29f2ec2ab774c4

Q4: You provide a Hive-based storage for your client's sales
transactions. Your clients run queries on their own data. 

Between *partitioning* and *bucketing* which is optimal for strong
this data?
<<<<<<< HEAD

    Partitioning
    Each client will get their own partition and will query within it 
=======
>>>>>>> 42ec54c5b49980d837de660dbe29f2ec2ab774c4
