##Hive (I) - Quiz

- How are number of mappers decided in Hive? Is there any default or minimum number for Hive querries?  
    A large file is split up into 128MB or less blocks, each block gets its own mapper 
    

- Can Hive be used to process image or video data? 
      No, hive works for structured and semi-structured data ONLY

- A Hive table consists of a schema stored in the (-------) and data stored in (-------).
    Metastore
    HDFS

- True or False: The Hive metastore requires an underlying SQL database.
    True, the data is stored in Derby a.k.a RDMS by default 
    

- What happens to the underlying data of a Hive-managed table when the table is dropped? 
    Internal Table (Hive-managed table): table and data is deleted
    External Table: only table is deleted
    
- True or False: A Hive external table must define a LOCATION.
    False

- List three different ways data can be loaded into a Hive table
    pull data from HDFS
    using load data command
    using the insert query in HiveQL
    

- Explain the output of the following query:  
    `select * from movies order by title;`

    all data from all columns in table called movies will be displayed 
    and is globally ordered by the column called title
