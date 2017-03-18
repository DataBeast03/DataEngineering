##Part 1: Introduction to SparkSQL

SparkSQL allows you to execute relational queries on **structured** data using Spark. First you would turn a regular `RDD` into a `SchemaRDD` which then can be queried as a SQL table. Here we will be running some queries 
on a Yelp dataset.

<br>

1. Instantiate a `HiveContext()` and load the Yelp business data in using `jsonFile()`. 

   ```python
   yelp_business_schema_rdd = hive_contxt.jsonFile('s3n://[YOUR ACCESS_KEY]:[YOUR SECRET_KEY]@sparkdatasets/yelp_academic_dataset_business.json')
   ```

2. Print the schema and register the `yelp_business_schema_rdd` as a table named `yelp_business`.

3. Write a query that returns the `name` of entries that fulfill the following conditions:
   - Rated at 5 `stars`
   - In the city of Phoenix
   - Accepts credit card (Reference the `Accept Credit Card` field by ````attributes.`Accepts Credit Cards`````
   - And is under the `Restaurants` category

<br>

##Part 2: Spark and SparkSQL in Practice 

Now we have a basic knowledge of how SparkSQL works, let's try dealing with a real-life scenario where some data manipulation is required in a regular Spark RDD before querying the data with SparkSQL.

<br>

1. Load the `user` and `transaction` datasets into 2 separate RDDs with the following code. 

   ```python
   user_rdd = sc.textFile('s3n://[YOUR ACCESS_KEY]:[YOUR SECRET_KEY]@sparkdatasets/users.txt')
   transaction_rdd = sc.textFile('s3n://[YOUR ACCESS_KEY]:[YOUR SECRET_KEY]@sparkdatasets/transactions.txt')
   ```

2. Each row in the `user` RDD represent the user with his/her `user_id, name, email, phone`. Each row in the 
   `transaction` RDDs has the columns  `user_id, amount_paid, date`. Map functions to the RDDs to make each row in 
   the RDDs a json **string** such as `{user_id: XXX, name: XXX, email:XXX, phone:XXX}` (use `json.dumps()`).

   **P.S.: Strip the `$` sign in the `amount_paid` column in the `transaction` RDD so it would be recognize as a   
   float when read into a SchemaRDD.**

3. Convert the `user` and `transaction` RDDs to SchemaRDDs. Print the schemas to make sure the conversion is 
   successful. Register the SchemaRDDs as separate tables and print the first couple of rows with SQL queries.

4. Write a SQL query to return the names and the amount paid for the users with the **top 10** transaction amount.
