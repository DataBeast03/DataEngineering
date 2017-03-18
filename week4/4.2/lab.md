## Step 1: Start Hive

Follow the steps in the lecture to start your Hortonworks Sandbox VM.

Start 2 `ssh` session2 into it.

Start the Hive shell in one session.

Leave the other session running the Bash shell.

## Step 2: Upload Movielens Data to HDFS

Download the MovieLens data files from
<http://files.grouplens.org/datasets/movielens/ml-latest-small.zip>
and unzip it.

This should produce the following files.

- `links.csv`
- `movies.csv`
- `ratings.csv`
- `tags.csv`

Look at the first few lines of each file.

Create the following dirs in HDFS: 

- `/user/root/links` 
- `/user/root/movies` 
- `/user/root/ratings` 
- `/user/root/tags`

Upload each CSV files into its directory in HDFS.

## Step 3: Create Tables

Here are the header lines from the files.

File           |First Line
----           |----------
`links.csv `   |`movieId,imdbId,tmdbId`
`movies.csv`   |`movieId,title,genres`
`ratings.csv`  |`userId,movieId,rating,timestamp`
`tags.csv `    |`userId,movieId,tag,timestamp`

Execute `CREATE EXTERNAL TABLE` commands to create external tables for
each of these files. Ingest the data into the tables.

Use `DESCRIBE X` to verify that the tables were created correctly. Use
`SELECT * FROM X LIMIT 5` to verify that the data was ingested
correctly from HDFS. Replace `X` with the table name in the commands.

## Step 4: Hive Queries

Write Hive Queries to perform the following actions.

- Count the number of movies in the `movies` table.

- Count the number of distinct tags grouped by tags.

## Step 5: Extra Challenge

- For each movie find how many ratings it has.

- For each movie find out the average rating.

- Find top 10 movies with the highest average ratings that have at least 5 ratings.

## Step 6: Drop Tables

- When you are done drop the tables.

- Do you need to do anything else to get rid of the data in HDFS?

