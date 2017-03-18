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

```bash
# On the Sandbox.
mkdir -p ~/tmp/movielens
cd       ~/tmp/movielens
wget http://files.grouplens.org/datasets/movielens/ml-latest-small.zip
unzip ml-latest-small.zip
cd ml-latest-small
```

Look at the first few lines of each file.

```bash
head -n 2 links.csv
head -n 2 movies.csv
head -n 2 ratings.csv
head -n 2 tags.csv
```

Create the following dirs in HDFS: 

- `/user/root/links` 
- `/user/root/movies` 
- `/user/root/ratings` 
- `/user/root/tags`

```bash
hadoop fs -mkdir -p /user/root/links
hadoop fs -mkdir -p /user/root/movies
hadoop fs -mkdir -p /user/root/ratings
hadoop fs -mkdir -p /user/root/tags
```

Upload these CSV files into HDFS.

```bash
hadoop fs -put links.csv   /user/root/links/links.csv
hadoop fs -put movies.csv  /user/root/movies/movies.csv
hadoop fs -put ratings.csv /user/root/ratings/ratings.csv
hadoop fs -put tags.csv    /user/root/tags/tags.csv
```

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


### Add links to Hive

```sql
-- Drop table if it exists.
DROP TABLE IF EXISTS links;

-- Create table.
CREATE EXTERNAL TABLE links
  (movieId INT,
  imdbId STRING,
  tmdbId STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/root/links'
TBLPROPERTIES("skip.header.line.count"="1");

-- Verify that table has right structure.
DESCRIBE links;

-- Verify that table has data.
SELECT * FROM links LIMIT 3;
```

### Add movies to Hive

```sql
-- Drop table if it exists.
DROP TABLE IF EXISTS movies;

-- Create table.
CREATE EXTERNAL TABLE movies
  (movieId INT,
  title STRING,
  genres STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/root/movies'
TBLPROPERTIES("skip.header.line.count"="1");

-- Verify that table has right structure.
DESCRIBE movies;

-- Verify that table has data.
SELECT * FROM movies LIMIT 3;
```

### Add ratings to Hive

```sql
-- Drop table if it exists.
DROP TABLE IF EXISTS ratings;

-- Create table.
CREATE EXTERNAL TABLE ratings
  (userId INT,
  movieId INT,
  rating FLOAT,
  time STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/root/ratings'
TBLPROPERTIES("skip.header.line.count"="1");

-- Verify that table has right structure.
DESCRIBE ratings;

-- Verify that table has data.
SELECT * FROM ratings LIMIT 3;
```

### Add tags to Hive

```sql
-- Drop table if it exists.
DROP TABLE IF EXISTS tags;

-- Create table.
CREATE EXTERNAL TABLE tags
  (userId INT,
  movieId INT,
  tag STRING,
  time STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/root/tags'
TBLPROPERTIES("skip.header.line.count"="1");

-- Verify that table has right structure.
DESCRIBE tags;

-- Verify that table has data.
SELECT * FROM tags LIMIT 3;
```

### Notes

We had to rename `timestamp` to `time` because `timestamp` is a
reserved word in HiveQL.

## Step 4: Hive Queries

Write Hive Queries to perform the following actions.

- Count the number of movies in the `movies` table.

```sql
SELECT COUNT(title) FROM movies;
```

- Count the number of distinct tags grouped by tags.

```
SELECT tag,COUNT(*) FROM tags GROUP BY tag;
```

## Step 5: Extra Challenge

- For each movie find how many ratings it has.

```sql
SELECT movies.title, COUNT(ratings.rating)
FROM movies
LEFT JOIN ratings
ON (movies.movieId = ratings.movieId)
GROUP BY movies.movieId, movies.title;
```

- For each movie find out the average rating.

```sql
SELECT movies.title, AVG(ratings.rating)
FROM movies
LEFT JOIN ratings
ON (movies.movieId = ratings.movieId)
GROUP BY movies.movieId, movies.title;
```

- Find top 10 movies with the highest average ratings that have at least 5 ratings.

```sql
-- Create table.
DROP TABLE IF EXISTS movie_ratings;
CREATE TABLE movie_ratings (
  movieId INT,
  title STRING,
  rating_count INT,
  rating_avg DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Save intermediate data in it.
INSERT INTO movie_ratings
SELECT movies.movieId, movies.title, 
  COUNT(ratings.rating) AS rating_count,
  AVG(ratings.rating)   AS rating_avg
FROM movies
LEFT JOIN ratings
ON (movies.movieId = ratings.movieId)
GROUP BY movies.movieId, movies.title;

-- Now calculate the result.
SELECT *
FROM movie_ratings
WHERE rating_count > 5
DISTRIBUTE BY rating_avg
SORT BY rating_avg DESC
LIMIT 10;
```

## Step 6: Drop Tables

- When you are done drop the tables.

```sql
DROP TABLE links;
DROP TABLE movies;
DROP TABLE ratings;
DROP TABLE tags;
```

- Do you need to do anything else to get rid of the data in HDFS?

Yes. We have to delete the data in HDFS since we used external tables.

```bash
hadoop fs -rm -r /user/root/links
hadoop fs -rm -r /user/root/movies
hadoop fs -rm -r /user/root/ratings
hadoop fs -rm -r /user/root/tags
```
