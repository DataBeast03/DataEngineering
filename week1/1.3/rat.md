## More SQL Practice

You have the following log data from a retail company. This is the schema:

```
log
    userid
    tmstmp
    itemid
    event

items
    itemid
    name
    category
```

The possible events are:
* `view`: Viewing the details of an item
* `add`: Adding the item to shopping cart
* `buy`: Buying an item

### Write some queries

1. Write a SQL query which gets all the users, item pairs where the user added the item to their shopping cart but did not then buy it. If a user bought an item and then added it to their shopping cart, this should be included in your query.

    If you're having trouble, try starting with something simpler: Can you get all the user, item pairs where the user first added and then bought the item?

2. Write a SQL query which gives the each category and the ratio of purchases to views for items in that category.

    To simplify things, try each part of the ratio separately. First count the total views for the category, then count the total items. Can you combine these results?


### Test your queries!
In practice you may not have a place to test your queries outside of running them in production. If the database is large, this is very expensive! It will take a while for you to confirm if your query ran correctly and this will use company resources. Furthermore, if you get values back, how do you know they are right? It's good practice to create a dummy database to try out your queries.

1. Open postgres on the work stations by opening finder and typing "postgres"

1. In your terminal, create your database: `CREATE DATABASE retail_log;`

2. Open the database: `psql -U postgres retail_log`

3. Create the tables:

    ```sql
    CREATE TABLE log (
        userid   int,
        tmstmp   timestamp,
        itemid   int,
        event    varchar(10)
    );
    ```

4. To add the items, you can use `INSERT INTO`. Another method is to create a csv file with the data. Something liks this:

    ```
    2,2014-09-10 09:10:13,5,view
    3,2014-09-10 09:11:13,5,view
    2,2014-09-10 09:12:13,5,add
    4,2014-09-10 09:13:13,8,view
    3,2014-09-10 09:14:13,5,add
    4,2014-09-10 09:15:13,8,add
    2,2014-09-10 09:16:13,5,buy
    4,2014-09-10 09:17:13,8,buy
    5,2014-09-10 09:18:13,1,buy
    5,2014-09-10 09:19:13,1,add
    ```

    To get this csv file (say it's called `log.csv`) into your SQL table:

    ```sql
    COPY log FROM '~/github/recommendation-systems-soln/log.csv' CSV;
    ```

    Note that you need to use the full path. To get the full path of your current directory from the command line, run `pwd`.

5. Repeat the same steps to create an example `items` table so that you can test the second query. Create dummy data so that your queries returns nonempty tables.
