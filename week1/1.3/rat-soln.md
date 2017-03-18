#### Write some queries

1. Write a SQL query which gets all the users, item pairs where the user added the item to their shopping cart but did not then buy it. If a user bought an item and then added it to their shopping cart, this should be included in your query.

    ```sql
    SELECT
        a.userid,
        a.itemid
    FROM log a
    LEFT OUTER JOIN log b
    ON
        a.userid=b.userid AND
        a.itemid=b.itemid AND
        a.event='add' AND
        b.event='buy' AND
        a.tmstmp<b.tmstmp
    WHERE
        a.event='add' AND
        b.event IS NULL;
    ```


2. Write a SQL query which gives the each category and the ratio of views to purchases for items in that category.

    ```sql
    WITH joined AS (
        SELECT
            userid,
            log.itemid,
            log.event,
            items.category
        FROM log
        JOIN items ON log.itemid=items.itemid
        )
    SELECT
        category,
        1.0 * SUM(CASE WHEN event='buy' THEN 1 ELSE 0 END) /
            SUM(CASE WHEN event='view' THEN 1 ELSE 0 END)
    FROM joined GROUP BY category;
    ```
