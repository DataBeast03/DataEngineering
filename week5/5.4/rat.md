Quiz 
====

Setup
-----

Step 1: Create a JSON file called `sales.json` containing this data:

```
{"id":101, "date":"11/13/2014", "store":100, "state":"WA", "product":331, "amount":300.00}
{"id":104, "date":"11/18/2014", "store":700, "state":"OR", "product":329, "amount":450.00}
{"id":102, "date":"11/15/2014", "store":203, "state":"CA", "product":321, "amount":200.00}
{"id":106, "date":"11/19/2014", "store":202, "state":"CA", "product":331, "amount":330.00}
{"id":103, "date":"11/17/2014", "store":101, "state":"WA", "product":373, "amount":750.00}
{"id":105, "date":"11/19/2014", "store":202, "state":"CA", "product":321, "amount":200.00}
```

Step 2: Consider the following code:

    import pyspark
    sc = pyspark.SparkContext()
    sqlContext = pyspark.HiveContext(sc)
    sales = sqlContext.read.json('sales.json')
    sales.registerTempTable('sales')
    # Find largest sale.
    sqlContext.sql('SELECT * from sales ORDER BY amount DESC LIMIT 1').show()
    # Find largest sale in CA.
    sqlContext.sql('SELECT * from sales WHERE state='CA' ORDER BY amount DESC LIMIT 1').show()

Questions
---------

Q1: How could you write the `sales` data as Parquet?

Q2: How many times is the data read from disk?

Q3: How can you prevent the data being read from disk multiple times?

Q4: What is the downside of using `ORDER BY` in Hive?

Q5: Does Spark SQL have the same issue?
