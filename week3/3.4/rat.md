Miniquiz
--------

1. What must be true of elements of an RDD before you can use the Pair
   RDD operations on it?

2. What will the output of this be?
   `sc.parallelize([("CA",100),("CA",200)]).reduceByKey(lambda v1,v2:min(v1,v2)).collect()`

3. What two properties must be true of an operation that you can use
   in `reduceByKey`?

4. Between `groupByKey` and `reduceByKey` which one has a smaller
   memory footprint?

5. In the following code which `join` should we use to make
   `sales.join(states).collect()` list states with no sales?

   `sales = sc.parallelize([("CA",100),("CA",200)])`    
   `states = sc.parallelize([("CA","California"),("NV","Nevada")])`    
   `sales.join(states).collect()`    

6. Between `map()`, `reduceByKey()`, `filter()`, `keyBy()` which one
   is the wide transformation?

7. When should an RDD be cached or persisted?
