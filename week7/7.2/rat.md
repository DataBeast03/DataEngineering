Miniquiz
--------

1. *True or False*: DStreams are immutable like RDDs.
    True, DStreams are comprised of micro RDDs 

2. If you want to consolidate data from different batches what are
   some ways to do this?
    Windows, States, and joins

3. *True or False*: Window duration must be larger than batch duration.
    True

4. *True or False*: Window duration must be a multiple of batch duration.
    True

5. *True or False*: Slide duration must be a multiple of batch duration.
    True

6. *True or False*: Window duration must be a multiple of slide duration.
    False

7. *True or False*: `ds.pprint()` will execute when you reach this line.
    False

8. *True or False*: DStreams do nothing until `ssc.start()` is called.
    True

9. What happens if the time it takes to process a batch is longer than
   a batch duration?

   (a) The next batch will wait for processing to finish. <-------
  
   (b) The two batches get processed at the same time.
