Why Big Data?
=======================================

Big Data needs data science, not necessarily the other way around.

Companies have lots of data, and it might not necessarily fit in to memory.


# Why Map-Reduce ?

MapReduce is a framework originally developed at Google that allows large scale distributed computing. Apache Hadoop is the open source implementation that is gaining significant traction in industry due to its ease of use and wide availability.  It scales well to many thousands of nodes and petabytes of data. 

![Map-Reduce Architecture](https://developers.google.com/appengine/docs/python/images/mapreduce_mapshuffle.png)

What problem does Map Reduce solve?
=========================================== 

It's a computing paradigm similar to divide and conquer.

If you break up a problem in to smaller sub problems and solve them in parallel, then reduce down to the results,

you can do faster computation leveraging the power of today's hardware.



Mapper
=========================

This is also called a mapper.

Mappers run several smaller tasks concurrently. This is to ensure work can get done faster vs the more sequential workflows we are used to.


Reducers
=========================

Reducers take the output of the mappers and reduce them down to a final result. 





Abstracting away what parallel means, let's think about what happens when we break up a task.

If we were to all clean a part of this building we could get work done faster. Cleaning can be broken up into smaller sub tasks such as mopping sweeping dishes.

Think about if one person were doing that vs all of us.

The takeaway here is that the way you write map reduce is relative to the problem definition.


This again?
==========================

Let's think about word counting. 

If we want to count all of the words in a dataset what would that look like?

First think about how to break up the problem.

A mapper would take a document and count each individual word.

A reducer would take the counts from each of those words and sum the counts to an overall result.


Parallelism
===================================

Parallel can be across multiple cores or multiple servers.

If we think about your laptop.

It can multitask, we see that it can run your web browser and your ipython notebook simultaneously.

Let's think about multiserver. How does a website handle traffic?

A good way to think about this is via load balancing. Load balancing is a way of delegating requests to multiple servers that serve the same purpose in this case rendering the same website. 

This is distributed computing.

Both are parallel.


Note that map reduce can be run on multi processor just fine. Most datasets don't need a cluster.



Input splits / Batching
========================================

Let's think about wrt to data now. How do we split up data, also called batching?

Batching is splitting up a dataset a computation can run in parallel.

This brings up an interesting problem. How do we split up a dataset? This is a problem we call finding the optimal input split. 

The optimal input split itself has a few parameters. 

These parameters are relative to the amount of data (number of examples) and the number of servers or processors available.

For example your system typically has 8 cores. If you have 40 examples your input split is 5 . This ensures each core will receive work.


Functional Programming:

One thing of note is the idea that map and reduce are both very strong functional programming paradigms.

Map takes an input and returns some output. Maps are the composed and piped in to a reduce step

for further processing.

A higher order operation of map is taking a function and applying a transform on the give input

An easy example of this would be a list:

input = [1,2,3,4,5]


def map_add_1(input):
     return input + 1

ret = input.map(map_add_1)

[2,3,4,5,6]


Reduce 

def reduce_sum(input):
    sum = 0
   for num in input:
       sum += num
    return sum

reduce_sum(ret) = 20



DataSet Splitting
======================================

Say we have a data set (pair of input/outputs) of 5 million examples and 10000 features

This, especially as dense, will not fit in to RAM on most machines.

Say we have a cheap compute cluster: Elastic Reduce.

Say we have a storage mechanism that  knows how to split up that data set (a distributed file system)

Now we are able to run computations on the huge data set splitting up the batches across the machines.

This is how distributed machine learning works.

## Design Patterns

Often the most difficult part to working with Hadoop (and MapReduce) are the constraints it imposes.  The idea of no shared state is very natural to anyone coming from functional programming but can often feel limiting (especially if you come from the multi-paradigm land of modern languages).  Let us look at a few common examples of tasks you might need to perform in the framework and how we might overcome them.
