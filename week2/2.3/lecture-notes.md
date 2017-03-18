A new paradigm for Big Data
===========================

Learning Objectives
-------------------------------------------------
### Standard: Evaluate when to apply Lambda Architecture

By the end of this lesson, students should be able to:

- Explain components of Lambda architecture
- Name (8) desired properties of big data system
- Discuss advantages & disadvantages of Lambda Architecture vs traditional databases

Scaling with a traditional database
-------------------------------------------------

Scenario: you are the data engineer at a brand new web startup. You have designed your OLAP RDBMS according to all the best practices. You've anticipated questions like: "How many daily active users do we have?" and "How many pages do users visit?" which can be answered by tables like:

| Column name 	| Type
|:------------:	|:------------:
| id          	| integer
| user_id      	| integer
| url          	| varchar(255)
| pageviews   	| bigint

The site is a hit and before long you start seeing:

<font size=6 color=red><b><center>Timeout error on inserting to the database.</center></b></font>

You remember how random writes can be time consuming, so you insert a queue to cache updates and apply them in batches:

### Scaling with a queue

![Batching updates with queue and worker](https://s3-us-west-2.amazonaws.com/dsci6007/assets/01fig02.jpg)

This helps for a while, but only temporarily. Eventually even with batch updates, the database can only handle so many writes. Then you recall an article on Instagram's engineering blog about sharding. It's a beast to set up, but eventually you've got it:

### Scaling by sharding the database
![](http://i.imgur.com/1LoFj7B.png)

### Fault-tolerance issues begin

Your website continues to grow exponentially and before you know it, your database is distributed over hundreds of nodes which are now failing at a rate of one every few weeks. (Think about this, if the average machine will fail once in 10 years, how long before one of 200 machines fails?) Fortunately Postgres 9 has built-in replication, but this too is non-trivial to set up.

### Corruption issues

It's now been a year since your website has gone live and you haven't had a good night's sleep in all that time, so it's no surprise when you accidentally deploy a bug to production that increments the number of pageviews by two, instead of by one, for every URL. You don’t notice until 24 hours later, but by then the damage is done. Your weekly backups don’t help because there’s no way of knowing which data got corrupted. After all this work trying to make your system scalable and tolerant of machine failures, your system has no resilience to a human making a mistake. And if there’s one guarantee in software, it’s that bugs inevitably make it to production, no matter how hard you try to prevent it.

### What went wrong?

### How will Big Data techniques help?

(NoSQL is not a panacea)

First principles
-------------------------------------------------

$$􏰁\text{query} = function(􏰁\text{all data}􏰂)$$

Desired properties of a Big Data system
-------------------------------------------------

### Robustness and fault tolerance

Building systems that "do the right thing" is difficult in the face of the challenges of distributed systems. Systems need to behave correctly despite machines going down randomly, the complex semantics of consistency in distributed databases, duplicated data, concurrency, and more. These challenges make it difficult even to reason about what a system is doing. Part of making a Big Data system robust is avoiding these complexities so that you can easily reason about the system.

### Low latency reads and updates

The vast majority of applications require reads to be satisfied with very low latency, typically between a few milliseconds to a few hundred milliseconds. On the other hand, the update latency requirements vary a great deal between applications. Some applications require updates to propagate immediately, but in other applications a latency of a few hours is fine. Regardless, you need to be able to achieve low latency updates *when you need them* in your Big Data systems. More importantly, you need to be able to achieve low latency reads and updates without compromising the robustness of the system.

### Scalability

Scalability is the ability to maintain performance in the face of increasing data or load by adding resources to the system. The Lambda Architecture is horizontally scalable across all layers of the system stack: scaling is accomplished by adding more machines.

### Generalization

A general system can support a wide range of applications. Indeed, this book wouldn’t be very useful if it didn’t generalize to a wide range of applications! Because the Lambda Architecture is based on functions of all data, it generalizes to all applications, whether financial management systems, social media analytics, scientific applications, social networking, or anything else.

### Extensibility

You don’t want to have to reinvent the wheel each time you add a related feature or make a change to how your system works. Extensible systems allow functionality to be added with a minimal development cost.

### Ad hoc queries

Being able to do ad hoc queries on your data is extremely important. Nearly every large dataset has unanticipated value within it. Being able to mine a dataset arbitrarily gives opportunities for business optimization and new applications. Ultimately, you can’t discover interesting things to do with your data unless you can ask arbitrary questions of it.

### Minimal maintenance

Maintenance is a tax on developers. Maintenance is the work required to keep a system running smoothly. This includes anticipating when to add machines to scale, keeping processes up and running, and debugging anything that goes wrong in production.

An important part of minimizing maintenance is choosing components that have as little *implementation complexity* as possible. You want to rely on components that have simple mechanisms underlying them. In particular, distributed databases tend to have very complicated internals. The more complex a system, the more likely something will go wrong, and the more you need to understand about the system to debug and tune it.

### Debuggability

A Big Data system must provide the information necessary to debug the system when things go wrong. The key is to be able to trace, for each value in the system, exactly what caused it to have that value.

"Debuggability" is accomplished in the Lambda Architecture through the functional nature of the batch layer and by preferring to use recomputation algorithms when possible.

The problems with fully incremental architectures
-------------------------------------------------
![Fully incremental architecture](https://s3-us-west-2.amazonaws.com/dsci6007/assets/01fig03.jpg)

### Operational complexity

### Extreme complexity of achieving eventual consistency

![Using replication to increase availability](https://s3-us-west-2.amazonaws.com/dsci6007/assets/01fig04_alt.jpg)

### Lack of human-fault tolerance

![Adding logging to fully incremental architectures](https://s3-us-west-2.amazonaws.com/dsci6007/assets/01fig05.jpg)

### Fully incremental solution vs. Lambda Architecture solution

Lambda Architecture
-------------------------------------------------
![Lambda Architecture](https://s3-us-west-2.amazonaws.com/dsci6007/assets/01fig06.jpg)
$$
\begin{align*}
􏰁\text{batch view} &= function(􏰁\text{all data}􏰂)\\
\text{query} &= function(􏰁\text{batch view}􏰂)
\end{align*}
$$

![Architecture of the batch layer](https://s3-us-west-2.amazonaws.com/dsci6007/assets/01fig07.jpg)

### Batch layer
![Batch layer](https://s3-us-west-2.amazonaws.com/dsci6007/assets/01fig08.jpg)
```java
function runBatchLayer():
  while(true):
    recomputeBatchViews()
```

### Serving layer
![Serving layer](https://s3-us-west-2.amazonaws.com/dsci6007/assets/01fig09.jpg)

### Batch and serving layers satisfy almost all properties
* Robustness and fault tolerance
* Scalability
* Generalization
* Extensibility
* Ad hoc queries
* Minimal maintenance
* Debuggability  

### Speed layer
![Speed layer](https://s3-us-west-2.amazonaws.com/dsci6007/assets/01fig10.jpg)

$$
\text{realtime view} = function(􏰁\text{realtime view}, \text{new data}􏰂)
$$  

$$
\begin{align*}
􏰁􏰁\text{batch view} &= function(􏰁\text{all data}􏰂)\\
\text{realtime view} &= function(􏰁\text{realtime view}, \text{new data}􏰂)\\
\text{query} &= function(􏰁\text{batch view}􏰂, \text{realtime view})
\end{align*}
$$

Question: Is a speed layer always necessary? Under what circumstances would you want it?

![Lambda Architecture diagram](https://s3-us-west-2.amazonaws.com/dsci6007/assets/01fig11_alt.jpg)

Question: Give at least two examples of when you would want to use Lambda Architecture, 
and two examples of when you wouldn't.

Recent trends in technology
-------------------------------------------------
### CPUs aren’t getting faster
### Elastic clouds
### Vibrant open source ecosystem for Big Data
* Batch computation systems 
* Serialization frameworks
* Random-access NoSQL databases
* Messaging/queuing systems
* Realtime computation system 

