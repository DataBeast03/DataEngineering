Real Time Intrusion Detection
=============================

Nemesis Networks has hired you as a consultant to help build out their
real time intrusion detection system. 

They want to use Kafka to capture all login and logout events in a
system and then look for patterns.

For example, if there is a marked uptick in login failures the system
should raise an alert. This way the system can lock out people who are
trying to hack into the system by guessing passwords.

Step 1
------

Using the steps from the lecture:

- Download Kafka

- Install Kafka

- Start Zookeeper and Kafka

Step 2
------

Use `pip` to install `kafka-python` and `avro`.

Step 3
------

In Kafka create a topic called `login-topic`.

Step 4
------

Create an Avro schema for objects that look like this.

```javascript
{"date":"2015-10-01","time":"08:43:14","user":"alice","op":"login","success":"false"}
```

Field      |Value
-----      |-----
`date`     |Date in `yyyy-mm-dd` format
`time`     |Time in `hh:mm:ss` format
`user`     |Some user ID like `alice` or `bob`
`op`       |`login` `logout`
`success`  |`true` `false`


Step 5
------

Create a producer that uses the `get_login_event()` function defined
below to feed events into the Kafka topic `login-topic`. The producer
should serialize the events using Avro. 

```python
import random
import time

LOGIN_USERS = ['alice','bob','chas','dee','eve']
LOGIN_OPS = ['login','logout']

def get_login_event():
    return {
        'date'    : time.strftime('%F'),
        'time'    : time.strftime('%T'),
        'user'    : random.choice(LOGIN_USERS),
        'op'      : random.choice(LOGIN_OPS),
        'success' : bool(random.randint(0,1)) }
```

Step 6
------

Create a consumer that consumes the login events published to
`login-topic`.

The consumer keeps track of average login success rate. It prints out
the rate.

The average login success rate is `success/total_count`.

Step 7
------

Suppose you had the data from all the servers coming into your Kafka
topic. How would you scale it?

Suppose you scale this with hundreds of consumers. What might some of
the issues be? How can you resolve those issues?
