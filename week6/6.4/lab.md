System Monitoring
-----------------

You have been hired as a data engineer at BigDog, Inc. Their main
product monitors system logs to look for various conditions.

We are going to create a syslog monitor that will keep track of the
number of processes opened and closed across the monitored machines.

Syslog Processing
-----------------

- Create a process that will publish the system log to port 9999 and
  save it to `sys-watcher.sh`.

```sh
#!/bin/sh

LINUX_SYSLOG=/var/log/syslog
MAC_SYSLOG=/var/log/system.log
SYSLOG=$MAC_SYSLOG;

tail -F $SYSLOG | nc -lk 9999
```

- Modify this code to point to the right location for your syslog.

- Use Spark Streaming to listen to the lines of text on that port.

- In each batch count how many processes have started and stopped in
  that batch.

- Here is what process start and stop syslog lines look like on a Mac.

```text
        Aug 14 08:49:41 host1234 login[70321]: USER_PROCESS: 70321 ttys005
        Aug 14 08:49:42 host1234 login[70325]: USER_PROCESS: 70325 ttys006
        Aug 14 08:49:43 host1234 login[70329]: USER_PROCESS: 70329 ttys007
        Aug 14 08:49:44 host1234 login[70325]: DEAD_PROCESS: 70325 ttys006
        Aug 14 08:49:44 host1234 login[70329]: DEAD_PROCESS: 70329 ttys007
        Aug 14 08:49:45 host1234 login[70321]: DEAD_PROCESS: 70321 ttys005
```
 
- Verify what they look like on your system and then write a Spark
  Streaming application to capture and process these lines.

- Use a batch duration of 10 seconds. Write out the number of
  processes that are starting and stopping in each batch.

- Write the output to `proc-stats` directory.

- For testing you may use `pprint` to print it to stdout.

- The output should look like this:

```text
Started: 33
Stopped: 11
```

Challenge: Windows
------------------

- Create a window with window duration of 1 minute, and a slide
  duration of 20 seconds, and use that to determine how many processes
  are starting and stopping within a window.

Challenge: Preserving State
---------------------------

- Calculate the number of processes running by keeping track of the
  number of processes starting and stopping (do not make any system
  calls).

- Using the time series above calculate the mean and standard
  deviation of the number of running processes.

- Raise an alert if the number of running processes exceeds twice the
  standard deviation.

- Raise the alert by printing out a warning message.
