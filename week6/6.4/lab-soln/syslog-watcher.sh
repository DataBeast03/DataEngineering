#!/bin/sh

LINUX_SYSLOG=/var/log/syslog
MAC_SYSLOG=/var/log/system.log
SYSLOG=$MAC_SYSLOG;

tail -F $SYSLOG | nc -lk 9999
