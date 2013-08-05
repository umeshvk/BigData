#!/bin/sh

hadoop fs -cp /data/alpha/20030115050607/data-orders.dat /tmp/theData.dat
hadoop fs -ls /tmp/theData.dat
hive -f test.hive 
