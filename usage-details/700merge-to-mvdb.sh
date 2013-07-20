#!/bin/sh
cd ${ETL_DIR} 
hadoop jar /home/umesh/work/BigData/mvdb/target/mvdb-0.0.1.jar  /tmp/f1 /tmp/d1
cd ${BIGDATA_DIR}/usage-details
