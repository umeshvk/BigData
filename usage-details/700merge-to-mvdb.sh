#!/bin/sh
cd ${ETL_DIR} 
hadoop jar /home/umesh/work/BigData/mvdb/target/mvdb-0.0.1.jar  /data/alpha 
cd ${BIGDATA_DIR}/usage-details
