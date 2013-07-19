#!/bin/sh

cd ${ETL_DIR} 
mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.InitCustomerData" -Dexec.args="--customer alpha --batchCount 1  --batchSize 10" 
cd ${BIGDATA_DIR}/usage-details

