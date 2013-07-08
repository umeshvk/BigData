#!/bin/sh

#logback file: ~/work/BigData/configuration/logback.xml
mvn -Dlogback.configurationFile=~/work/BigData/configuration/logback.xml   exec:java -Dexec.mainClass="com.mvdb.etl.actions.InitDB" 
mvn exec:java -Dexec.mainClass="com.mvdb.etl.actions.InitCustomerData" -Dexec.args="--customer alpha --batchCount 1  --batchSize 10" 
#mvn exec:java -Dexec.mainClass="com.mvdb.etl.actions.ModifyCustomerData" -Dexec.args="--batchCount 1  --batchSize 1000" 
mvn exec:java -Dexec.mainClass="com.mvdb.etl.actions.ExtractDBChanges" -Dexec.args="--customer alpha" 
mvn exec:java -Dexec.mainClass="com.mvdb.etl.actions.ScanDBChanges" -Dexec.args="--customer alpha --snapshotDir 20130706104735" 
