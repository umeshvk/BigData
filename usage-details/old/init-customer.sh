#!/bin/sh

#-------------------------------------------------------------------------------
# Copyright 2014 Umesh Kanitkar
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#-------------------------------------------------------------------------------

export BIGDATA_DIR=~/work/BigData
export ETL_DIR=${BIGDATA_DIR}/etl/etl
export MVDB_DIR=${BIGDATA_DIR}/mvdb
#logback file: ~/work/BigData/configuration/logback.xml
cd ${ETL_DIR}; mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.InitCustomerData" -Dexec.args="--customer alpha --batchCount 1  --batchSize 10" ; cd ${BIGDATA_DIR}/usage-details
cd ${ETL_DIR}; mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.ModifyCustomerData" -Dexec.args="" ; cd ${BIGDATA_DIR}/usage-details
cd ${ETL_DIR}; mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.ExtractDBChanges" -Dexec.args="--customer alpha" ; cd ${BIGDATA_DIR}/usage-details
cd ${ETL_DIR}; mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.ScanDBChanges" -Dexec.args="--customer alpha --snapshotDir `cat /tmp/etl.extractdbchanges.directory.txt`" ; cd ${BIGDATA_DIR}/usage-details
