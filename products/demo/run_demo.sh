#!/bin/bash

# build project
#
cd ..
mvn clean package -Dmaven.test.skip=true

# start Apache Ignite cluster and fix its topology by activating it 
#
docker rm -f $(docker ps -aq)
docker-compose up -d --build ignite igniteactivator

# collect java options and classpath for demo module
#
JAVA_OPT="-DIGNITE_HOME=$(pwd)/core/ignite -DIGNITE_LOG_DIR=$(pwd)/demo/logs \
  -DIGNITE_CONFIG_HOME=file:$(pwd)/core/config"
TOOL_CLASSPATH="$(pwd)/demo/target/demo-1.0.0-jar-with-dependencies.jar"

executeStep() {
  echo "---- step started: $1 ----"
  java $JAVA_OPT -classpath $TOOL_CLASSPATH $1 $2 $3
  echo "---- step finished: $1 ----"
}

# populate the cluster with test data, then export all data and meta data from the 
#   cluster into an isolated directory in form of Avro files
#
executeStep org.alliedium.ignite.migration.patchtools.CreateIgniteDataAndWriteIntoAvro ./avro_original

# stop Apache Ignite cluster to stress that Avro files provide a self-sufficient snapshot of the data
#
docker-compose down

# apply a simple transformation (adding a new field to every cache) to Avro files
#
executeStep org.alliedium.ignite.migration.patches.AlterCacheAddFieldPatch ./avro_original ./avro_transformed

# start a new Apache Ignite cluster
#
docker-compose up -d --build ignite igniteactivator

# upload the transformed Avro files to the cluster
#
executeStep org.alliedium.ignite.migration.patchtools.LoadDataFromAvroAndCheckFieldAdded ./avro_transformed

# shutdown the cluster
#
docker-compose down
