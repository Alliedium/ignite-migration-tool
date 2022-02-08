#!/bin/bash

# build project
#
cd ..
mvn clean package -Dmaven.test.skip=true

# start Apache Ignite cluster and fix its topology by activating it 
#
docker rm -f $(docker ps -aq)
docker-compose up -d --build ignite igniteactivator

# sleep 10 second in order to give time for ignite activator activate ignite cluster
sleep 10

# collect java options and classpath for demo module
#
JAVA_OPT="-DIGNITE_HOME=$(pwd)/core/ignite -DIGNITE_LOG_DIR=$(pwd)/demo/logs \
  -DIGNITE_CONFIG_HOME=file:$(pwd)/core/config"
TOOL_CLASSPATH="$(pwd)/demo/target/demo-0.0.1-jar-with-dependencies.jar"

executeStep() {
  echo "---- step started: $1 ----"
  java $JAVA_OPT -classpath $TOOL_CLASSPATH $1 $2 $3
  echo "---- step finished: $1 ----"
}

# populate the cluster with test data, then export all data and meta data from the 
#   cluster into an isolated directory in form of Avro files
#
executeStep org.alliedium.ignite.migration.demotools.CreateIgniteDataAndWriteIntoAvro ./avro_original

# stop Apache Ignite cluster to stress that Avro files provide a self-sufficient snapshot of the data
#
docker-compose down

# apply a simple transformation
# (adding a new field to first cache, removing another field from second cache) to Avro files
#
executeStep org.alliedium.ignite.migration.patches.AlterCachesDemoPatch ./avro_original ./avro_transformed

# start a new Apache Ignite cluster
#
docker-compose up -d --build ignite igniteactivator

# sleep 10 second in order to give time for ignite activator activate ignite cluster
sleep 10

# upload the transformed Avro files to the cluster
#
executeStep org.alliedium.ignite.migration.demotools.LoadDataFromAvroAndCheckPatchApplied ./avro_transformed

# shutdown the cluster
#
docker-compose down
