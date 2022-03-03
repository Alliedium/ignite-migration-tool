#!/bin/bash

# build project
#
cd ..
mvn clean package -Dmaven.test.skip=true

# start Apache Ignite cluster and fix its topology by activating it
#
docker rm -f $(docker ps -aq)
docker-compose up -d --build ignite igniteactivator

# sleep 5 second in order to give time for ignite activator activate ignite cluster
sleep 5

source ./demo/common.sh

# populate the cluster with test data, then export all data and meta data from the
#   cluster into an isolated directory in form of Avro files
#
executeStep io.github.alliedium.ignite.migration.patches.CreateNestedData ./avro_original $@

# stop Apache Ignite cluster to stress that Avro files provide a self-sufficient snapshot of the data
#
docker-compose down

# apply a simple transformation
# (adding a new field to first cache, removing another field from second cache) to Avro files
#
executeStep io.github.alliedium.ignite.migration.patches.AlterNestedData ./avro_original ./avro_transformed $@

# start a new Apache Ignite cluster
#
docker-compose up -d --build ignite igniteactivator

# sleep 5 second in order to give time for ignite activator activate ignite cluster
sleep 5

# upload the transformed Avro files to the cluster
#
executeStep io.github.alliedium.ignite.migration.patches.CheckNestedData ./avro_transformed $@

# shutdown the cluster
#
docker-compose down