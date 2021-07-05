#!/bin/bash

cd ..

# parse and save all arguments into associative array
declare -A params=()

for ARGUMENT in "$@"
do
    KEY=$(echo $ARGUMENT | cut -f1 -d=)
    VALUE=$(echo $ARGUMENT | cut -f2 -d=)
    params[$KEY]=$VALUE
done

# collect java opt for test
JAVA_OPT="-DIGNITE_HOME=$(pwd)/core/ignite -DIGNITE_LOG_DIR=$(pwd)/core/logs \
  -DIGNITE_CONFIG_HOME=file:$(pwd)/core/config -DIGNITE_QUIET=false -Djava.net.preferIPv4Stack=true"

[[ ! -z "${params['resultsFileName']}" ]] && JAVA_OPT="$JAVA_OPT -DresultsFileName=${params['resultsFileName']}"
[[ ! -z "${params['recordsStep']}" ]] && JAVA_OPT="$JAVA_OPT -DrecordsStep=${params['recordsStep']}"
[[ ! -z "${params['maxRecords']}" ]] && JAVA_OPT="$JAVA_OPT -DmaxRecords=${params['maxRecords']}"

# collect java opt for ignite server node
IGNITE_DOCKER_JVM_OPTS=""

[[ ! -z "${params['igniteWalMode']}" ]] && IGNITE_DOCKER_JVM_OPTS="$IGNITE_DOCKER_JVM_OPTS -DigniteWalMode=${params['igniteWalMode']}"
[[ ! -z "${params['ignitePersistence']}" ]] && IGNITE_DOCKER_JVM_OPTS="$IGNITE_DOCKER_JVM_OPTS -DignitePersistence=${params['ignitePersistence']}"
[[ ! -z "${params['igniteXmx']}" ]] && IGNITE_DOCKER_JVM_OPTS="$IGNITE_DOCKER_JVM_OPTS -Xms0m -Xmx${params['igniteXmx']}"


# print docker jvm opt
echo "docker jvm opts: $IGNITE_DOCKER_JVM_OPTS"

# remove all containers
docker rm -f $(docker ps -aq)

# start Apache Ignite cluster and fix its topology by activating it
docker-compose build --build-arg IGNITE_DOCKER_JVM_OPTS="$IGNITE_DOCKER_JVM_OPTS"
docker-compose up -d ignite igniteactivator

# print java opt
echo "java opt: $JAVA_OPT"
# run test
mvn -B clean test $JAVA_OPT -Dtest=ProcessingTimeTest -DfailIfNoTests=false -pl core -am test

# remove all containers
docker rm -f $(docker ps -aq)

# copy result csv file to reports folder
if [[ ! -z "${params['resultsFileName']}" ]]
then
  mkdir -p reports
  cp ./core/target/${params['resultsFileName']}.csv ./reports/
fi
