# collect java options and classpath for demo module
#
JAVA_OPT="-DIGNITE_HOME=$(pwd)/core/ignite -DIGNITE_LOG_DIR=$(pwd)/demo/logs \
  -DIGNITE_CONFIG_HOME=file:$(pwd)/core/config"
TOOL_CLASSPATH="$(pwd)/demo/target/demo-*-jar-with-dependencies.jar"

executeStep() {
  echo "---- step started: $1 ----"
  java $JAVA_OPT -classpath $TOOL_CLASSPATH $@
  echo "---- step finished: $1 ----"
}
