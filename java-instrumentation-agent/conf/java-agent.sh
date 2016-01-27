#!/usr/bin/env bash

echo 'Loading instrumentation javaagent '

# add required jars to the classpath based on the folder location
#	wso2 product : $CARBON_HOME/lib/javaagent/*.jar
#	other product : path/to/javaagent/lib/*.jar
# Eg: export CARBON_CLASSPATH="$CARBON_CLASSPATH":"$(echo $CARBON_HOME/lib/javaagent/*.jar | tr ' ' ':')"

export CARBON_CLASSPATH="$CARBON_CLASSPATH":"$(echo $CARBON_HOME/lib/javaagent/*.jar | tr ' ' ':')"

# pass product type and path of configuration files to the agent as arguments
# (arguments in [carbon_product,config_file_path] order seperated by ',')
# 	wso2 product : true
#	other product : false,path/to/config/file/folder/
# Eg: export JAVA_OPTS="$JAVA_OPTS -javaagent:/path/to/instrumentation-agent-1.0-SNAPSHOT.jar=flase,path/to/config/folder/"
# content of configuration folder
# 	[data-agent-config.xml, inst-agent-config.xml, log4j.properties, client-trustore.jks]

export JAVA_OPTS="$JAVA_OPTS -javaagent:/path/to/agent/instrumentation-agent-1.0-SNAPSHOT.jar=true"


