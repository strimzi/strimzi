#!/usr/bin/env bash
set -e
set +x

export CLASSPATH="$CLASSPATH:/opt/cruise-control/libs/*"
export SCALA_VERSION="2.11.11"

# Generate temporary keystore password
CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)
export CERTS_STORE_PASSWORD

mkdir -p /tmp/cruise-control

# Import certificates into keystore and truststore
"$CRUISE_CONTROL_HOME"/cruise_control_tls_prepare_certificates.sh

export STRIMZI_TRUSTSTORE_LOCATION=/tmp/cruise-control/client-server.truststore.p12
export STRIMZI_TRUSTSTORE_PASSWORD="$CERTS_STORE_PASSWORD"

export STRIMZI_KEYSTORE_LOCATION=/tmp/cruise-control/client-server.keystore.p12
export STRIMZI_KEYSTORE_PASSWORD="$CERTS_STORE_PASSWORD"

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  export KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:$CRUISE_CONTROL_HOME/custom-config/log4j2.properties"
fi

# enabling Prometheus JMX exporter as Java agent
if [ "$CRUISE_CONTROL_METRICS_ENABLED" = "true" ]; then
  KAFKA_OPTS="${KAFKA_OPTS} -javaagent:$(ls "$KAFKA_HOME"/libs/jmx_prometheus_javaagent*.jar)=9404:$CRUISE_CONTROL_HOME/custom-config/metrics-config.yml"
  export KAFKA_OPTS
fi

if [ -z "$KAFKA_HEAP_OPTS" ] && [ -n "${DYNAMIC_HEAP_FRACTION}" ]; then
    . ./dynamic_resources.sh
    # Calculate a max heap size based some DYNAMIC_HEAP_FRACTION of the heap
    # available to a jvm using 100% of the GCroup-aware memory
    # up to some optional DYNAMIC_HEAP_MAX
    CALC_MAX_HEAP=$(get_heap_size "${DYNAMIC_HEAP_FRACTION}" "${DYNAMIC_HEAP_MAX}")
    if [ -n "$CALC_MAX_HEAP" ]; then
      export KAFKA_HEAP_OPTS="-Xms${CALC_MAX_HEAP} -Xmx${CALC_MAX_HEAP}"
    fi
fi

# Generate and print the config file
echo "Starting Cruise Control with configuration:"
"$CRUISE_CONTROL_HOME"/cruise_control_config_generator.sh | tee /tmp/cruisecontrol.properties | sed -e 's/password=.*/password=[hidden]/g'
echo ""

# JVM performance options
if [ -z "$KAFKA_JVM_PERFORMANCE_OPTS" ]; then
  KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true"
fi

# starting Cruise Control server with final configuration
# shellcheck disable=SC2086
exec /usr/bin/tini -w -e 143 -- java ${KAFKA_HEAP_OPTS} ${KAFKA_JVM_PERFORMANCE_OPTS} ${KAFKA_GC_LOG_OPTS} ${KAFKA_JMX_OPTS} ${KAFKA_LOG4J_OPTS} ${KAFKA_OPTS} -classpath "${CLASSPATH}" com.linkedin.kafka.cruisecontrol.KafkaCruiseControlMain /tmp/cruisecontrol.properties
