#!/usr/bin/env bash
set -e

CC_CAPACITY_FILE="/tmp/capacity.json"
CC_CLUSTER_CONFIG_FILE="/tmp/clusterConfig.json"
CC_ACCESS_LOG="/tmp/access.log"

# Generate capacity file
cat <<EOF > $CC_CAPACITY_FILE
{
	"brokerCapacities": [{
		"brokerId": "-1",
		"capacity": {
			"DISK": "$BROKER_DISK_MIB_CAPACITY",
			"CPU": "$BROKER_CPU_UTILIZATION_CAPACITY",
			"NW_IN": "$BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY",
			"NW_OUT": "$BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY"
		},
		"doc": "This is the default capacity. Capacity unit used for disk is in MB, cpu is in percentage, network throughput is in KB."
	}]
}
EOF

# Generate cluster config
cat <<EOF > $CC_CLUSTER_CONFIG_FILE
{
min.insync.replicas=$MIN_INSYNC_REPLICAS
}
EOF

# Write all webserver access logs to stdout
ln -sf /dev/stdout $CC_ACCESS_LOG

# Write the config file
cat <<EOF
bootstrap.servers=$STRIMZI_KAFKA_BOOTSTRAP_SERVERS
zookeeper.connect=localhost:2181
partition.metric.sample.store.topic=strimzi.cruisecontrol.partitionmetricsamples
broker.metric.sample.store.topic=strimzi.cruisecontrol.modeltrainingsamples
metric.reporter.topic=strimzi.cruisecontrol.metrics
capacity.config.file=$CC_CAPACITY_FILE
cluster.configs.file=$CC_CLUSTER_CONFIG_FILE
webserver.accesslog.path=$CC_ACCESS_LOG
webserver.http.address=0.0.0.0
webserver.http.cors.allowmethods=OPTIONS,GET
webserver.ssl.keystore.location=/tmp/cruise-control/client-server.keystore.p12
webserver.ssl.keystore.password=$CERTS_STORE_PASSWORD
webserver.ssl.keystore.type=PKCS12
webserver.ssl.key.password=$CERTS_STORE_PASSWORD
security.protocol=SSL
ssl.keystore.type=PKCS12
ssl.keystore.location=/tmp/cruise-control/client-server.keystore.p12
ssl.keystore.password=$CERTS_STORE_PASSWORD
ssl.truststore.type=PKCS12
ssl.truststore.location=/tmp/cruise-control/client-server.truststore.p12
ssl.truststore.password=$CERTS_STORE_PASSWORD
${CRUISE_CONTROL_CONFIGURATION}
EOF
