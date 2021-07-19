#!/usr/bin/env bash
set -e

SCRIPT_DIRS="docker-images/operator/scripts/*.sh
cluster-operator/scripts/*.sh
user-operator/scripts/*.sh
topic-operator/scripts/*.sh
kafka-init/scripts/*.sh
docker-images/kafka/scripts/*.sh
docker-images/kafka/cruise-control-scripts/*.sh
docker-images/kafka/stunnel-scripts/*.sh
docker-images/kafka/exporter-scripts/*.sh
docker-images/jmxtrans/*.sh
tools/olm-bundle/*.sh"

for SCRIPTS in $SCRIPT_DIRS; do
    shellcheck -a -P $(dirname "$SCRIPTS") -x "$SCRIPTS"
done
