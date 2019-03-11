#!/usr/bin/env bash
export JAVA_CLASSPATH=lib/io.strimzi.kafka-init-0.12.0-SNAPSHOT.jar:lib/com.squareup.okhttp3.okhttp-3.9.1.jar:lib/com.squareup.okio.okio-1.13.0.jar:lib/com.fasterxml.jackson.module.jackson-module-jaxb-annotations-2.7.5.jar:lib/org.apache.logging.log4j.log4j-core-2.11.1.jar:lib/org.slf4j.slf4j-api-1.7.25.jar:lib/dk.brics.automaton.automaton-1.11-8.jar:lib/com.fasterxml.jackson.core.jackson-annotations-2.9.8.jar:lib/com.fasterxml.jackson.core.jackson-databind-2.9.8.jar:lib/io.fabric8.kubernetes-model-4.1.1.jar:lib/com.github.mifmif.generex-1.0.1.jar:lib/org.yaml.snakeyaml-1.23.jar:lib/org.slf4j.jul-to-slf4j-1.7.13.jar:lib/javax.validation.validation-api-1.1.0.Final.jar:lib/org.apache.logging.log4j.log4j-slf4j-impl-2.11.1.jar:lib/com.squareup.okhttp3.logging-interceptor-3.9.1.jar:lib/com.fasterxml.jackson.dataformat.jackson-dataformat-yaml-2.9.8.jar:lib/com.fasterxml.jackson.core.jackson-core-2.9.8.jar:lib/io.fabric8.kubernetes-client-4.1.1.jar:lib/io.fabric8.zjsonpatch-0.3.0.jar:lib/org.apache.logging.log4j.log4j-api-2.11.1.jar
export JAVA_MAIN=io.strimzi.kafka.init.Main
exec ${STRIMZI_HOME}/bin/launch_java.sh