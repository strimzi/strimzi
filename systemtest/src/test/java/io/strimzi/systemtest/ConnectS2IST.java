/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.utils.StUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

@Tag(REGRESSION)
class ConnectS2IST extends AbstractST {

    public static final String NAMESPACE = "connect-s2i-cluster-test";
    public static final String CONNECT_CLUSTER_NAME = "my-connect-cluster";
    private static final Logger LOGGER = LogManager.getLogger(ConnectS2IST.class);

    private static final String CONNECT_S2I_TOPIC_NAME = "connect-s2i-topic-example";

    @Test
    @OpenShiftOnly
    @Tag(ACCEPTANCE)
    void testDeployS2IWithMongoDBPlugin() {
        testClassResources().kafkaConnectS2I(CONNECT_CLUSTER_NAME, 1, CLUSTER_NAME)
            .editMetadata()
                .addToLabels("type", "kafka-connect-s2i")
            .endMetadata()
            .done();

        Map<String, String> connectSnapshot = StUtils.depConfigSnapshot(CLUSTER_NAME + "-connect");

        File dir = StUtils.downloadAndUnzip("https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/0.7.5/debezium-connector-mongodb-0.7.5-plugin.zip");

        // Start a new image build using the plugins directory
        cmdKubeClient().exec("oc", "start-build", CLUSTER_NAME + "-connect", "--from-dir", dir.getAbsolutePath(), "-n", NAMESPACE);
        // Wait for rolling update connect pods
        StUtils.waitTillDepConfigHasRolled(CLUSTER_NAME + "-connect", 1, connectSnapshot);
        String connectS2IPodName = kubeClient().listPods("type", "kafka-connect-s2i").get(0).getMetadata().getName();
        String plugins = cmdKubeClient().execInPod(connectS2IPodName, "curl", "-X", "GET", "http://localhost:8083/connector-plugins").out();

        assertThat(plugins, containsString("io.debezium.connector.mongodb.MongoDbConnector"));
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testSecretsWithKafkaConnectS2IWithTlsAndScramShaAuthentication() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewTls()
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                        .endTls()
                        .withNewKafkaListenerExternalNodePort()
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        final String userName = "user-example-one";

        testMethodResources().scramShaUser(CLUSTER_NAME, userName).done();

        StUtils.waitForSecretReady(userName);

        testMethodResources().kafkaConnectS2I(CONNECT_CLUSTER_NAME, 1, CLUSTER_NAME)
                .editMetadata()
                    .addToLabels("type", "kafka-connect-s2i")
                .endMetadata()
                .editSpec()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .withNewTls()
                        .addNewTrustedCertificate()
                            .withSecretName(CLUSTER_NAME + "-cluster-ca-cert")
                            .withCertificate("ca.crt")
                        .endTrustedCertificate()
                    .endTls()
                    .withBootstrapServers(CLUSTER_NAME + "-kafka-bootstrap:9093")
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(userName)
                        .withNewPasswordSecret()
                            .withSecretName(userName)
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha512()
                .endSpec()
                .done();

        testMethodResources().topic(CLUSTER_NAME, CONNECT_S2I_TOPIC_NAME).done();

        String kafkaConnectS2IPodName = kubeClient().listPods("type", "kafka-connect-s2i").get(0).getMetadata().getName();
        String kafkaConnectS2ILogs = kubeClient().logs(kafkaConnectS2IPodName);

        LOGGER.info("Verifying that in kafka connect logs are everything fine");
        assertThat(kafkaConnectS2ILogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink connector in pod {} with topic {}", kafkaConnectS2IPodName, CONNECT_S2I_TOPIC_NAME);
        StUtils.createFileSinkConnector(kafkaConnectS2IPodName, CONNECT_S2I_TOPIC_NAME);

        waitForClusterAvailabilityScramSha(userName, NAMESPACE, CLUSTER_NAME, CONNECT_S2I_TOPIC_NAME, 2);

        StUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectS2IPodName);

        assertThat(cmdKubeClient().execInPod(kafkaConnectS2IPodName, "/bin/bash", "-c", "cat /tmp/test-file-sink.txt").out(),
                containsString("0\n1\n"));
    }

    @BeforeEach
    void createTestResources() {
        createTestMethodResources();
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources().clusterOperator(NAMESPACE).done();
        deployTestSpecificResources();
    }

    void deployTestSpecificResources() {
        testClassResources().kafkaEphemeral(CLUSTER_NAME, 3, 1).done();
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        super.recreateTestEnv(coNamespace, bindingsNamespaces);
        deployTestSpecificResources();
    }
}
