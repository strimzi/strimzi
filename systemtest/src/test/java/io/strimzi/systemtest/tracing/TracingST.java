/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.tracing;

import io.fabric8.kubernetes.api.model.networking.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.listener.v2.KafkaListenerType;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeClientsResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaTracingClientsResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.specific.TracingUtils;
import io.strimzi.test.TestUtils;
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectS2IResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.CONNECT_S2I;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.TRACING;
import static io.strimzi.systemtest.bridge.HttpBridgeAbstractST.bridgePort;
import static io.strimzi.systemtest.bridge.HttpBridgeAbstractST.bridgeServiceName;
import static io.strimzi.test.TestUtils.getFileAsString;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(TRACING)
@Tag(INTERNAL_CLIENTS_USED)
@ExtendWith(VertxExtension.class)
public class TracingST extends AbstractST {

    private static final String NAMESPACE = "tracing-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(TracingST.class);

    private static final String JI_INSTALL_DIR = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/jaeger-instance/";
    private static final String JO_INSTALL_DIR = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/jaeger-operator/";

    private static final String JAEGER_PRODUCER_SERVICE = "hello-world-producer";
    private static final String JAEGER_CONSUMER_SERVICE = "hello-world-consumer";
    private static final String JAEGER_KAFKA_STREAMS_SERVICE = "hello-world-streams";
    private static final String JAEGER_MIRROR_MAKER_SERVICE = "my-mirror-maker";
    private static final String JAEGER_MIRROR_MAKER2_SERVICE = "my-mirror-maker2";
    private static final String JAEGER_KAFKA_CONNECT_SERVICE = "my-connect";
    private static final String JAEGER_KAFKA_CONNECT_S2I_SERVICE = "my-connect-s2i";
    private static final String JAEGER_KAFKA_BRIDGE_SERVICE = "my-kafka-bridge";

    protected static final String PRODUCER_JOB_NAME = "hello-world-producer";
    protected static final String CONSUMER_JOB_NAME = "hello-world-consumer";

    private static final String JAEGER_AGENT_NAME = "my-jaeger-agent";
    private static final String JAEGER_SAMPLER_TYPE = "const";
    private static final String JAEGER_SAMPLER_PARAM = "1";

    private static final String TOPIC_NAME = "my-topic";
    private static final String TOPIC_TARGET_NAME = "cipot-ym";

    private Stack<String> jaegerConfigs = new Stack<>();

    private String kafkaClientsPodName;

    private static KafkaTracingClientsResource kafkaTracingClient;

    @Test
    void testProducerService() {
        Map<String, Object> configOfSourceKafka = new HashMap<>();
        configOfSourceKafka.put("offsets.topic.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.min.isr", "1");

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME)
                .editSpec()
                    .withReplicas(1)
                    .withPartitions(12)
                .endSpec()
                .done();

        kafkaTracingClient.producerWithTracing().done();

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_NAME);
    }

    @Test
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testConnectService() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

        Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "1");
        configOfKafkaConnect.put("status.storage.replication.factor", "1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
                .withNewSpec()
                    .withConfig(configOfKafkaConnect)
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withBootstrapServers(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                    .withReplicas(1)
                    .withNewTemplate()
                        .withNewConnectContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_KAFKA_CONNECT_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endConnectContainer()
                    .endTemplate()
                .endSpec()
                .done();

        String kafkaConnectPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();
        String pathToConnectorSinkConfig = TestUtils.USER_PATH + "/../systemtest/src/test/resources/file/sink/connector.json";
        String connectorConfig = getFileAsString(pathToConnectorSinkConfig);

        LOGGER.info("Creating file sink in {}", pathToConnectorSinkConfig);
        cmdKubeClient().execInPod(kafkaConnectPodName, "/bin/bash", "-c", "curl -X POST -H \"Content-Type: application/json\" --data "
                + "'" + connectorConfig + "'" + " http://localhost:8083/connectors");

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(TEST_TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        TracingUtils.verify(JAEGER_KAFKA_CONNECT_SERVICE, kafkaClientsPodName);

        LOGGER.info("Deleting topic {} from CR", TEST_TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TEST_TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TEST_TOPIC_NAME);
    }

    @Test
    void testProducerWithStreamsService() {
        Map<String, Object> configOfSourceKafka = new HashMap<>();
        configOfSourceKafka.put("offsets.topic.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.min.isr", "1");

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .withConfig(configOfSourceKafka)
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_TARGET_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        kafkaTracingClient.producerWithTracing().done();

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName);

        kafkaTracingClient.kafkaStreamsWithTracing().done();

        TracingUtils.verify(JAEGER_KAFKA_STREAMS_SERVICE, kafkaClientsPodName);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_NAME);

        LOGGER.info("Deleting topic {} from CR", TOPIC_TARGET_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_TARGET_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_TARGET_NAME);
    }

    @Test
    void testProducerConsumerService() {
        Map<String, Object> configOfSourceKafka = new HashMap<>();
        configOfSourceKafka.put("offsets.topic.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.min.isr", "1");

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .withConfig(configOfSourceKafka)
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        kafkaTracingClient.producerWithTracing().done();

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName);

        kafkaTracingClient.consumerWithTracing().done();

        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_NAME);
    }

    @Test
    @Tag(ACCEPTANCE)
    void testProducerConsumerStreamsService() {
        Map<String, Object> configOfSourceKafka = new HashMap<>();
        configOfSourceKafka.put("offsets.topic.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.min.isr", "1");

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .withConfig(configOfSourceKafka)
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();


        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_TARGET_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        kafkaTracingClient.producerWithTracing().done();

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName);

        kafkaTracingClient.consumerWithTracing().done();

        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName);

        kafkaTracingClient.kafkaStreamsWithTracing().done();

        TracingUtils.verify(JAEGER_KAFKA_STREAMS_SERVICE, kafkaClientsPodName);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_NAME);

        LOGGER.info("Deleting topic {} from CR", TOPIC_TARGET_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_TARGET_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_TARGET_NAME);
    }

    @Test
    @Tag(MIRROR_MAKER2)
    void testProducerConsumerMirrorMaker2Service() {
        final String kafkaClusterSourceName = CLUSTER_NAME + "-source";
        final String kafkaClusterTargetName = CLUSTER_NAME + "-target";

        KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 3, 1)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

        KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 3, 1)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        KafkaTopicResource.topic(kafkaClusterSourceName, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        KafkaTopicResource.topic(kafkaClusterTargetName, kafkaClusterSourceName + "." + TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        LOGGER.info("Setting for kafka source plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));

        KafkaTracingClientsResource sourceKafkaTracingClient = new KafkaTracingClientsResource(kafkaTracingClient, KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));
        sourceKafkaTracingClient.producerWithTracing().done();

        LOGGER.info("Setting for kafka target plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));
        KafkaTracingClientsResource targetKafkaTracingClient = new KafkaTracingClientsResource(kafkaTracingClient, KafkaResources.plainBootstrapAddress(kafkaClusterTargetName), kafkaClusterSourceName + "." + TOPIC_NAME);
        targetKafkaTracingClient.consumerWithTracing().done();

        KafkaMirrorMaker2Resource.kafkaMirrorMaker2(CLUSTER_NAME, kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
                .editMetadata()
                    .withName("my-mirror-maker2")
                .endMetadata()
                .editSpec()
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withNewTemplate()
                        .withNewConnectContainer()
                            .addNewEnv()
                                .withNewName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_MIRROR_MAKER2_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endConnectContainer()
                    .endTemplate()
                .endSpec()
                .done();

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName, "To_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName, "From_" + kafkaClusterSourceName + "." + TOPIC_NAME);
        TracingUtils.verify(JAEGER_MIRROR_MAKER2_SERVICE, kafkaClientsPodName, "From_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_MIRROR_MAKER2_SERVICE, kafkaClientsPodName, "To_" + kafkaClusterSourceName + "." + TOPIC_NAME);
    }

    @Test
    @Tag(MIRROR_MAKER)
    void testProducerConsumerMirrorMakerService() {
        final String kafkaClusterSourceName = CLUSTER_NAME + "-source";
        final String kafkaClusterTargetName = CLUSTER_NAME + "-target";

        KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 3, 1)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

        KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 3, 1)
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        KafkaTopicResource.topic(kafkaClusterSourceName, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        KafkaTopicResource.topic(kafkaClusterTargetName, TOPIC_NAME + "-target")
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                    .withTopicName(TOPIC_NAME)
                .endSpec()
                .done();

        LOGGER.info("Setting for kafka source plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));

        KafkaTracingClientsResource sourceKafkaTracingClient = new KafkaTracingClientsResource(kafkaTracingClient, KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));
        sourceKafkaTracingClient.producerWithTracing().done();

        LOGGER.info("Setting for kafka target plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));

        KafkaTracingClientsResource targetKafkaTracingClient = new KafkaTracingClientsResource(kafkaTracingClient, KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));
        targetKafkaTracingClient.consumerWithTracing().done();

        KafkaMirrorMakerResource.kafkaMirrorMaker(CLUSTER_NAME, kafkaClusterSourceName, kafkaClusterTargetName,
            ClientUtils.generateRandomConsumerGroup(), 1, false)
                .editMetadata()
                    .withName("my-mirror-maker")
                .endMetadata()
                .editSpec()
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withNewTemplate()
                        .withNewMirrorMakerContainer()
                            .addNewEnv()
                                .withNewName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_MIRROR_MAKER_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endMirrorMakerContainer()
                    .endTemplate()
                .endSpec()
                .done();

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName, "To_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName, "From_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "From_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "To_" + TOPIC_NAME);
    }

    @Test
    @Tag(CONNECT)
    @Tag(MIRROR_MAKER)
    @Tag(CONNECT_COMPONENTS)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testProducerConsumerMirrorMakerConnectStreamsService() {
        final String kafkaClusterSourceName = CLUSTER_NAME + "-source";
        final String kafkaClusterTargetName = CLUSTER_NAME + "-target";

        KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 3, 1).done();
        KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 3, 1).done();

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        KafkaTopicResource.topic(kafkaClusterSourceName, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        KafkaTopicResource.topic(kafkaClusterSourceName, TOPIC_TARGET_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        KafkaTopicResource.topic(kafkaClusterTargetName, TOPIC_NAME + "-target")
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                    .withTopicName(TOPIC_NAME)
                .endSpec()
                .done();

        KafkaTopicResource.topic(kafkaClusterTargetName, TOPIC_TARGET_NAME + "-target")
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                    .withTopicName(TOPIC_TARGET_NAME)
                .endSpec()
                .done();

        KafkaTracingClientsResource sourceKafkaTracingClient = new KafkaTracingClientsResource(kafkaTracingClient, KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));
        sourceKafkaTracingClient.producerWithTracing().done();

        KafkaTracingClientsResource targetKafkaTracingClient = new KafkaTracingClientsResource(kafkaTracingClient, KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));
        targetKafkaTracingClient.consumerWithTracing().done();

        sourceKafkaTracingClient.kafkaStreamsWithTracing().done();

        Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "1");
        configOfKafkaConnect.put("status.storage.replication.factor", "1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
                .withNewSpec()
                    .withConfig(configOfKafkaConnect)
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withBootstrapServers(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
                    .withReplicas(1)
                    .withNewTemplate()
                        .withNewConnectContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_KAFKA_CONNECT_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endConnectContainer()
                    .endTemplate()
                .endSpec()
                .done();


        String kafkaConnectPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();
        String pathToConnectorSinkConfig = TestUtils.USER_PATH + "/../systemtest/src/test/resources/file/sink/connector.json";
        String connectorConfig = getFileAsString(pathToConnectorSinkConfig);

        LOGGER.info("Creating file sink in {}", pathToConnectorSinkConfig);
        cmdKubeClient().execInPod(kafkaConnectPodName, "/bin/bash", "-c", "curl -X POST -H \"Content-Type: application/json\" --data "
                + "'" + connectorConfig + "'" + " http://localhost:8083/connectors");

        KafkaMirrorMakerResource.kafkaMirrorMaker(CLUSTER_NAME, kafkaClusterSourceName, kafkaClusterTargetName,
            ClientUtils.generateRandomConsumerGroup(), 1, false)
                .editMetadata()
                    .withName("my-mirror-maker")
                .endMetadata()
                .editSpec()
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withNewTemplate()
                        .withNewMirrorMakerContainer()
                            .addNewEnv()
                                .withNewName("JAEGER_SERVICE_NAME")
                                .withValue("my-mirror-maker")
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endMirrorMakerContainer()
                    .endTemplate()
                .endSpec()
                .done();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(TEST_TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(kafkaClusterTargetName)
            .withMessageCount(MESSAGE_COUNT)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName, "To_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName, "From_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_KAFKA_CONNECT_SERVICE, kafkaClientsPodName, "From_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_KAFKA_STREAMS_SERVICE, kafkaClientsPodName, "From_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_KAFKA_STREAMS_SERVICE, kafkaClientsPodName, "To_" + TOPIC_TARGET_NAME);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "From_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "To_" + TOPIC_NAME);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "From_" + TOPIC_TARGET_NAME);
        TracingUtils.verify(JAEGER_MIRROR_MAKER_SERVICE, kafkaClientsPodName, "To_" + TOPIC_TARGET_NAME);
    }

    @Test
    @OpenShiftOnly
    @Tag(CONNECT_S2I)
    @Tag(CONNECT_COMPONENTS)
    void testConnectS2IService() {

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .addNewListValue()
                                .withName("tls")
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                            .endListValue()
                            .addNewListValue()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(false)
                            .endListValue()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .done();

        kafkaTracingClient.producerWithTracing().done();
        kafkaTracingClient.consumerWithTracing().done();

        final String kafkaConnectS2IName = "kafka-connect-s2i-name-1";

        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME).done();

        Map<String, Object> configOfKafkaConnectS2I = new HashMap<>();
        configOfKafkaConnectS2I.put("key.converter.schemas.enable", "false");
        configOfKafkaConnectS2I.put("value.converter.schemas.enable", "false");
        configOfKafkaConnectS2I.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnectS2I.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");


        KafkaConnectS2IResource.kafkaConnectS2I(kafkaConnectS2IName, CLUSTER_NAME, 1)
                .editSpec()
                    .withConfig(configOfKafkaConnectS2I)
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withNewTemplate()
                        .withNewConnectContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_KAFKA_CONNECT_S2I_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endConnectContainer()
                    .endTemplate()
                .endSpec()
                .done();

        String kafkaConnectS2IPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND).get(0).getMetadata().getName();
        String execPodName = kubeClient().listPodsByPrefixInName(KAFKA_CLIENTS_NAME).get(0).getMetadata().getName();

        LOGGER.info("Creating FileSink connect via Pod:{}", execPodName);
        KafkaConnectorUtils.createFileSinkConnector(execPodName, TEST_TOPIC_NAME, Constants.DEFAULT_SINK_FILE_PATH,
                KafkaConnectResources.url(kafkaConnectS2IName, NAMESPACE, 8083));

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(TEST_TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectS2IPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");

        TracingUtils.verify(JAEGER_PRODUCER_SERVICE, kafkaClientsPodName);
        TracingUtils.verify(JAEGER_CONSUMER_SERVICE, kafkaClientsPodName);
        TracingUtils.verify(JAEGER_KAFKA_CONNECT_S2I_SERVICE, kafkaClientsPodName);

        LOGGER.info("Deleting topic {} from CR", TEST_TOPIC_NAME);
        cmdKubeClient().deleteByName(KafkaTopic.RESOURCE_KIND, TEST_TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TEST_TOPIC_NAME);
    }

    @Tag(NODEPORT_SUPPORTED)
    @Tag(BRIDGE)
    @Test
    void testKafkaBridgeService() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();

        // Deploy http bridge
        KafkaBridgeResource.kafkaBridge(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1)
            .editSpec()
                .withNewJaegerTracing()
                .endJaegerTracing()
                    .withNewTemplate()
                        .withNewBridgeContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_KAFKA_BRIDGE_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endBridgeContainer()
                    .endTemplate()
            .endSpec()
            .done();

        String bridgeProducer = "bridge-producer";
        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();

        KafkaBridgeClientsResource kafkaBridgeClientJob = new KafkaBridgeClientsResource(bridgeProducer, "", bridgeServiceName,
            TOPIC_NAME, MESSAGE_COUNT, "", ClientUtils.generateRandomConsumerGroup(), bridgePort, 1000, 1000);

        kafkaBridgeClientJob.producerStrimziBridge().done();
        ClientUtils.waitForClientSuccess(bridgeProducer, NAMESPACE, MESSAGE_COUNT);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .build();

        assertThat(internalKafkaClient.receiveMessagesPlain(), is(MESSAGE_COUNT));

        TracingUtils.verify(JAEGER_KAFKA_BRIDGE_SERVICE, kafkaClientsPodName);
    }

    /**
     * Delete Jaeger instance
     */
    void deleteJaeger() {
        while (!jaegerConfigs.empty()) {
            cmdKubeClient().clientWithAdmin().namespace(cluster.getNamespace()).deleteContent(jaegerConfigs.pop());
        }
    }

    private void deployJaeger() {
        LOGGER.info("=== Applying jaeger operator install files ===");

        Map<File, String> operatorFiles = Arrays.stream(Objects.requireNonNull(new File(JO_INSTALL_DIR).listFiles())
        ).collect(Collectors.toMap(file -> file, f -> TestUtils.getContent(f, TestUtils::toYamlString), (x, y) -> x, LinkedHashMap::new));

        for (Map.Entry<File, String> entry : operatorFiles.entrySet()) {
            LOGGER.info("Applying configuration file: {}", entry.getKey());
            jaegerConfigs.push(entry.getValue());
            cmdKubeClient().clientWithAdmin().namespace(cluster.getNamespace()).applyContent(entry.getValue());
        }

        installJaegerInstance();

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
            .withNewApiVersion("networking.k8s.io/v1")
            .withNewKind("NetworkPolicy")
            .withNewMetadata()
                .withName("jaeger-allow")
            .endMetadata()
            .withNewSpec()
                .addNewIngress()
                .endIngress()
                .withNewPodSelector()
                    .addToMatchLabels("app", "jaeger")
                .endPodSelector()
                .withPolicyTypes("Ingress")
            .endSpec()
            .build();

        LOGGER.debug("Going to apply the following NetworkPolicy: {}", networkPolicy.toString());
        KubernetesResource.deleteLater(kubeClient().getClient().network().networkPolicies().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(networkPolicy));
        LOGGER.info("Network policy for jaeger successfully applied");
    }

    /**
     * Install of Jaeger instance
     */
    void installJaegerInstance() {
        LOGGER.info("=== Applying jaeger instance install files ===");

        Map<File, String> operatorFiles = Arrays.stream(Objects.requireNonNull(new File(JI_INSTALL_DIR).listFiles())
        ).collect(Collectors.toMap(file -> file, f -> TestUtils.getContent(f, TestUtils::toYamlString), (x, y) -> x, LinkedHashMap::new));

        for (Map.Entry<File, String> entry : operatorFiles.entrySet()) {
            LOGGER.info("Applying configuration file: {}", entry.getKey());
            jaegerConfigs.push(entry.getValue());
            cmdKubeClient().clientWithAdmin().namespace(cluster.getNamespace()).applyContent(entry.getValue());
        }
    }

    @AfterEach
    void tearDown() {
        deleteJaeger();
    }

    @BeforeEach
    void createTestResources() {
        // deployment of the jaeger
        deployJaeger();

        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME).done();

        kafkaClientsPodName = kubeClient().listPodsByPrefixInName(KAFKA_CLIENTS_NAME).get(0).getMetadata().getName();
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);

        kafkaTracingClient = new KafkaTracingClientsResource(PRODUCER_JOB_NAME, CONSUMER_JOB_NAME,
            KafkaResources.plainBootstrapAddress(CLUSTER_NAME), TOPIC_NAME, MESSAGE_COUNT, "", ClientUtils.generateRandomConsumerGroup(),
            JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE, JAEGER_KAFKA_STREAMS_SERVICE);
    }
}
