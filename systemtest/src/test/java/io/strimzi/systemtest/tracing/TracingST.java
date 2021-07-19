/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.tracing;

import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.SetupClusterOperator;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.resources.ResourceItem;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeExampleClients;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaTracingExampleClients;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.TracingUtils;
import io.strimzi.test.TestUtils;
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.KAFKA_TRACING_CLIENT_KEY;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.NAMESPACE_KEY;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.TRACING;
import static io.strimzi.systemtest.bridge.HttpBridgeAbstractST.bridgePort;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
@Tag(TRACING)
@Tag(INTERNAL_CLIENTS_USED)
@ExtendWith(VertxExtension.class)
public class TracingST extends AbstractST {

    private static final String NAMESPACE = "tracing-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(TracingST.class);

    private static final String JAEGER_INSTANCE_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/jaeger-instance.yaml";
    private static final String JAEGER_OPERATOR_FILES_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/operator-files/";

    private static final String JAEGER_PRODUCER_SERVICE = "hello-world-producer";
    private static final String JAEGER_CONSUMER_SERVICE = "hello-world-consumer";
    private static final String JAEGER_KAFKA_STREAMS_SERVICE = "hello-world-streams";
    private static final String JAEGER_MIRROR_MAKER_SERVICE = "my-mirror-maker";
    private static final String JAEGER_MIRROR_MAKER2_SERVICE = "my-mirror-maker2";
    private static final String JAEGER_KAFKA_CONNECT_SERVICE = "my-connect";
    private static final String JAEGER_KAFKA_BRIDGE_SERVICE = "my-kafka-bridge";

    protected static final String PRODUCER_JOB_NAME = "hello-world-producer";
    protected static final String CONSUMER_JOB_NAME = "hello-world-consumer";

    private static final String JAEGER_INSTANCE_NAME = "my-jaeger";
    private static final String JAEGER_SAMPLER_TYPE = "const";
    private static final String JAEGER_SAMPLER_PARAM = "1";
    private static final String JAEGER_OPERATOR_DEPLOYMENT_NAME = "jaeger-operator";
    private static final String JAEGER_AGENT_NAME = JAEGER_INSTANCE_NAME + "-agent";
    private static final String JAEGER_QUERY_SERVICE = JAEGER_INSTANCE_NAME + "-query";

    private static final String JAEGER_VERSION = "1.22.1";

    private Stack<String> jaegerConfigs = new Stack<>();

    private String kafkaClientsPodName;

    private static KafkaTracingExampleClients kafkaTracingClient;

    private String topicName = "";
    private String streamsTopicTargetName = "";
    private String kafkaClientsName = NAMESPACE + "-shared-kafka-clients";

    @ParallelNamespaceTest
    void testProducerService(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(), 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(),
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString())
            .editSpec()
                .withReplicas(1)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext,
            ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).producerWithTracing().build());

        TracingUtils.verify(
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(),
            JAEGER_PRODUCER_SERVICE,
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
            JAEGER_QUERY_SERVICE);
    }

    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testConnectService(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(), 3, 1).build());


        Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "1");
        configOfKafkaConnect.put("status.storage.replication.factor", "1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .withNewSpec()
                .withConfig(configOfKafkaConnect)
                .withNewJaegerTracing()
                .endJaegerTracing()
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString()))
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
            .build());

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString())
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString())
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).producerWithTracing().build());
        resourceManager.createResource(extensionContext, ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).consumerWithTracing().build());

        TracingUtils.verify(
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(),
            JAEGER_KAFKA_CONNECT_SERVICE,
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
            JAEGER_QUERY_SERVICE);
    }

    @ParallelNamespaceTest
    void testProducerWithStreamsService(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());
        String targetTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaEphemeral(
                extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(), 3, 1).build());
        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(),
                extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString())
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(), targetTopicName)
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).producerWithTracing().build());

        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(),
            JAEGER_PRODUCER_SERVICE,
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
            JAEGER_QUERY_SERVICE);

        resourceManager.createResource(extensionContext, ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).kafkaStreamsWithTracing().build());

        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(),
            JAEGER_KAFKA_STREAMS_SERVICE,
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
            JAEGER_QUERY_SERVICE);
    }

    @ParallelNamespaceTest
    void testProducerConsumerService(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaEphemeral(
                extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(), 3, 1).build());
        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(),
                extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString())
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).producerWithTracing().build());

        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(),
            JAEGER_PRODUCER_SERVICE,
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
            JAEGER_QUERY_SERVICE);

        resourceManager.createResource(extensionContext, ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).consumerWithTracing().build());

        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(),
            JAEGER_CONSUMER_SERVICE,
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
            JAEGER_QUERY_SERVICE);
    }

    @ParallelNamespaceTest
    @Tag(ACCEPTANCE)
    void testProducerConsumerStreamsService(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaEphemeral(
                extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(), 3, 1).build());

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(),
                extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString())
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(),
                extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.STREAM_TOPIC_KEY).toString())
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).producerWithTracing().build());

        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(),
            JAEGER_PRODUCER_SERVICE,
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
            JAEGER_QUERY_SERVICE);

        resourceManager.createResource(extensionContext, ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).consumerWithTracing().build());

        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(),
            JAEGER_CONSUMER_SERVICE,
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
            JAEGER_QUERY_SERVICE);

        resourceManager.createResource(extensionContext, ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).kafkaStreamsWithTracing().build());

        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(),
            JAEGER_KAFKA_STREAMS_SERVICE,
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
            JAEGER_QUERY_SERVICE);
    }

    @ParallelNamespaceTest
    @Tag(MIRROR_MAKER2)
    void testProducerConsumerMirrorMaker2Service(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        final String kafkaClusterSourceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString();
        final String kafkaClusterTargetName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString() + "-target";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 3, 1)
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withSize("10")
                        .withDeleteClaim(true)
                    .endPersistentClaimStorage()
                .endKafka()
                .editZookeeper()
                    .withNewPersistentClaimStorage()
                        .withSize("10")
                        .withDeleteClaim(true)
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec()
            .build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString())
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterTargetName, kafkaClusterSourceName + "." + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString())
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        LOGGER.info("Setting for kafka source plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));

        KafkaTracingExampleClients sourceKafkaTracingClient = ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).toBuilder()
            .withTopicName(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .build();

        resourceManager.createResource(extensionContext, sourceKafkaTracingClient.producerWithTracing().build());

        LOGGER.info("Setting for kafka target plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));

        KafkaTracingExampleClients targetKafkaTracingClient = ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .withTopicName(kafkaClusterSourceName + "." + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString())
            .build();

        resourceManager.createResource(extensionContext, targetKafkaTracingClient.consumerWithTracing().build());

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(), kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
            .editSpec()
                .withNewJaegerTracing()
                .endJaegerTracing()
                .withNewTemplate()
                    .withNewConnectContainer()
                        .addNewEnv()
                            .withName("JAEGER_SERVICE_NAME")
                            .withValue(JAEGER_MIRROR_MAKER2_SERVICE)
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
            .build());

        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(),
            JAEGER_PRODUCER_SERVICE,
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
            "To_" + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString(),
            JAEGER_QUERY_SERVICE);
        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(),
            JAEGER_CONSUMER_SERVICE, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "From_" + kafkaClusterSourceName + "." + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(),
            JAEGER_MIRROR_MAKER2_SERVICE,
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
            "From_" + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString(),
            JAEGER_QUERY_SERVICE);
        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(),
            JAEGER_MIRROR_MAKER2_SERVICE, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(),
            "To_" + kafkaClusterSourceName + "." + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString(), JAEGER_QUERY_SERVICE);
    }

    @ParallelNamespaceTest
    @Tag(MIRROR_MAKER)
    void testProducerConsumerMirrorMakerService(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        final String kafkaClusterSourceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString();
        final String kafkaClusterTargetName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString() + "-target";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 3, 1).build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString())
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterTargetName, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString() + "-target")
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
                .withTopicName(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString())
            .endSpec()
            .build());

        LOGGER.info("Setting for kafka source plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));

        KafkaTracingExampleClients sourceKafkaTracingClient = ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .build();

        resourceManager.createResource(extensionContext, sourceKafkaTracingClient.producerWithTracing().build());

        LOGGER.info("Setting for kafka target plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));

        KafkaTracingExampleClients targetKafkaTracingClient =  ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .build();

        resourceManager.createResource(extensionContext, targetKafkaTracingClient.consumerWithTracing().build());

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(), kafkaClusterSourceName, kafkaClusterTargetName,
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
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_MIRROR_MAKER_SERVICE)
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
                        .endMirrorMakerContainer()
                    .endTemplate()
                .endSpec()
                .build());

        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(), JAEGER_PRODUCER_SERVICE, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "To_" + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(), JAEGER_CONSUMER_SERVICE, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "From_" + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(), JAEGER_MIRROR_MAKER_SERVICE, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "From_" + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(), JAEGER_MIRROR_MAKER_SERVICE, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "To_" + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString(), JAEGER_QUERY_SERVICE);
    }

    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(MIRROR_MAKER)
    @Tag(CONNECT_COMPONENTS)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testProducerConsumerMirrorMakerConnectStreamsService(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        final String kafkaClusterSourceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString();
        final String kafkaClusterTargetName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString() + "-target";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 3, 1).build());

        // Create topic and deploy clients before Mirror Maker to not wait for MM to find the new topics
        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(kafkaClusterSourceName,
                extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString())
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(kafkaClusterSourceName,
                extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.STREAM_TOPIC_KEY).toString())
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterTargetName,
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString() + "-target")
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
                .withTopicName(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString())
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(kafkaClusterTargetName,
                extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.STREAM_TOPIC_KEY).toString() + "-target")
            .editSpec()
                .withReplicas(3)
                .withPartitions(12)
                .withTopicName(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.STREAM_TOPIC_KEY).toString())
            .endSpec()
            .build());

        KafkaTracingExampleClients sourceKafkaTracingClient = ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .build();

        resourceManager.createResource(extensionContext, sourceKafkaTracingClient.producerWithTracing().build());

        KafkaTracingExampleClients targetKafkaTracingClient = ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).toBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .build();

        resourceManager.createResource(extensionContext, targetKafkaTracingClient.consumerWithTracing().build());
        resourceManager.createResource(extensionContext, sourceKafkaTracingClient.kafkaStreamsWithTracing().build());

        Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "1");
        configOfKafkaConnect.put("status.storage.replication.factor", "1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
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
            .build());

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString())
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString())
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(), kafkaClusterSourceName, kafkaClusterTargetName,
            ClientUtils.generateRandomConsumerGroup(), 1, false)
                .editSpec()
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withNewTemplate()
                        .withNewMirrorMakerContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue("my-mirror-maker")
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
                        .endMirrorMakerContainer()
                    .endTemplate()
                .endSpec()
                .build());

        KafkaTracingExampleClients kafkaTracingClient = ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).toBuilder()
            .withProducerName(PRODUCER_JOB_NAME + "-x")
            .withConsumerName(CONSUMER_JOB_NAME + "-x")
            .withTopicName(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.STREAM_TOPIC_KEY).toString())
            .build();

        resourceManager.createResource(extensionContext, kafkaTracingClient.producerStrimzi().build());
        resourceManager.createResource(extensionContext, kafkaTracingClient.consumerStrimzi().build());

        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(), JAEGER_PRODUCER_SERVICE, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "To_" + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(), JAEGER_CONSUMER_SERVICE, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "From_" + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(), JAEGER_KAFKA_CONNECT_SERVICE, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "From_" + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(), JAEGER_KAFKA_STREAMS_SERVICE, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "From_" + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(), JAEGER_KAFKA_STREAMS_SERVICE, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "To_" + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.STREAM_TOPIC_KEY).toString(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(), JAEGER_MIRROR_MAKER_SERVICE, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "From_" + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(), JAEGER_MIRROR_MAKER_SERVICE, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "To_" + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(), JAEGER_MIRROR_MAKER_SERVICE, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "From_" + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.STREAM_TOPIC_KEY).toString(), JAEGER_QUERY_SERVICE);
        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(), JAEGER_MIRROR_MAKER_SERVICE, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(), "To_" + extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.STREAM_TOPIC_KEY).toString(), JAEGER_QUERY_SERVICE);
    }

    @Tag(NODEPORT_SUPPORTED)
    @Tag(BRIDGE)
    @ParallelNamespaceTest
    void testKafkaBridgeService(ExtensionContext extensionContext) {
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(), 3, 1).build());
        // Deploy http bridge
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(), KafkaResources.plainBootstrapAddress(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString()), 1)
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
            .build());

        String bridgeProducer = "bridge-producer";
        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString(), extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString())
                .build());

        KafkaBridgeExampleClients kafkaBridgeClientJob = new KafkaBridgeExampleClients.Builder()
            .withProducerName(bridgeProducer)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString()))
            .withTopicName(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString())
            .withMessageCount(MESSAGE_COUNT)
            .withPort(bridgePort)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .build();

        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.producerStrimziBridge().build());
        resourceManager.createResource(extensionContext, ((KafkaTracingExampleClients) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(KAFKA_TRACING_CLIENT_KEY)).consumerWithTracing().build());
        ClientUtils.waitForClientSuccess(bridgeProducer, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(), MESSAGE_COUNT);

        TracingUtils.verify(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(NAMESPACE_KEY).toString(), JAEGER_KAFKA_BRIDGE_SERVICE, extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_POD_KEY).toString(), JAEGER_QUERY_SERVICE);
    }

    /**
     * Delete Jaeger instance
     */
    void deleteJaeger() {
        while (!jaegerConfigs.empty()) {
            cmdKubeClient().namespace(cluster.getNamespace()).deleteContent(jaegerConfigs.pop());
        }
    }

    private void deployJaegerContent() throws FileNotFoundException {
        File folder = new File(JAEGER_OPERATOR_FILES_PATH);
        File[] files = folder.listFiles();

        if (files != null && files.length > 0) {
            for (File file : files) {
                String yamlContent = TestUtils.setMetadataNamespace(file, NAMESPACE)
                    .replace("namespace: \"observability\"", "namespace: \"" + NAMESPACE + "\"");
                jaegerConfigs.push(yamlContent);
                LOGGER.info("Creating {} from {}", file.getName(), file.getAbsolutePath());
                cmdKubeClient(NAMESPACE).applyContent(yamlContent);
            }
        } else {
            throw new FileNotFoundException("Folder with Jaeger files is empty or doesn't exist");
        }
    }

    private void deployJaegerOperator(ExtensionContext extensionContext) throws IOException, FileNotFoundException {
        LOGGER.info("=== Applying jaeger operator install files ===");

        deployJaegerContent();

        ResourceManager.STORED_RESOURCES.computeIfAbsent(extensionContext.getDisplayName(), k -> new Stack<>());
        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(new ResourceItem(() -> this.deleteJaeger()));
        DeploymentUtils.waitForDeploymentAndPodsReady(NAMESPACE, JAEGER_OPERATOR_DEPLOYMENT_NAME, 1);

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
            .withApiVersion("networking.k8s.io/v1")
            .withKind(Constants.NETWORK_POLICY)
            .withNewMetadata()
                .withName("jaeger-allow")
                .withNamespace(NAMESPACE)
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
        resourceManager.createResource(extensionContext, networkPolicy);
        LOGGER.info("Network policy for jaeger successfully applied");
    }

    /**
     * Install of Jaeger instance
     */
    void deployJaegerInstance(ExtensionContext extensionContext, String namespaceName) {
        LOGGER.info("=== Applying jaeger instance install file ===");

        String instanceYamlContent = TestUtils.getContent(new File(JAEGER_INSTANCE_PATH), TestUtils::toYamlString);
        cmdKubeClient(namespaceName).applyContent(instanceYamlContent.replaceAll("image: 'jaegertracing/all-in-one:*'", "image: 'jaegertracing/all-in-one:" + JAEGER_VERSION.substring(0, 4) + "'"));
        ResourceManager.STORED_RESOURCES.computeIfAbsent(extensionContext.getDisplayName(), k -> new Stack<>());
        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(new ResourceItem(() -> cmdKubeClient(namespaceName).deleteContent(instanceYamlContent)));
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, JAEGER_INSTANCE_NAME, 1);
    }

    @BeforeEach
    void createTestResources(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String streamsTopicTargetName = KafkaTopicUtils.generateRandomNameOfTopic();
        final String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        final String producerName = clusterName + PRODUCER_JOB_NAME;
        final String consumerName = clusterName + CONSUMER_JOB_NAME;

        deployJaegerInstance(extensionContext, namespaceName);

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(namespaceName, false, kafkaClientsName).build());

        final String kafkaClientsPodName = kubeClient(namespaceName).listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        final KafkaTracingExampleClients kafkaTracingClient = new KafkaTracingExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
            .withTopicName(topicName)
            .withStreamsTopicTargetName(streamsTopicTargetName)
            .withMessageCount(MESSAGE_COUNT)
            .withJaegerServiceProducerName(JAEGER_PRODUCER_SERVICE)
            .withJaegerServiceConsumerName(JAEGER_CONSUMER_SERVICE)
            .withJaegerServiceStreamsName(JAEGER_KAFKA_STREAMS_SERVICE)
            .withJaegerServiceAgentName(JAEGER_AGENT_NAME)
            .build();

        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.NAMESPACE_KEY, namespaceName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.CLUSTER_KEY, clusterName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.TOPIC_KEY, topicName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.STREAM_TOPIC_KEY, streamsTopicTargetName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.KAFKA_CLIENTS_KEY, kafkaClientsName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.KAFKA_CLIENTS_POD_KEY, kafkaClientsPodName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PRODUCER_KEY, producerName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.CONSUMER_KEY, consumerName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.KAFKA_TRACING_CLIENT_KEY, kafkaTracingClient);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) throws IOException {
        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(NAMESPACE)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .createInstallation()
            .runInstallation();
        // deployment of the jaeger
        deployJaegerOperator(extensionContext);
    }
}
