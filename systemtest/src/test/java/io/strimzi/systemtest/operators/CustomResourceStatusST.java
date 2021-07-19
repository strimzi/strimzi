/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.KafkaBridgeStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.status.KafkaMirrorMaker2Status;
import io.strimzi.api.kafka.model.status.KafkaMirrorMakerStatus;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.status.KafkaTopicStatus;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.SetupClusterOperator;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMaker2Utils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMakerUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.KafkaResources.externalBootstrapServiceName;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECTOR_OPERATOR;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils.getKafkaSecretCertificates;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils.getKafkaStatusCertificates;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;

@Tag(REGRESSION)
class CustomResourceStatusST extends AbstractST {
    static final String NAMESPACE = "status-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(CustomResourceStatusST.class);
    private static int topicOperatorReconciliationInterval;
    private static final String CUSTOM_RESOURCE_STATUS_CLUSTER_NAME = "custom-resource-status-cluster-name";
    private static final String EXAMPLE_TOPIC_NAME = "example-topic-name";

    @ParallelTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testKafkaStatus(ExtensionContext extensionContext) {
        LOGGER.info("Checking status of deployed kafka cluster");
        KafkaUtils.waitForKafkaReady(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesPlain(),
            basicExternalKafkaClient.receiveMessagesPlain()
        );

        assertKafkaStatus(1, KafkaResources.bootstrapServiceName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME) + ".status-cluster-test.svc");

        KafkaResource.replaceKafkaResource(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, k -> {
            k.getSpec().getEntityOperator().getTopicOperator().setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100000m"))
                    .build());
        });

        LOGGER.info("Wait until cluster will be in NotReady state ...");
        KafkaUtils.waitForKafkaNotReady(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);

        LOGGER.info("Recovery cluster to Ready state ...");
        KafkaResource.replaceKafkaResource(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, k -> {
            k.getSpec().getEntityOperator().getTopicOperator().setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100m"))
                    .build());
        });

        KafkaUtils.waitForKafkaReady(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        assertKafkaStatus(3, KafkaResources.bootstrapServiceName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME) + ".status-cluster-test.svc");
    }

    @ParallelTest
    void testKafkaUserStatus(ExtensionContext extensionContext) {
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, userName).build());

        LOGGER.info("Checking status of deployed KafkaUser");
        Condition kafkaCondition = KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(userName).get().getStatus().getConditions().get(0);
        LOGGER.info("KafkaUser Status: {}", kafkaCondition.getStatus());
        LOGGER.info("KafkaUser Type: {}", kafkaCondition.getType());
        assertThat("KafkaUser is in wrong state!", kafkaCondition.getType(), is(Ready.toString()));
        LOGGER.info("KafkaUser is in desired state: Ready");
    }

    @ParallelTest
    void testKafkaUserStatusNotReady(ExtensionContext extensionContext) {
        // Simulate NotReady state with userName longer than 64 characters
        String userName = "sasl-use-rabcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdef";

        resourceManager.createResource(extensionContext, false, KafkaUserTemplates.defaultUser(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, userName).build());

        KafkaUserUtils.waitForKafkaUserNotReady(userName);

        LOGGER.info("Checking status of deployed KafkaUser {}", userName);
        Condition kafkaCondition = KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(userName).get().getStatus().getConditions().get(0);
        LOGGER.info("KafkaUser Status: {}", kafkaCondition.getStatus());
        LOGGER.info("KafkaUser Type: {}", kafkaCondition.getType());
        LOGGER.info("KafkaUser Message: {}", kafkaCondition.getMessage());
        LOGGER.info("KafkaUser Reason: {}", kafkaCondition.getReason());
        assertThat("KafkaUser is in wrong state!", kafkaCondition.getType(), is(NotReady.toString()));
        LOGGER.info("KafkaUser {} is in desired state: {}", userName, kafkaCondition.getType());

        KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(userName).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(userName);
    }

    @ParallelTest
    @Tag(MIRROR_MAKER)
    void testKafkaMirrorMakerStatus(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String mirrorMakerName = clusterName + "-mirror-maker";

        // Deploy Mirror Maker
        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(mirrorMakerName, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, ClientUtils.generateRandomConsumerGroup(), 1, false).build());
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(mirrorMakerName);
        assertKafkaMirrorMakerStatus(1, mirrorMakerName);
        // Corrupt Mirror Maker pods
        KafkaMirrorMakerResource.replaceMirrorMakerResource(mirrorMakerName, mm -> mm.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerNotReady(mirrorMakerName);
        // Restore Mirror Maker pod
        KafkaMirrorMakerResource.replaceMirrorMakerResource(mirrorMakerName, mm -> mm.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100m"))
                .build()));
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(mirrorMakerName);
        assertKafkaMirrorMakerStatus(3, mirrorMakerName);
    }

    @ParallelTest
    @Tag(MIRROR_MAKER)
    void testKafkaMirrorMakerStatusWrongBootstrap(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String mirrorMakerName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(mirrorMakerName, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, ClientUtils.generateRandomConsumerGroup(), 1, false).build());
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(mirrorMakerName);
        assertKafkaMirrorMakerStatus(1, mirrorMakerName);
        // Corrupt Mirror Maker pods
        KafkaMirrorMakerResource.replaceMirrorMakerResource(mirrorMakerName, mm -> mm.getSpec().getConsumer().setBootstrapServers("non-exists-bootstrap"));
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerNotReady(mirrorMakerName);
        // Restore Mirror Maker pods
        KafkaMirrorMakerResource.replaceMirrorMakerResource(mirrorMakerName, mm -> mm.getSpec().getConsumer().setBootstrapServers(KafkaResources.plainBootstrapAddress(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME)));
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(mirrorMakerName);
        assertKafkaMirrorMakerStatus(3, mirrorMakerName);
    }

    @ParallelTest
    @Tag(BRIDGE)
    void testKafkaBridgeStatus(ExtensionContext extensionContext) {
        String bridgeUrl = KafkaBridgeResources.url(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, NAMESPACE, 8080);

        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME), 1).build());
        KafkaBridgeUtils.waitForKafkaBridgeReady(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        assertKafkaBridgeStatus(1, bridgeUrl);

        KafkaBridgeResource.replaceBridgeResource(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        KafkaBridgeUtils.waitForKafkaBridgeNotReady(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);

        KafkaBridgeResource.replaceBridgeResource(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("10m"))
                .build()));
        KafkaBridgeUtils.waitForKafkaBridgeReady(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        assertKafkaBridgeStatus(3, bridgeUrl);
    }

    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECTOR_OPERATOR)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaConnectAndConnectorStatus(ExtensionContext extensionContext) {
        String connectUrl = KafkaConnectResources.url(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, NAMESPACE, 8083);

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME)
            .editSpec()
                .addToConfig("topic", EXAMPLE_TOPIC_NAME)
            .endSpec()
            .build());

        assertKafkaConnectStatus(1, connectUrl);
        assertKafkaConnectorStatus(1, "RUNNING|UNASSIGNED", "source", List.of());

        KafkaConnectResource.replaceKafkaConnectResource(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        KafkaConnectUtils.waitForConnectNotReady(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);

        KafkaConnectResource.replaceKafkaConnectResource(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100m"))
                .build()));
        KafkaConnectUtils.waitForConnectReady(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        assertKafkaConnectStatus(3, connectUrl);

        KafkaConnectorResource.replaceKafkaConnectorResource(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME,
            kc -> kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, "non-existing-connect-cluster")));
        KafkaConnectorUtils.waitForConnectorNotReady(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        assertThat(KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME).get().getStatus().getConnectorStatus(), is(nullValue()));

        KafkaConnectorResource.replaceKafkaConnectorResource(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME,
            kc -> kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME)));
        KafkaConnectorUtils.waitForConnectorReady(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        assertKafkaConnectorStatus(1, "RUNNING|UNASSIGNED", "source", List.of(EXAMPLE_TOPIC_NAME));

        String defaultClass = KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME).get().getSpec().getClassName();

        KafkaConnectorResource.replaceKafkaConnectorResource(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME,
            kc -> kc.getSpec().setClassName("non-existing-class"));
        KafkaConnectorUtils.waitForConnectorNotReady(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        assertThat(KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME).get().getStatus().getConnectorStatus(), is(nullValue()));

        KafkaConnectorResource.replaceKafkaConnectorResource(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME,
            kc -> {
                kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME));
                kc.getSpec().setClassName(defaultClass);
            });

        KafkaConnectorUtils.waitForConnectorReady(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        assertKafkaConnectorStatus(3, "RUNNING|UNASSIGNED", "source", List.of(EXAMPLE_TOPIC_NAME));
    }

    @ParallelTest
    @Tag(CONNECTOR_OPERATOR)
    void testKafkaConnectorWithoutClusterConfig(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        // This test check NPE when connect cluster is not specified in labels
        // Check for NPE in CO logs is performed after every test in BaseST
        resourceManager.createResource(extensionContext, false, KafkaConnectorTemplates.kafkaConnector(clusterName, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, 2)
            .withNewMetadata()
                .withName(clusterName)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
            .endMetadata()
            .build());

        KafkaConnectorUtils.waitForConnectorNotReady(clusterName);

        KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(clusterName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        KafkaConnectorUtils.waitForConnectorDeletion(clusterName);
    }

    @ParallelTest
    void testKafkaTopicStatus(ExtensionContext extensionContext) {
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, topicName).build());

        KafkaTopicUtils.waitForKafkaTopicReady(topicName);
        assertKafkaTopicStatus(1, topicName);
    }

    @ParallelTest
    void testKafkaTopicStatusNotReady(ExtensionContext extensionContext) {
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, false, KafkaTopicTemplates.topic(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, topicName, 1, 10, 10).build());
        KafkaTopicUtils.waitForKafkaTopicNotReady(topicName);
        assertKafkaTopicStatus(1, topicName);

        cmdKubeClient().deleteByName(KafkaTopic.RESOURCE_KIND, topicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(topicName);
    }

    @ParallelTest
    void testKafkaStatusCertificate(ExtensionContext extensionContext) {
        String certs = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        String secretCerts = getKafkaSecretCertificates(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME + "-cluster-ca-cert", "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(secretCerts, is(certs));
    }

    @ParallelTest
    @Tag(MIRROR_MAKER2)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaMirrorMaker2Status(ExtensionContext extensionContext) {
        String targetClusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String mirrorMaker2Name = targetClusterName + "-mirror-maker-2";
        String mm2Url = KafkaMirrorMaker2Resources.url(mirrorMaker2Name, NAMESPACE, 8083);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(targetClusterName, 1, 1).build());
        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(mirrorMaker2Name, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, targetClusterName, 1, false).build());
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(mirrorMaker2Name);
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2ConnectorReadiness(NAMESPACE, mirrorMaker2Name);
        assertKafkaMirrorMaker2Status(1, mm2Url, mirrorMaker2Name);

        // Corrupt Mirror Maker pods
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2Resource(mirrorMaker2Name, mm2 -> mm2.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2NotReady(mirrorMaker2Name);
        // Restore Mirror Maker pod
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2Resource(mirrorMaker2Name, mm2 -> mm2.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100m"))
                .build()));
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(mirrorMaker2Name);
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2ConnectorReadiness(NAMESPACE, mirrorMaker2Name);
        assertKafkaMirrorMaker2Status(3, mm2Url, mirrorMaker2Name);
        // Wait for pods stability and check that pods weren't rolled
        PodUtils.verifyThatRunningPodsAreStable(NAMESPACE, KafkaMirrorMaker2Resources.deploymentName(mirrorMaker2Name));
        assertKafkaMirrorMaker2Status(3, mm2Url, mirrorMaker2Name);
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2ConnectorReadiness(NAMESPACE, mirrorMaker2Name);
    }

    @ParallelTest
    @Tag(MIRROR_MAKER2)
    void testKafkaMirrorMaker2WrongBootstrap(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String mirrorMaker2Name = clusterName + "-mirror-maker-2";

        KafkaMirrorMaker2 kafkaMirrorMaker2 = KafkaMirrorMaker2Templates.kafkaMirrorMaker2(mirrorMaker2Name,
            "non-existing-source", "non-existing-target", 1, false).build();

        resourceManager.createResource(extensionContext, false, kafkaMirrorMaker2);

        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2NotReady(mirrorMaker2Name);

        // delete
        KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(mirrorMaker2Name).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();

        DeploymentUtils.waitForDeploymentDeletion(KafkaMirrorMaker2Resources.deploymentName(mirrorMaker2Name));
    }

    @ParallelTest
    void testKafkaTopicDecreaseStatus(ExtensionContext extensionContext) throws InterruptedException {
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, topicName, 5).build());
        int decreaseTo = 1;

        LOGGER.info("Decreasing number of partitions to {}", decreaseTo);
        KafkaTopicResource.replaceTopicResource(topicName, kafkaTopic -> kafkaTopic.getSpec().setPartitions(decreaseTo));
        KafkaTopicUtils.waitForKafkaTopicPartitionChange(topicName, decreaseTo);
        KafkaTopicUtils.waitForKafkaTopicNotReady(topicName);

        assertKafkaTopicDecreasePartitionsStatus(topicName);

        // Wait some time to check if error is still present in KafkaTopic status
        LOGGER.info("Wait {} ms for next reconciliation", topicOperatorReconciliationInterval);
        Thread.sleep(topicOperatorReconciliationInterval);
        assertKafkaTopicDecreasePartitionsStatus(topicName);
    }

    @ParallelTest
    void testKafkaTopicChangingInSyncReplicasStatus(ExtensionContext extensionContext) throws InterruptedException {
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, topicName, 5).build());
        String invalidValue = "x";

        LOGGER.info("Changing min.insync.replicas to random char");
        KafkaTopicResource.replaceTopicResource(topicName, kafkaTopic -> kafkaTopic.getSpec().getConfig().replace("min.insync.replicas", invalidValue));
        KafkaTopicUtils.waitForKafkaTopicNotReady(topicName);

        assertKafkaTopicWrongMinInSyncReplicasStatus(topicName, invalidValue);

        // Wait some time to check if error is still present in KafkaTopic status
        LOGGER.info("Wait {} ms for next reconciliation", topicOperatorReconciliationInterval);
        Thread.sleep(topicOperatorReconciliationInterval);
        assertKafkaTopicWrongMinInSyncReplicasStatus(topicName, invalidValue);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(NAMESPACE)
            .withOperationTimeout(Constants.CO_OPERATION_TIMEOUT_SHORT)
            .createInstallation()
            .runInstallation();

        GenericKafkaListener plain = new GenericKafkaListenerBuilder()
                .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();
        GenericKafkaListener tls = new GenericKafkaListenerBuilder()
                .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                .withPort(9093)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .build();
        GenericKafkaListener nodePort = new GenericKafkaListenerBuilder()
                .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                .withPort(9094)
                .withType(KafkaListenerType.NODEPORT)
                .withTls(false)
                .build();

        List<GenericKafkaListener> listeners;
        if (Environment.isNamespaceRbacScope()) {
            listeners = asList(plain, tls);
        } else {
            listeners = asList(plain, tls, nodePort);
        }


        KafkaBuilder kafkaBuilder = KafkaTemplates.kafkaEphemeral(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .withListeners(listeners)
                .endKafka()
            .endSpec();

        String kafkaClientsName = NAMESPACE + "-shared-" + Constants.KAFKA_CLIENTS;

        resourceManager.createResource(extensionContext, kafkaBuilder.build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, TOPIC_NAME).build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());

        topicOperatorReconciliationInterval = KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME).get()
            .getSpec().getEntityOperator().getTopicOperator().getReconciliationIntervalSeconds() * 1_000 * 2 + 5_000;
    }

    void assertKafkaStatus(long expectedObservedGeneration, String internalAddress) {
        KafkaStatus kafkaStatus = KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME).get().getStatus();
        assertThat("Kafka cluster status has incorrect Observed Generation", kafkaStatus.getObservedGeneration(), is(expectedObservedGeneration));

        for (ListenerStatus listener : kafkaStatus.getListeners()) {
            switch (listener.getType()) {
                case Constants.TLS_LISTENER_DEFAULT_NAME:
                    assertThat("TLS bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(9093));
                    assertThat("TLS bootstrap has incorrect host", listener.getAddresses().get(0).getHost(), is(internalAddress));
                    break;
                case Constants.PLAIN_LISTENER_DEFAULT_NAME:
                    assertThat("Plain bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(9092));
                    assertThat("Plain bootstrap has incorrect host", listener.getAddresses().get(0).getHost(), is(internalAddress));
                    break;
                case Constants.EXTERNAL_LISTENER_DEFAULT_NAME:
                    Service extBootstrapService = kubeClient(NAMESPACE).getClient().services()
                            .inNamespace(NAMESPACE)
                            .withName(externalBootstrapServiceName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME))
                            .get();
                    assertThat("External bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(extBootstrapService.getSpec().getPorts().get(0).getNodePort()));
                    assertThat("External bootstrap has incorrect host", listener.getAddresses().get(0).getHost() != null);
                    break;
            }
        }
    }

    void assertKafkaMirrorMakerStatus(long expectedObservedGeneration, String mirrorMakerName) {
        KafkaMirrorMakerStatus kafkaMirrorMakerStatus = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(NAMESPACE).withName(mirrorMakerName).get().getStatus();
        assertThat("Kafka MirrorMaker cluster status has incorrect Observed Generation", kafkaMirrorMakerStatus.getObservedGeneration(), is(expectedObservedGeneration));
    }

    void assertKafkaMirrorMaker2Status(long expectedObservedGeneration, String apiUrl, String mirrorMaker2Name) {
        KafkaMirrorMaker2Status kafkaMirrorMaker2Status = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(mirrorMaker2Name).get().getStatus();
        assertThat("Kafka MirrorMaker2 cluster status has incorrect Observed Generation", kafkaMirrorMaker2Status.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("Kafka MirrorMaker2 cluster status has incorrect URL", kafkaMirrorMaker2Status.getUrl(), is(apiUrl));
        for (Map<String, Object> connector : kafkaMirrorMaker2Status.getConnectors()) {
            assertThat("One of the connectors is not RUNNING:\n" + connector.toString(), ((Map<String, String>) connector.get("connector")).get("state"), is("RUNNING"));
        }
    }

    void assertKafkaBridgeStatus(long expectedObservedGeneration, String bridgeAddress) {
        KafkaBridgeStatus kafkaBridgeStatus = KafkaBridgeResource.kafkaBridgeClient().inNamespace(NAMESPACE).withName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME).get().getStatus();
        assertThat("Kafka Bridge cluster status has incorrect Observed Generation", kafkaBridgeStatus.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("Kafka Bridge cluster status has incorrect URL", kafkaBridgeStatus.getUrl(), is(bridgeAddress));
    }

    void assertKafkaConnectStatus(long expectedObservedGeneration, String expectedUrl) {
        KafkaConnectStatus kafkaConnectStatus = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME).get().getStatus();
        assertThat("Kafka Connect cluster status has incorrect Observed Generation", kafkaConnectStatus.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("Kafka Connect cluster status has incorrect URL", kafkaConnectStatus.getUrl(), is(expectedUrl));

        validateConnectPlugins(kafkaConnectStatus.getConnectorPlugins());
    }

    void validateConnectPlugins(List<ConnectorPlugin> pluginsList) {
        assertThat(pluginsList, notNullValue());
        List<String> pluginsClasses = pluginsList.stream().map(p -> p.getConnectorClass()).collect(Collectors.toList());
        assertThat(pluginsClasses, hasItems("org.apache.kafka.connect.file.FileStreamSinkConnector",
                "org.apache.kafka.connect.file.FileStreamSourceConnector",
                "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
                "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
                "org.apache.kafka.connect.mirror.MirrorSourceConnector"));
    }

    @SuppressWarnings("unchecked")
    void assertKafkaConnectorStatus(long expectedObservedGeneration, String connectorStates, String type, List<String> topics) {
        KafkaConnectorStatus kafkaConnectorStatus = KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME).get().getStatus();
        assertThat(kafkaConnectorStatus.getObservedGeneration(), is(expectedObservedGeneration));
        Map<String, Object> connectorStatus = kafkaConnectorStatus.getConnectorStatus();
        String currentState = ((LinkedHashMap<String, String>) connectorStatus.get("connector")).get("state");
        assertThat(connectorStates, containsString(currentState));
        assertThat(connectorStatus.get("name"), is(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME));
        assertThat(connectorStatus.get("type"), is(type));
        assertThat(connectorStatus.get("tasks"), notNullValue());
        assertThat(kafkaConnectorStatus.getTopics(), is(topics));
    }

    void assertKafkaTopicStatus(long expectedObservedGeneration, String topicName) {
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get().getStatus();
        assertThat("KafkaTopic status has incorrect Observed Generation", kafkaTopicStatus.getObservedGeneration(), is(expectedObservedGeneration));
    }

    void assertKafkaTopicDecreasePartitionsStatus(String topicName) {
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get().getStatus();

        assertThat(kafkaTopicStatus.getConditions().stream()
            .anyMatch(condition -> condition.getType().equals(NotReady.toString())), is(true));
        assertThat(kafkaTopicStatus.getConditions().stream()
            .anyMatch(condition -> condition.getReason().equals("PartitionDecreaseException")), is(true));
        assertThat(kafkaTopicStatus.getConditions().stream()
            .anyMatch(condition -> condition.getMessage().contains("Number of partitions cannot be decreased")), is(true));
    }

    void assertKafkaTopicWrongMinInSyncReplicasStatus(String topicName, String invalidValue) {
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get().getStatus();

        assertThat(kafkaTopicStatus.getConditions().stream()
            .anyMatch(condition -> condition.getType().equals(NotReady.toString())), is(true));
        assertThat(kafkaTopicStatus.getConditions().stream()
            .anyMatch(condition -> condition.getReason().equals("InvalidRequestException")), is(true));
        assertThat(kafkaTopicStatus.getConditions().stream()
            .anyMatch(condition -> condition.getMessage().contains(String.format("Invalid value %s for configuration min.insync.replicas", invalidValue))), is(true));
    }
}
