/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.mirrormaker;

import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaMirrorMaker2Status;
import io.strimzi.api.kafka.model.template.DeploymentStrategy;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.SetupClusterOperator;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicExampleClients;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMaker2Utils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.GLOBAL_POLL_INTERVAL;
import static io.strimzi.systemtest.Constants.GLOBAL_TIMEOUT;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.SCALABILITY;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@Tag(REGRESSION)
@Tag(MIRROR_MAKER2)
@Tag(CONNECT_COMPONENTS)
@Tag(INTERNAL_CLIENTS_USED)
class MirrorMaker2ST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MirrorMaker2ST.class);
    public static final String NAMESPACE = "mirrormaker2-cluster-test";

    private static final String MIRRORMAKER2_TOPIC_NAME = "mirrormaker2-topic-example";
    private final int messagesCount = 200;

    @SuppressWarnings({"checkstyle:MethodLength"})
    @ParallelNamespaceTest
    void testMirrorMaker2(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        final String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClusterSourceName = clusterName + "-source";
        String kafkaClusterTargetName = clusterName + "-target";
        String sourceTopicName = "availability-topic-source-" + mapWithTestTopics.get(extensionContext.getDisplayName());
        String targetTopicName = "availability-topic-target-" + mapWithTestTopics.get(extensionContext.getDisplayName());

        Map<String, Object> expectedConfig = StUtils.loadProperties("group.id=mirrormaker2-cluster\n" +
                "key.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n" +
                "value.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n" +
                "header.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n" +
                "config.storage.topic=mirrormaker2-cluster-configs\n" +
                "status.storage.topic=mirrormaker2-cluster-status\n" +
                "offset.storage.topic=mirrormaker2-cluster-offsets\n" +
                "config.storage.replication.factor=1\n" +
                "status.storage.replication.factor=1\n" +
                "offset.storage.replication.factor=1\n" + 
                "config.providers=file\n" + 
                "config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider\n");

        String topicSourceName = MIRRORMAKER2_TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);
        String topicTargetName = kafkaClusterSourceName + "." + topicSourceName;
        String topicSourceNameMirrored = kafkaClusterSourceName + "." + sourceTopicName;

        // Deploy source kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());
        // Deploy Topic
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicSourceName, 3).build());

        resourceManager.createResource(extensionContext, false, KafkaClientsTemplates.kafkaClients(namespaceName, false, kafkaClientsName).build());

        final String kafkaClientsPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(namespaceName, kafkaClientsName).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(sourceTopicName)
            .withNamespaceName(namespaceName)
            .withClusterName(kafkaClusterSourceName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        // Check brokers availability
        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            sourceTopicName, kafkaClusterSourceName, MESSAGE_COUNT);

        internalKafkaClient.produceAndConsumesPlainMessagesUntilBothOperationsAreSuccessful();

        LOGGER.info("Setting topic to {}, cluster to {} and changing consumer group",
            targetTopicName, kafkaClusterTargetName);

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(targetTopicName)
            .withClusterName(kafkaClusterTargetName)
            .build();

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            targetTopicName, kafkaClusterTargetName, MESSAGE_COUNT);

        internalKafkaClient.produceAndConsumesPlainMessagesUntilBothOperationsAreSuccessful();

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
            .editSpec()
                .editFirstMirror()
                    .editSourceConnector()
                        .addToConfig("refresh.topics.interval.seconds", "60")
                    .endSourceConnector()
                .endMirror()
            .endSpec()
            .build());

        LOGGER.info("Looks like the mirrormaker2 cluster my-cluster deployed OK");

        String podName = PodUtils.getPodNameByPrefix(namespaceName, KafkaMirrorMaker2Resources.deploymentName(clusterName));
        String kafkaPodJson = TestUtils.toJsonString(kubeClient(namespaceName).getPod(namespaceName, podName));

        assertThat(kafkaPodJson, hasJsonPath(StUtils.globalVariableJsonPathBuilder(0, "KAFKA_CONNECT_BOOTSTRAP_SERVERS"),
                hasItem(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))));
        assertThat(StUtils.getPropertiesFromJson(0, kafkaPodJson, "KAFKA_CONNECT_CONFIGURATION"), is(expectedConfig));
        testDockerImagesForKafkaMirrorMaker2(clusterName, NAMESPACE, namespaceName);

        verifyLabelsOnPods(namespaceName, clusterName, "mirrormaker2", "KafkaMirrorMaker2");
        verifyLabelsForService(namespaceName, clusterName, "mirrormaker2-api", "KafkaMirrorMaker2");
        verifyLabelsForConfigMaps(namespaceName, kafkaClusterSourceName, null, kafkaClusterTargetName);
        verifyLabelsForServiceAccounts(namespaceName, kafkaClusterSourceName, null);

        LOGGER.info("Setting topic to {}, cluster to {} and changing consumer group",
            topicSourceName, kafkaClusterSourceName);

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicSourceName)
            .withClusterName(kafkaClusterSourceName)
            .build();

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            topicSourceName, kafkaClusterSourceName, MESSAGE_COUNT);
        int sent = internalKafkaClient.sendMessagesPlain();

        LOGGER.info("Consumer in source cluster and topic should receive {} messages", MESSAGE_COUNT);

        internalKafkaClient.consumesPlainMessagesUntilOperationIsSuccessful(sent);

        LOGGER.info("Now setting topic to {} and cluster to {} - the messages should be mirrored",
            topicTargetName, kafkaClusterTargetName);

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicTargetName)
            .withClusterName(kafkaClusterTargetName)
            .build();

        LOGGER.info("Consumer in target cluster and topic should receive {} messages", MESSAGE_COUNT);

        internalKafkaClient.consumesPlainMessagesUntilOperationIsSuccessful(sent);


        LOGGER.info("Changing topic to {}", topicSourceNameMirrored);

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicSourceNameMirrored)
            .build();

        LOGGER.info("Check if mm2 mirror automatically created topic");
        internalKafkaClient.consumesPlainMessagesUntilOperationIsSuccessful(sent);

        LOGGER.info("Mirrored successful");

        KafkaTopic mirroredTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicTargetName).get();
        assertThat(mirroredTopic.getSpec().getPartitions(), is(3));
        assertThat(mirroredTopic.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(kafkaClusterTargetName));

        // Replace source topic resource with new data and check that mm2 update target topi
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicSourceName, kt -> kt.getSpec().setPartitions(8), namespaceName);
        KafkaTopicUtils.waitForKafkaTopicPartitionChange(namespaceName, topicTargetName, 8);
    }

    /**
     * Test mirroring messages by MirrorMaker 2.0 over tls transport using mutual tls auth
     */
    @SuppressWarnings({"checkstyle:MethodLength"})
    @ParallelNamespaceTest
    @Tag(ACCEPTANCE)
    void testMirrorMaker2TlsAndTlsClientAuth(ExtensionContext extensionContext) throws Exception {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        final String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClusterSourceName = clusterName + "-source";
        String kafkaClusterTargetName = clusterName + "-target";
        String topicName = "availability-topic-source-" + mapWithTestTopics.get(extensionContext.getDisplayName());
        String topicSourceNameMirrored = kafkaClusterSourceName + "." + topicName;
        String topicSourceName = MIRRORMAKER2_TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);
        String topicTargetName = kafkaClusterSourceName + "." + topicSourceName;
        String kafkaUserSourceName = clusterName + "-my-user-source";
        String kafkaUserTargetName = clusterName + "-my-user-target";

        // Deploy source kafka with tls listener and mutual tls auth
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        // Deploy target kafka with tls listener and mutual tls auth
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        // Deploy topic
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicSourceName, 3).build());
        // Create Kafka user
        KafkaUser userSource = KafkaUserTemplates.tlsUser(kafkaClusterSourceName, kafkaUserSourceName).build();
        KafkaUser userTarget = KafkaUserTemplates.tlsUser(kafkaClusterTargetName, kafkaUserTargetName).build();

        resourceManager.createResource(extensionContext, userSource);
        resourceManager.createResource(extensionContext, userTarget);
        resourceManager.createResource(extensionContext, false, KafkaClientsTemplates.kafkaClients(namespaceName, true, kafkaClientsName, userSource, userTarget).build());

        final String kafkaClientsPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(namespaceName, kafkaClientsName).get(0).getMetadata().getName();

        String baseTopic = mapWithTestTopics.get(extensionContext.getDisplayName());
        String topicTestName1 = baseTopic + "-test-1";
        String topicTestName2 = baseTopic + "-test-2";

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicTestName1).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterTargetName, topicTestName2).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicTestName1)
            .withNamespaceName(namespaceName)
            .withClusterName(kafkaClusterSourceName)
            .withKafkaUsername(userSource.getMetadata().getName())
            .withMessageCount(messagesCount)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        // Check brokers availability
        ClientUtils.waitUntilProducerAndConsumerSuccessfullySendAndReceiveMessages(extensionContext, internalKafkaClient);

        LOGGER.info("Setting topic to {}, cluster to {} and changing user to {}",
            topicTestName2, kafkaClusterTargetName, userTarget.getMetadata().getName());

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withClusterName(kafkaClusterTargetName)
            .withTopicName(topicTestName2)
            .withKafkaUsername(userTarget.getMetadata().getName())
            .build();

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            topicTestName2, kafkaClusterTargetName, messagesCount);
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        // Initialize CertSecretSource with certificate and secret names for source
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterSourceName));

        // Initialize CertSecretSource with certificate and secret names for target
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterTargetName));

        // Deploy Mirror Maker 2.0 with tls listener and mutual tls auth
        KafkaMirrorMaker2ClusterSpec sourceClusterWithTlsAuth = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias(kafkaClusterSourceName)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaClusterSourceName))
                .withNewKafkaClientAuthenticationTls()
                    .withNewCertificateAndKey()
                        .withSecretName(kafkaUserSourceName)
                        .withCertificate("user.crt")
                        .withKey("user.key")
                    .endCertificateAndKey()
                .endKafkaClientAuthenticationTls()
                .withNewTls()
                    .withTrustedCertificates(certSecretSource)
                .endTls()
                .build();

        KafkaMirrorMaker2ClusterSpec targetClusterWithTlsAuth = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias(kafkaClusterTargetName)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaClusterTargetName))
                .withNewKafkaClientAuthenticationTls()
                    .withNewCertificateAndKey()
                        .withSecretName(kafkaUserTargetName)
                        .withCertificate("user.crt")
                        .withKey("user.key")
                    .endCertificateAndKey()
                .endKafkaClientAuthenticationTls()
                .withNewTls()
                    .withTrustedCertificates(certSecretTarget)
                .endTls()
                .addToConfig("config.storage.replication.factor", 1)
                .addToConfig("offset.storage.replication.factor", 1)
                .addToConfig("status.storage.replication.factor", 1)
                .build();

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 1, true)
            .editSpec()
                .withClusters(sourceClusterWithTlsAuth, targetClusterWithTlsAuth)
                .editFirstMirror()
                    .withNewTopicsPattern(MIRRORMAKER2_TOPIC_NAME + ".*")
                .endMirror()
            .endSpec()
            .build());

        LOGGER.info("Setting topic to {}, cluster to {} and changing user to {}",
            topicSourceName, kafkaClusterSourceName, userSource.getMetadata().getName());

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicSourceName)
            .withClusterName(kafkaClusterSourceName)
            .withKafkaUsername(userSource.getMetadata().getName())
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            topicSourceName, kafkaClusterSourceName, messagesCount);
        int sent = internalKafkaClient.sendMessagesTls();

        LOGGER.info("Receiving messages from - topic {}, cluster {} and message count of {}",
            topicSourceName, kafkaClusterSourceName, messagesCount);
        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesTls()
        );

        LOGGER.info("Now setting topic to {}, cluster to {} and user to {} - the messages should be mirrored",
            topicTargetName, kafkaClusterTargetName, userTarget.getMetadata().getName());

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicTargetName)
            .withClusterName(kafkaClusterTargetName)
            .withKafkaUsername(userTarget.getMetadata().getName())
            .build();

        LOGGER.info("Consumer in target cluster and topic should receive {} messages", messagesCount);
        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesTls()
        );
        LOGGER.info("Messages successfully mirrored");

        KafkaTopic mirroredTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicTargetName).get();
        assertThat(mirroredTopic.getSpec().getPartitions(), is(3));
        assertThat(mirroredTopic.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(kafkaClusterTargetName));

        mirroredTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicSourceNameMirrored).get();
        assertThat(mirroredTopic, nullValue());
    }

    /**
     * Test mirroring messages by MirrorMaker 2.0 over tls transport using scram-sha-512 auth
     */
    @SuppressWarnings({"checkstyle:MethodLength"})
    @ParallelNamespaceTest
    void testMirrorMaker2TlsAndScramSha512Auth(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        final String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClusterSourceName = clusterName + "-source";
        String kafkaClusterTargetName = clusterName + "-target";
        String sourceTopicName = "availability-topic-source-" + mapWithTestTopics.get(extensionContext.getDisplayName());
        String targetTopicName = "availability-topic-target-" + mapWithTestTopics.get(extensionContext.getDisplayName());
        String topicSourceNameMirrored = kafkaClusterSourceName + "." + sourceTopicName;
        String topicSourceName = MIRRORMAKER2_TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);
        String topicTargetName = kafkaClusterSourceName + "." + topicSourceName;
        String kafkaUserSource = clusterName + "-my-user-source";
        String kafkaUserTarget = clusterName + "-my-user-target";

        // Deploy source kafka with tls listener and SCRAM-SHA authentication
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        // Deploy target kafka with tls listener and SCRAM-SHA authentication
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        // Deploy topic
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicSourceName, 3).build());

        // Create Kafka user for source cluster
        KafkaUser userSource = KafkaUserTemplates.scramShaUser(kafkaClusterSourceName, kafkaUserSource).build();
        resourceManager.createResource(extensionContext, userSource);

        // Create Kafka user for target cluster
        KafkaUser userTarget = KafkaUserTemplates.scramShaUser(kafkaClusterTargetName, kafkaUserTarget).build();
        resourceManager.createResource(extensionContext, userTarget);

        // Initialize PasswordSecretSource to set this as PasswordSecret in MirrorMaker2 spec
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName(kafkaUserSource);
        passwordSecretSource.setPassword("password");

        // Initialize PasswordSecretSource to set this as PasswordSecret in MirrorMaker2 spec
        PasswordSecretSource passwordSecretTarget = new PasswordSecretSource();
        passwordSecretTarget.setSecretName(kafkaUserTarget);
        passwordSecretTarget.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for source
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterSourceName));

        // Initialize CertSecretSource with certificate and secret names for target
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterTargetName));

        // Deploy client
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(namespaceName, true, kafkaClientsName, userSource, userTarget).build());

        final String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(sourceTopicName)
            .withNamespaceName(namespaceName)
            .withClusterName(kafkaClusterSourceName)
            .withKafkaUsername(userSource.getMetadata().getName())
            .withMessageCount(messagesCount)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            sourceTopicName, kafkaClusterSourceName, messagesCount);
        // Check brokers availability
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        LOGGER.info("Setting topic to {}, cluster to {} and changing user to {}",
            targetTopicName, kafkaClusterTargetName, userTarget.getMetadata().getName());

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(targetTopicName)
            .withClusterName(kafkaClusterTargetName)
            .withKafkaUsername(userTarget.getMetadata().getName())
            .build();

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            targetTopicName, kafkaClusterTargetName, messagesCount);
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        // Deploy Mirror Maker with TLS and ScramSha512
        KafkaMirrorMaker2ClusterSpec sourceClusterWithScramSha512Auth = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias(kafkaClusterSourceName)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaClusterSourceName))
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername(kafkaUserSource)
                    .withPasswordSecret(passwordSecretSource)
                .endKafkaClientAuthenticationScramSha512()
                .withNewTls()
                    .withTrustedCertificates(certSecretSource)
                .endTls()
                .build();

        KafkaMirrorMaker2ClusterSpec targetClusterWithScramSha512Auth = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias(kafkaClusterTargetName)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaClusterTargetName))
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername(kafkaUserTarget)
                    .withPasswordSecret(passwordSecretTarget)
                .endKafkaClientAuthenticationScramSha512()
                .withNewTls()
                    .withTrustedCertificates(certSecretTarget)
                .endTls()
                .addToConfig("config.storage.replication.factor", 1)
                .addToConfig("offset.storage.replication.factor", 1)
                .addToConfig("status.storage.replication.factor", 1)
                .build();

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 1, true)
            .editSpec()
                .withClusters(targetClusterWithScramSha512Auth, sourceClusterWithScramSha512Auth)
                .editFirstMirror()
                    .withTopicsExcludePattern("availability.*")
                .endMirror()
            .endSpec()
            .build());

        LOGGER.info("Setting topic to {}, cluster to {} and changing user to {}",
            topicSourceName, kafkaClusterSourceName, userSource.getMetadata().getName());

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicSourceName)
            .withClusterName(kafkaClusterSourceName)
            .withKafkaUsername(userSource.getMetadata().getName())
            .build();

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            topicSourceName, kafkaClusterSourceName, messagesCount);
        int sent = internalKafkaClient.sendMessagesTls();

        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesTls()
        );

        LOGGER.info("Changing to target - topic {}, cluster {}, user {}", topicTargetName, kafkaClusterTargetName, userTarget.getMetadata().getName());

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicTargetName)
            .withClusterName(kafkaClusterTargetName)
            .withKafkaUsername(userTarget.getMetadata().getName())
            .build();

        LOGGER.info("Now messages should be mirrored to target topic and cluster");
        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesTls()
        );
        LOGGER.info("Messages successfully mirrored");

        KafkaTopic mirroredTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicTargetName).get();
        assertThat(mirroredTopic.getSpec().getPartitions(), is(3));
        assertThat(mirroredTopic.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(kafkaClusterTargetName));

        mirroredTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicSourceNameMirrored).get();
        assertThat(mirroredTopic, nullValue());
    }

    private void testDockerImagesForKafkaMirrorMaker2(String clusterName, String clusterOperatorNamespace, String mirrorMakerNamespace) {
        LOGGER.info("Verifying docker image names");
        // we must use NAMESPACE because there is CO deployed
        Map<String, String> imgFromDeplConf = getImagesFromConfig(clusterOperatorNamespace);
        //Verifying docker image for kafka mirrormaker2
        String mirrormaker2ImageName = PodUtils.getFirstContainerImageNameFromPod(mirrorMakerNamespace, kubeClient(mirrorMakerNamespace).listPods(mirrorMakerNamespace, clusterName, Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND)
               .get(0).getMetadata().getName());

        String mirrormaker2Version = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(mirrorMakerNamespace).withName(clusterName).get().getSpec().getVersion();
        if (mirrormaker2Version == null) {
            mirrormaker2Version = Environment.ST_KAFKA_VERSION;
        }

        assertThat(TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_MIRROR_MAKER_2_IMAGE_MAP)).get(mirrormaker2Version), is(mirrormaker2ImageName));
        LOGGER.info("Docker images verified");
    }

    @ParallelNamespaceTest
    @Tag(SCALABILITY)
    void testScaleMirrorMaker2Subresource(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClusterSourceName = clusterName + "-source";
        String kafkaClusterTargetName = clusterName + "-target";

        // Deploy source kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 1, false).build());

        int scaleTo = 4;
        long mm2ObsGen = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get().getStatus().getObservedGeneration();
        String mm2GenName = kubeClient(namespaceName).listPods(namespaceName, clusterName, Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling KafkaMirrorMaker2 subresource <-------");
        LOGGER.info("Scaling subresource replicas to {}", scaleTo);
        cmdKubeClient(namespaceName).scaleByName(KafkaMirrorMaker2.RESOURCE_KIND, clusterName, scaleTo);
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, KafkaMirrorMaker2Resources.deploymentName(clusterName), scaleTo);

        LOGGER.info("Check if replicas is set to {}, naming prefix should be same and observed generation higher", scaleTo);
        List<String> mm2Pods = kubeClient(namespaceName).listPodNames(namespaceName, clusterName, Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND);

        assertThat(mm2Pods.size(), is(4));
        assertThat(KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get().getSpec().getReplicas(), is(4));
        assertThat(KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get().getStatus().getReplicas(), is(4));
        /*
        observed generation should be higher than before scaling -> after change of spec and successful reconciliation,
        the observed generation is increased
        */
        assertThat(mm2ObsGen < KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get().getStatus().getObservedGeneration(), is(true));
        for (String pod : mm2Pods) {
            assertThat(pod.contains(mm2GenName), is(true));
        }
    }

    @ParallelNamespaceTest
    void testMirrorMaker2CorrectlyMirrorsHeaders(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClusterSourceName = clusterName + "-source";
        String kafkaClusterTargetName = clusterName + "-target";
        String sourceProducerName = clusterName + "-source-producer";
        String targetConsumerName = clusterName + "-target-consumer";
        String sourceExampleTopic = clusterName + "-source-example-topic";
        String targetExampleTopic = kafkaClusterSourceName + "." + sourceExampleTopic;

        // Deploy source kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());
// Deploy Topic for example clients
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, sourceExampleTopic).build());

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 1, false).build());

        //deploying example clients for checking if mm2 will mirror messages with headers

        KafkaBasicExampleClients targetKafkaClientsJob = new KafkaBasicExampleClients.Builder()
            .withConsumerName(targetConsumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .withTopicName(targetExampleTopic)
            .withMessageCount(MESSAGE_COUNT)
            .withDelayMs(1000)
            .build();

        resourceManager.createResource(extensionContext, targetKafkaClientsJob.consumerStrimzi().build());

        KafkaBasicExampleClients sourceKafkaClientsJob = new KafkaBasicExampleClients.Builder()
            .withProducerName(sourceProducerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .withTopicName(sourceExampleTopic)
            .withMessageCount(MESSAGE_COUNT)
            .withDelayMs(1000)
            .build();

        resourceManager.createResource(extensionContext, sourceKafkaClientsJob.producerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editContainer(0)
                            .addNewEnv()
                                .withName("HEADERS")
                                .withValue("header_key_one=header_value_one, header_key_two=header_value_two")
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build());

        ClientUtils.waitTillContinuousClientsFinish(sourceProducerName, targetConsumerName, namespaceName, MESSAGE_COUNT);

        LOGGER.info("Checking log of {} job if the headers are correct", targetConsumerName);
        String header1 = "key: header_key_one, value: header_value_one";
        String header2 = "key: header_key_two, value: header_value_two";
        String log = StUtils.getLogFromPodByTime(namespaceName, kubeClient(namespaceName).listPodsByPrefixInName(targetConsumerName).get(0).getMetadata().getName(), "", MESSAGE_COUNT + "s");
        assertThat(log, containsString(header1));
        assertThat(log, containsString(header2));
    }

    @ParallelNamespaceTest
    @Tag(SCALABILITY)
    void testScaleMirrorMaker2ToZero(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClusterSourceName = clusterName + "-source";
        String kafkaClusterTargetName = clusterName + "-target";

        // Deploy source kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 3, false).build());

        long oldObsGen = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get().getStatus().getObservedGeneration();
        String mm2DepName = KafkaMirrorMaker2Resources.deploymentName(clusterName);
        List<String> mm2Pods = kubeClient(namespaceName).listPodNames(clusterName, Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND);
        assertThat(mm2Pods.size(), is(3));

        LOGGER.info("Scaling MirrorMaker2 to zero");
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(clusterName, mm2 -> mm2.getSpec().setReplicas(0), namespaceName);

        PodUtils.waitForPodsReady(namespaceName, kubeClient(namespaceName).getDeploymentSelectors(mm2DepName), 0, true, () -> { });

        mm2Pods = kubeClient().listPodNames(clusterName, Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND);
        KafkaMirrorMaker2Status mm2Status = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get().getStatus();
        long actualObsGen = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get().getMetadata().getGeneration();

        assertThat(mm2Pods.size(), is(0));
        assertThat(mm2Status.getConditions().get(0).getType(), is(Ready.toString()));
        assertThat(actualObsGen, is(not(oldObsGen)));

        TestUtils.waitFor("Until mirror maker 2 status url is null", GLOBAL_POLL_INTERVAL, GLOBAL_TIMEOUT, () -> {
            KafkaMirrorMaker2Status mm2StatusUrl = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get().getStatus();
            return mm2StatusUrl.getUrl() == null;
        });
    }

    @ParallelNamespaceTest
    void testIdentityReplicationPolicy(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClusterSourceName = clusterName + "-source";
        String kafkaClusterTargetName = clusterName + "-target";
        String originalTopicName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        // Deploy source kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());
        // Create topic
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, originalTopicName, 3).build());

        resourceManager.createResource(extensionContext, false, KafkaClientsTemplates.kafkaClients(namespaceName, false, kafkaClientsName).build());

        final String kafkaClientsPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(namespaceName, kafkaClientsName).get(0).getMetadata().getName();

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
            .editSpec()
                .editMirror(0)
                    .editSourceConnector()
                        .addToConfig("replication.policy.class", "io.strimzi.kafka.connect.mirror.IdentityReplicationPolicy")
                    .endSourceConnector()
                .endMirror()
            .endSpec()
            .build());

        LOGGER.info("Sending and receiving messages via {}", kafkaClusterSourceName);
        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withNamespaceName(namespaceName)
            .withTopicName(originalTopicName)
            .withClusterName(kafkaClusterSourceName)
            .withMessageCount(MESSAGE_COUNT)
            .withUsingPodName(kafkaClientsPodName)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.assertSentAndReceivedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        LOGGER.info("Changing to {} and will try to receive messages", kafkaClusterTargetName);

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withClusterName(kafkaClusterTargetName)
            .build();

        assertThat(internalKafkaClient.receiveMessagesPlain(), equalTo(MESSAGE_COUNT));

        LOGGER.info("Checking if the mirrored topic name is same as the original one");

        List<String> kafkaTopics = KafkaCmdClient.listTopicsUsingPodCli(namespaceName, kafkaClusterTargetName, 0);
        assertNotNull(kafkaTopics.stream().filter(kafkaTopic -> kafkaTopic.equals(originalTopicName)).findAny());

        List<String> kafkaTopicSpec = KafkaCmdClient.describeTopicUsingPodCli(namespaceName, kafkaClusterTargetName, 0, originalTopicName);
        assertThat(kafkaTopicSpec.stream().filter(token -> token.startsWith("Topic:")).findFirst().orElse(null), equalTo("Topic:" + originalTopicName));
        assertThat(kafkaTopicSpec.stream().filter(token -> token.startsWith("PartitionCount:")).findFirst().orElse(null), equalTo("PartitionCount:3"));
    }

    @ParallelNamespaceTest
    void testConfigureDeploymentStrategy(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClusterSourceName = clusterName + "-source";
        String kafkaClusterTargetName = clusterName + "-target";

        // Deploy source kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
            .editSpec()
                .editOrNewTemplate()
                    .editOrNewDeployment()
                        .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                    .endDeployment()
                .endTemplate()
            .endSpec()
            .build());

        String mm2DepName = KafkaMirrorMaker2Resources.deploymentName(clusterName);

        LOGGER.info("Adding label to MirrorMaker2 resource, the CR should be recreated");
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(clusterName,
            mm2 -> mm2.getMetadata().setLabels(Collections.singletonMap("some", "label")), namespaceName);
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, mm2DepName, 1);

        KafkaMirrorMaker2 kmm2 = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get();

        LOGGER.info("Checking that observed gen. is still on 1 (recreation) and new label is present");
        assertThat(kmm2.getStatus().getObservedGeneration(), is(1L));
        assertThat(kmm2.getMetadata().getLabels().toString(), containsString("some=label"));
        assertThat(kmm2.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.RECREATE));

        LOGGER.info("Changing deployment strategy to {}", DeploymentStrategy.ROLLING_UPDATE);
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(clusterName,
            mm2 -> mm2.getSpec().getTemplate().getDeployment().setDeploymentStrategy(DeploymentStrategy.ROLLING_UPDATE), namespaceName);
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(namespaceName, clusterName);

        LOGGER.info("Adding another label to MirrorMaker2 resource, pods should be rolled");
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(clusterName, mm2 -> mm2.getMetadata().getLabels().put("another", "label"), namespaceName);
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, mm2DepName, 1);

        LOGGER.info("Checking that observed gen. higher (rolling update) and label is changed");
        kmm2 = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(namespaceName).withName(clusterName).get();
        assertThat(kmm2.getStatus().getObservedGeneration(), is(2L));
        assertThat(kmm2.getMetadata().getLabels().toString(), containsString("another=label"));
        assertThat(kmm2.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.ROLLING_UPDATE));
    }

    @ParallelNamespaceTest
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testRestoreOffsetsInConsumerGroup(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String kafkaClusterSourceName = clusterName + "-source";
        final String kafkaClusterTargetName = clusterName + "-target";
        final String syncGroupOffsetsIntervalSeconds = "1";
        final String topicSourceNameMirrored = "test-sync-offset-" + new Random().nextInt(Integer.MAX_VALUE);
        final String topicTargetNameMirrored = kafkaClusterSourceName + "." + topicSourceNameMirrored;
        final String consumerGroup = "mm2-test-consumer-group";
        final String sourceProducerName = "mm2-producer-source-" + ClientUtils.generateRandomConsumerGroup();
        final String sourceConsumerName = "mm2-consumer-source-" + ClientUtils.generateRandomConsumerGroup();
        final String targetProducerName = "mm2-producer-target-" + ClientUtils.generateRandomConsumerGroup();
        final String targetConsumerName = "mm2-consumer-target-" + ClientUtils.generateRandomConsumerGroup();
        final String mm2SrcTrgName = clusterName + "-src-trg";
        final String mm2TrgSrcName = clusterName + "-trg-src";

        resourceManager.createResource(extensionContext, false,
            // Deploy source kafka
            KafkaTemplates.kafkaPersistent(kafkaClusterSourceName, 1, 1).build(),
            // Deploy target kafka
            KafkaTemplates.kafkaPersistent(kafkaClusterTargetName, 1, 1).build()
        );

        // Wait for Kafka clusters readiness
        KafkaUtils.waitForKafkaReady(namespaceName, kafkaClusterSourceName);
        KafkaUtils.waitForKafkaReady(namespaceName, kafkaClusterTargetName);

        resourceManager.createResource(extensionContext,
            // MM2 Active (S) <-> Active (T) // direction S -> T mirroring
            // *.replication.factor(s) to 1 are added just to speed up test by using only 1 ZK and 1 Kafka
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2(mm2TrgSrcName, kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
                .editSpec()
                .editFirstMirror()
                    .editSourceConnector()
                        .addToConfig("refresh.topics.interval.seconds", "1")
                        .addToConfig("replication.factor", "1")
                        .addToConfig("offset-syncs.topic.replication.factor", "1")
                    .endSourceConnector()
                    .editCheckpointConnector()
                        .addToConfig("refresh.groups.interval.seconds", "1")
                        .addToConfig("sync.group.offsets.enabled", "true")
                        .addToConfig("sync.group.offsets.interval.seconds", syncGroupOffsetsIntervalSeconds)
                        .addToConfig("emit.checkpoints.enabled", "true")
                        .addToConfig("emit.checkpoints.interval.seconds", "1")
                        .addToConfig("checkpoints.topic.replication.factor", "1")
                    .endCheckpointConnector()
                    .editHeartbeatConnector()
                        .addToConfig("heartbeats.topic.replication.factor", "1")
                    .endHeartbeatConnector()
                    .withTopicsPattern(".*")
                    .withGroupsPattern(".*")
                .endMirror()
            .endSpec().build(),
            // MM2 Active (S) <-> Active (T) // direction S <- T mirroring
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2(mm2SrcTrgName, kafkaClusterSourceName, kafkaClusterTargetName, 1, false)
                .editSpec()
                .editFirstMirror()
                    .editSourceConnector()
                        .addToConfig("refresh.topics.interval.seconds", "1")
                        .addToConfig("replication.factor", "1")
                        .addToConfig("offset-syncs.topic.replication.factor", "1")
                    .endSourceConnector()
                    .editCheckpointConnector()
                        .addToConfig("refresh.groups.interval.seconds", "1")
                        .addToConfig("sync.group.offsets.enabled", "true")
                        .addToConfig("sync.group.offsets.interval.seconds", syncGroupOffsetsIntervalSeconds)
                        .addToConfig("emit.checkpoints.enabled", "true")
                        .addToConfig("emit.checkpoints.interval.seconds", "1")
                        .addToConfig("checkpoints.topic.replication.factor", "1")
                    .endCheckpointConnector()
                    .editHeartbeatConnector()
                        .addToConfig("heartbeats.topic.replication.factor", "1")
                    .endHeartbeatConnector()
                    .withTopicsPattern(".*")
                    .withGroupsPattern(".*")
                .endMirror()
            .endSpec().build(),
            // deploy topic
            KafkaTopicTemplates.topic(kafkaClusterSourceName, topicSourceNameMirrored, 3).build());

        KafkaBasicExampleClients initialInternalClientSourceJob = new KafkaBasicExampleClients.Builder()
                .withProducerName(sourceProducerName)
                .withConsumerName(sourceConsumerName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
                .withTopicName(topicSourceNameMirrored)
                .withMessageCount(MESSAGE_COUNT)
                .withMessage("Producer A")
                .withConsumerGroup(consumerGroup)
                .withNamespaceName(namespaceName)
                .build();

        KafkaBasicExampleClients initialInternalClientTargetJob = new KafkaBasicExampleClients.Builder()
                .withProducerName(targetProducerName)
                .withConsumerName(targetConsumerName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
                .withTopicName(topicTargetNameMirrored)
                .withMessageCount(MESSAGE_COUNT)
                .withConsumerGroup(consumerGroup)
                .withNamespaceName(namespaceName)
                .build();

        LOGGER.info("Send & receive {} messages to/from Source cluster.", MESSAGE_COUNT);
        resourceManager.createResource(extensionContext,
            initialInternalClientSourceJob.producerStrimzi().build(),
            initialInternalClientSourceJob.consumerStrimzi().build());

        ClientUtils.waitForClientSuccess(sourceProducerName, namespaceName, MESSAGE_COUNT);
        ClientUtils.waitForClientSuccess(sourceConsumerName, namespaceName, MESSAGE_COUNT);

        JobUtils.deleteJobWithWait(namespaceName, sourceProducerName);
        JobUtils.deleteJobWithWait(namespaceName, sourceConsumerName);

        LOGGER.info("Send {} messages to Source cluster.", MESSAGE_COUNT);
        KafkaBasicExampleClients internalClientSourceJob = initialInternalClientSourceJob.toBuilder().withMessage("Producer B").build();

        resourceManager.createResource(extensionContext,
            internalClientSourceJob.producerStrimzi().build());
        ClientUtils.waitForClientSuccess(sourceProducerName, namespaceName, MESSAGE_COUNT);

        LOGGER.info("Wait 1 second as 'sync.group.offsets.interval.seconds=1'. As this is insignificant wait, we're skipping it");

        LOGGER.info("Receive {} messages from mirrored topic on Target cluster.", MESSAGE_COUNT);
        resourceManager.createResource(extensionContext,
            initialInternalClientTargetJob.consumerStrimzi().build());
        ClientUtils.waitForClientSuccess(targetConsumerName, namespaceName, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(namespaceName, sourceProducerName);
        JobUtils.deleteJobWithWait(namespaceName, targetConsumerName);

        LOGGER.info("Send 50 messages to Source cluster");
        internalClientSourceJob = internalClientSourceJob.toBuilder().withMessageCount(50).withMessage("Producer C").build();
        resourceManager.createResource(extensionContext,
            internalClientSourceJob.producerStrimzi().build());
        ClientUtils.waitForClientSuccess(sourceProducerName, namespaceName, 50);
        JobUtils.deleteJobWithWait(namespaceName, sourceProducerName);

        LOGGER.info("Wait 1 second as 'sync.group.offsets.interval.seconds=1'. As this is insignificant wait, we're skipping it");
        LOGGER.info("Receive 10 msgs from source cluster");
        internalClientSourceJob = internalClientSourceJob.toBuilder().withMessageCount(10).withAdditionalConfig("max.poll.records=10").build();
        resourceManager.createResource(extensionContext,
            internalClientSourceJob.consumerStrimzi().build());
        ClientUtils.waitForClientSuccess(sourceConsumerName, namespaceName, 10);
        JobUtils.deleteJobWithWait(namespaceName, sourceConsumerName);

        LOGGER.info("Wait 1 second as 'sync.group.offsets.interval.seconds=1'. As this is insignificant wait, we're skipping it");

        LOGGER.info("Receive 40 msgs from mirrored topic on Target cluster");
        KafkaBasicExampleClients internalClientTargetJob = initialInternalClientTargetJob.toBuilder().withMessageCount(40).build();
        resourceManager.createResource(extensionContext,
            internalClientTargetJob.consumerStrimzi().build());
        ClientUtils.waitForClientSuccess(targetConsumerName, namespaceName, 40);
        JobUtils.deleteJobWithWait(namespaceName, targetConsumerName);

        LOGGER.info("There should be no more messages to read. Try to consume at least 1 message. " +
                "This client job should fail on timeout.");
        resourceManager.createResource(extensionContext,
            initialInternalClientTargetJob.consumerStrimzi().build());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(targetConsumerName, namespaceName, 1));

        LOGGER.info("As it's Active-Active MM2 mode, there should be no more messages to read from Source cluster" +
                " topic. This client job should fail on timeout.");
        resourceManager.createResource(extensionContext,
            initialInternalClientSourceJob.consumerStrimzi().build());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(sourceConsumerName, namespaceName, 1));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(NAMESPACE)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .withOperationTimeout(Constants.CO_OPERATION_TIMEOUT_SHORT)
            .createInstallation()
            .runInstallation();
    }
}
