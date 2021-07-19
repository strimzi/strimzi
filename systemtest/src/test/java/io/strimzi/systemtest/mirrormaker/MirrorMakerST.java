/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.mirrormaker;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaMirrorMakerStatus;
import io.strimzi.api.kafka.model.template.DeploymentStrategy;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.SetupClusterOperator;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMakerUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.timemeasuring.Operation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.SCALABILITY;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(MIRROR_MAKER)
@Tag(INTERNAL_CLIENTS_USED)
public class MirrorMakerST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MirrorMakerST.class);

    public static final String NAMESPACE = "mm-cluster-test";
    private final int messagesCount = 200;

    @ParallelNamespaceTest
    void testMirrorMaker(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClusterSourceName = clusterName + "-source";
        String kafkaClusterTargetName = clusterName + "-target";

        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        String operationId = timeMeasuringSystem.startTimeMeasuring(Operation.MM_DEPLOYMENT, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());

        String topicSourceName = TOPIC_NAME + "-source" + "-" + rng.nextInt(Integer.MAX_VALUE);

        // Deploy source kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());
        // Deploy Topic
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicSourceName).build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(namespaceName, false, clusterName + "-" + Constants.KAFKA_CLIENTS).build());

        final String kafkaClientsPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(namespaceName, clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName("topic-for-test-broker-1")
            .withNamespaceName(namespaceName)
            .withClusterName(kafkaClusterSourceName)
            .withMessageCount(messagesCount)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        // Check brokers availability
        internalKafkaClient.produceAndConsumesPlainMessagesUntilBothOperationsAreSuccessful();

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName("topic-for-test-broker-2")
            .withClusterName(kafkaClusterTargetName)
            .build();

        internalKafkaClient.produceAndConsumesPlainMessagesUntilBothOperationsAreSuccessful();

        // Deploy Mirror Maker
        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(clusterName, kafkaClusterSourceName, kafkaClusterTargetName, ClientUtils.generateRandomConsumerGroup(), 1, false)
            .editSpec()
            .withResources(new ResourceRequirementsBuilder()
                    .addToLimits("memory", new Quantity("400M"))
                    .addToLimits("cpu", new Quantity("2"))
                    .addToRequests("memory", new Quantity("300M"))
                    .addToRequests("cpu", new Quantity("1"))
                    .build())
            .withNewJvmOptions()
                .withXmx("200m")
                .withXms("200m")
                .withXx(jvmOptionsXX)
            .endJvmOptions()
            .endSpec()
            .build());

        verifyLabelsOnPods(namespaceName, clusterName, "mirror-maker", "KafkaMirrorMaker");
        verifyLabelsForService(namespaceName, clusterName, "mirror-maker", "KafkaMirrorMaker");

        verifyLabelsForConfigMaps(namespaceName, kafkaClusterSourceName, null, kafkaClusterTargetName);
        verifyLabelsForServiceAccounts(namespaceName, kafkaClusterSourceName, null);

        String mirrorMakerPodName = kubeClient(namespaceName).listPodsByPrefixInName(KafkaMirrorMakerResources.deploymentName(clusterName)).get(0).getMetadata().getName();
        String kafkaMirrorMakerLogs = kubeClient(namespaceName).logs(mirrorMakerPodName);

        assertThat(kafkaMirrorMakerLogs,
            not(containsString("keytool error: java.io.FileNotFoundException: /opt/kafka/consumer-oauth-certs/**/* (No such file or directory)")));

        String podName = kubeClient(namespaceName).listPodsByNamespace(namespaceName, clusterName).stream().filter(n -> n.getMetadata().getName().startsWith(KafkaMirrorMakerResources.deploymentName(clusterName))).findFirst().orElseThrow().getMetadata().getName();
        assertResources(namespaceName, podName, clusterName.concat("-mirror-maker"),
                "400M", "2", "300M", "1");
        assertExpectedJavaOpts(namespaceName, podName, KafkaMirrorMakerResources.deploymentName(clusterName),
                "-Xmx200m", "-Xms200m", "-XX:+UseG1GC");

        timeMeasuringSystem.stopOperation(operationId, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicSourceName)
            .withClusterName(kafkaClusterSourceName)
            .build();

        int sent = internalKafkaClient.sendMessagesPlain();

        internalKafkaClient.consumesPlainMessagesUntilOperationIsSuccessful(sent);

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withClusterName(kafkaClusterTargetName)
            .build();

        internalKafkaClient.consumesPlainMessagesUntilOperationIsSuccessful(sent);
    }

    /**
     * Test mirroring messages by Mirror Maker over tls transport using mutual tls auth
     */
    @ParallelNamespaceTest
    @Tag(ACCEPTANCE)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testMirrorMakerTlsAuthenticated(ExtensionContext extensionContext) throws Exception {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClusterSourceName = clusterName + "-source";
        String kafkaClusterTargetName = clusterName + "-target";
        String topicSourceName = TOPIC_NAME + "-source" + "-" + rng.nextInt(Integer.MAX_VALUE);
        String kafkaSourceUserName = clusterName + "-my-user-source";
        String kafkaTargetUserName = clusterName + "-my-user-target";

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
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicSourceName).build());

        // createAndWaitForReadiness Kafka user
        KafkaUser userSource = KafkaUserTemplates.tlsUser(kafkaClusterSourceName, kafkaSourceUserName).build();
        KafkaUser userTarget = KafkaUserTemplates.tlsUser(kafkaClusterTargetName, kafkaTargetUserName).build();

        resourceManager.createResource(extensionContext, userSource);
        resourceManager.createResource(extensionContext, userTarget);

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterSourceName));

        // Initialize CertSecretSource with certificate and secret names for producer
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterTargetName));

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(namespaceName, true, clusterName + "-" + Constants.KAFKA_CLIENTS, userSource, userTarget).build());

        final String kafkaClientsPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(namespaceName, clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        String baseTopic = mapWithTestTopics.get(extensionContext.getDisplayName());
        String topicTestName1 = baseTopic + "-test-1";
        String topicTestName2 = baseTopic + "-test-2";

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicTestName1).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicTestName2).build());

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
        internalKafkaClient.produceAndConsumesTlsMessagesUntilBothOperationsAreSuccessful();

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicTestName2)
            .withClusterName(kafkaClusterTargetName)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .withKafkaUsername(userTarget.getMetadata().getName())
            .build();

        internalKafkaClient.produceAndConsumesTlsMessagesUntilBothOperationsAreSuccessful();

        // Deploy Mirror Maker with tls listener and mutual tls auth
        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(clusterName, kafkaClusterSourceName, kafkaClusterTargetName, ClientUtils.generateRandomConsumerGroup(), 1, true)
            .editSpec()
                .editConsumer()
                    .withNewTls()
                        .withTrustedCertificates(certSecretSource)
                    .endTls()
                    .withNewKafkaClientAuthenticationTls()
                        .withNewCertificateAndKey()
                            .withSecretName(kafkaSourceUserName)
                            .withCertificate("user.crt")
                            .withKey("user.key")
                        .endCertificateAndKey()
                    .endKafkaClientAuthenticationTls()
                .endConsumer()
                .editProducer()
                    .withNewTls()
                        .withTrustedCertificates(certSecretTarget)
                    .endTls()
                    .withNewKafkaClientAuthenticationTls()
                        .withNewCertificateAndKey()
                            .withSecretName(kafkaTargetUserName)
                            .withCertificate("user.crt")
                            .withKey("user.key")
                        .endCertificateAndKey()
                    .endKafkaClientAuthenticationTls()
                .endProducer()
            .endSpec()
            .build());

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicSourceName)
            .withClusterName(kafkaClusterSourceName)
            .withKafkaUsername(userSource.getMetadata().getName())
            .build();

        internalKafkaClient.produceAndConsumesTlsMessagesUntilBothOperationsAreSuccessful();

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withClusterName(kafkaClusterTargetName)
            .withKafkaUsername(userTarget.getMetadata().getName())
            .build();

        internalKafkaClient.consumesTlsMessagesUntilOperationIsSuccessful(internalKafkaClient.getMessageCount());
    }

    /**
     * Test mirroring messages by Mirror Maker over tls transport using scram-sha auth
     */
    @ParallelNamespaceTest
    @SuppressWarnings("checkstyle:methodlength")
    void testMirrorMakerTlsScramSha(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String kafkaClusterSourceName = clusterName + "-source";
        String kafkaClusterTargetName = clusterName + "-target";
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
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicName).build());

        // createAndWaitForReadiness Kafka user for source cluster
        KafkaUser userSource = KafkaUserTemplates.scramShaUser(kafkaClusterSourceName, kafkaUserSource).build();
        // createAndWaitForReadiness Kafka user for target cluster
        KafkaUser userTarget = KafkaUserTemplates.scramShaUser(kafkaClusterTargetName, kafkaUserTarget).build();

        resourceManager.createResource(extensionContext, userSource);
        resourceManager.createResource(extensionContext, userTarget);

        // Initialize PasswordSecretSource to set this as PasswordSecret in Mirror Maker spec
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName(kafkaUserSource);
        passwordSecretSource.setPassword("password");

        // Initialize PasswordSecretSource to set this as PasswordSecret in Mirror Maker spec
        PasswordSecretSource passwordSecretTarget = new PasswordSecretSource();
        passwordSecretTarget.setSecretName(kafkaUserTarget);
        passwordSecretTarget.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterSourceName));

        // Initialize CertSecretSource with certificate and secret names for producer
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterTargetName));

        // Deploy client
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(namespaceName, true, clusterName + "-" + Constants.KAFKA_CLIENTS, userSource, userTarget).build());

        final String kafkaClientsPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(namespaceName, clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        String baseTopic = mapWithTestTopics.get(extensionContext.getDisplayName());
        String topicTestName1 = baseTopic + "-test-1";
        String topicTestName2 = baseTopic + "-test-2";

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicTestName1).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicTestName2).build());

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
        internalKafkaClient.produceAndConsumesTlsMessagesUntilBothOperationsAreSuccessful();


        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicTestName2)
            .withClusterName(kafkaClusterTargetName)
            .withKafkaUsername(userTarget.getMetadata().getName())
            .build();

        internalKafkaClient.produceAndConsumesTlsMessagesUntilBothOperationsAreSuccessful();

        // Deploy Mirror Maker with TLS and ScramSha512
        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(clusterName, kafkaClusterSourceName, kafkaClusterTargetName, ClientUtils.generateRandomConsumerGroup(), 1, true)
            .editSpec()
                .editConsumer()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(kafkaUserSource)
                        .withPasswordSecret(passwordSecretSource)
                    .endKafkaClientAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecretSource)
                    .endTls()
                .endConsumer()
                .editProducer()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(kafkaUserTarget)
                        .withPasswordSecret(passwordSecretTarget)
                    .endKafkaClientAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecretTarget)
                    .endTls()
                .endProducer()
            .endSpec()
            .build());

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicName)
            .withClusterName(kafkaClusterSourceName)
            .withKafkaUsername(userSource.getMetadata().getName())
            .build();

        internalKafkaClient.produceAndConsumesTlsMessagesUntilBothOperationsAreSuccessful();
        internalKafkaClient.consumesTlsMessagesUntilOperationIsSuccessful(internalKafkaClient.getMessageCount());
    }

    @ParallelNamespaceTest
    void testIncludeList(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClusterSourceName = clusterName + "-source";
        String kafkaClusterTargetName = clusterName + "-target";

        String topicName = "included-topic";
        String topicNotIncluded = "not-included-topic";

        LOGGER.info("Creating kafka source cluster {}", kafkaClusterSourceName);
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        LOGGER.info("Creating kafka target cluster {}", kafkaClusterTargetName);
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, topicNotIncluded).build());

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(namespaceName, false, clusterName + "-" + Constants.KAFKA_CLIENTS).build());

        String kafkaClientsPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(namespaceName, clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName("topic-example-10")
            .withNamespaceName(namespaceName)
            .withClusterName(kafkaClusterSourceName)
            .withMessageCount(messagesCount)
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        // Check brokers availability
        internalKafkaClient.produceAndConsumesPlainMessagesUntilBothOperationsAreSuccessful();

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName("topic-example-11")
            .withClusterName(kafkaClusterTargetName)
            .build();

        internalKafkaClient.produceAndConsumesPlainMessagesUntilBothOperationsAreSuccessful();

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(clusterName, kafkaClusterSourceName, kafkaClusterTargetName, ClientUtils.generateRandomConsumerGroup(), 1, false)
            .editMetadata()
                .withNamespace(namespaceName)
            .endMetadata()
            .editSpec()
                .withNewInclude(topicName)
            .endSpec().build());

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicName)
            .withClusterName(kafkaClusterSourceName)
            .build();

        int sent = internalKafkaClient.sendMessagesPlain();

        internalKafkaClient.consumesPlainMessagesUntilOperationIsSuccessful(sent);

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withClusterName(kafkaClusterTargetName)
            .build();

        internalKafkaClient.consumesPlainMessagesUntilOperationIsSuccessful(sent);

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicNotIncluded)
            .withClusterName(kafkaClusterSourceName)
            .build();

        sent = internalKafkaClient.sendMessagesPlain();

        internalKafkaClient.consumesPlainMessagesUntilOperationIsSuccessful(sent);

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withClusterName(kafkaClusterTargetName)
            .build();

        assertThat("Received 0 messages in target kafka because topic " + topicNotIncluded + " is not included",
            internalKafkaClient.receiveMessagesPlain(), is(0));
    }

    @ParallelNamespaceTest
    void testCustomAndUpdatedValues(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
            .editSpec()
                .withNewEntityOperator()
                .endEntityOperator()
            .endSpec()
            .build());

        String usedVariable = "KAFKA_MIRRORMAKER_CONFIGURATION_PRODUCER";

        LinkedHashMap<String, String> envVarGeneral = new LinkedHashMap<>();
        envVarGeneral.put("TEST_ENV_1", "test.env.one");
        envVarGeneral.put("TEST_ENV_2", "test.env.two");
        envVarGeneral.put(usedVariable, "test.value");

        LinkedHashMap<String, String> envVarUpdated = new LinkedHashMap<>();
        envVarUpdated.put("TEST_ENV_2", "updated.test.env.two");
        envVarUpdated.put("TEST_ENV_3", "test.env.three");

        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put("acks", "all");

        Map<String, Object> updatedProducerConfig = new HashMap<>();
        updatedProducerConfig.put("acks", "0");

        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put("auto.offset.reset", "latest");

        Map<String, Object> updatedConsumerConfig = new HashMap<>();
        updatedConsumerConfig.put("auto.offset.reset", "earliest");

        int initialDelaySeconds = 30;
        int timeoutSeconds = 10;
        int updatedInitialDelaySeconds = 31;
        int updatedTimeoutSeconds = 11;
        int periodSeconds = 10;
        int successThreshold = 1;
        int failureThreshold = 3;
        int updatedPeriodSeconds = 5;
        int updatedFailureThreshold = 1;

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(clusterName, clusterName, clusterName, ClientUtils.generateRandomConsumerGroup(), 1, false)
            .editSpec()
                .editProducer()
                    .withConfig(producerConfig)
                .endProducer()
                .editConsumer()
                    .withConfig(consumerConfig)
                .endConsumer()
                .withNewTemplate()
                    .withNewMirrorMakerContainer()
                        .withEnv(StUtils.createContainerEnvVarsFromMap(envVarGeneral))
                    .endMirrorMakerContainer()
                .endTemplate()
                .withNewReadinessProbe()
                    .withInitialDelaySeconds(initialDelaySeconds)
                    .withTimeoutSeconds(timeoutSeconds)
                    .withPeriodSeconds(periodSeconds)
                    .withSuccessThreshold(successThreshold)
                    .withFailureThreshold(failureThreshold)
                .endReadinessProbe()
                .withNewLivenessProbe()
                    .withInitialDelaySeconds(initialDelaySeconds)
                    .withTimeoutSeconds(timeoutSeconds)
                    .withPeriodSeconds(periodSeconds)
                    .withSuccessThreshold(successThreshold)
                    .withFailureThreshold(failureThreshold)
                .endLivenessProbe()
            .endSpec()
            .build());

        Map<String, String> mirrorMakerSnapshot = DeploymentUtils.depSnapshot(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName));

        // Remove variable which is already in use
        envVarGeneral.remove(usedVariable);
        LOGGER.info("Verify values before update");
        checkReadinessLivenessProbe(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName),
            KafkaMirrorMakerResources.deploymentName(clusterName), initialDelaySeconds, timeoutSeconds, periodSeconds,
            successThreshold, failureThreshold);
        checkSpecificVariablesInContainer(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName),
            KafkaMirrorMakerResources.deploymentName(clusterName), envVarGeneral);
        checkComponentConfiguration(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName),
            KafkaMirrorMakerResources.deploymentName(clusterName), "KAFKA_MIRRORMAKER_CONFIGURATION_PRODUCER", producerConfig);
        checkComponentConfiguration(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName),
            KafkaMirrorMakerResources.deploymentName(clusterName), "KAFKA_MIRRORMAKER_CONFIGURATION_CONSUMER", consumerConfig);

        LOGGER.info("Check if actual env variable {} has different value than {}", usedVariable, "test.value");
        assertThat(StUtils.checkEnvVarInPod(namespaceName, kubeClient().listPods(namespaceName, clusterName, Labels.STRIMZI_KIND_LABEL,
            KafkaMirrorMaker.RESOURCE_KIND).get(0).getMetadata().getName(), usedVariable), CoreMatchers.is(not("test.value")));

        LOGGER.info("Updating values in MirrorMaker container");
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(clusterName, kmm -> {
            kmm.getSpec().getTemplate().getMirrorMakerContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
            kmm.getSpec().getProducer().setConfig(updatedProducerConfig);
            kmm.getSpec().getConsumer().setConfig(updatedConsumerConfig);
            kmm.getSpec().getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kmm.getSpec().getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kmm.getSpec().getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kmm.getSpec().getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kmm.getSpec().getLivenessProbe().setPeriodSeconds(updatedPeriodSeconds);
            kmm.getSpec().getReadinessProbe().setPeriodSeconds(updatedPeriodSeconds);
            kmm.getSpec().getLivenessProbe().setFailureThreshold(updatedFailureThreshold);
            kmm.getSpec().getReadinessProbe().setFailureThreshold(updatedFailureThreshold);
        }, namespaceName);

        DeploymentUtils.waitTillDepHasRolled(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName), 1, mirrorMakerSnapshot);

        LOGGER.info("Verify values after update");
        checkReadinessLivenessProbe(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName),
            KafkaMirrorMakerResources.deploymentName(clusterName), updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkSpecificVariablesInContainer(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName),
            KafkaMirrorMakerResources.deploymentName(clusterName), envVarUpdated);
        checkComponentConfiguration(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName),
            KafkaMirrorMakerResources.deploymentName(clusterName), "KAFKA_MIRRORMAKER_CONFIGURATION_PRODUCER", updatedProducerConfig);
        checkComponentConfiguration(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName),
            KafkaMirrorMakerResources.deploymentName(clusterName), "KAFKA_MIRRORMAKER_CONFIGURATION_CONSUMER", updatedConsumerConfig);
    }

    @ParallelNamespaceTest
    @Tag(SCALABILITY)
    void testScaleMirrorMakerSubresource(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClusterSourceName = clusterName + "-source";
        String kafkaClusterTargetName = clusterName + "-target";

        LOGGER.info("Creating kafka source cluster {}", kafkaClusterSourceName);
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        LOGGER.info("Creating kafka target cluster {}", kafkaClusterTargetName);
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, ClientUtils.generateRandomConsumerGroup(), 1, false).build());

        int scaleTo = 4;
        long mmObsGen = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getObservedGeneration();
        String mmGenName = kubeClient(namespaceName).listPods(namespaceName, clusterName, Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker.RESOURCE_KIND).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling KafkaMirrorMaker subresource <-------");
        LOGGER.info("Scaling subresource replicas to {}", scaleTo);
        cmdKubeClient().namespace(namespaceName).scaleByName(KafkaMirrorMaker.RESOURCE_KIND, clusterName, scaleTo);
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName), scaleTo);

        LOGGER.info("Check if replicas is set to {}, naming prefix should be same and observed generation higher", scaleTo);
        List<String> mmPods = kubeClient(namespaceName).listPodNames(namespaceName, clusterName, Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker.RESOURCE_KIND);
        assertThat(mmPods.size(), is(4));
        assertThat(KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(namespaceName).withName(clusterName).get().getSpec().getReplicas(), is(4));
        assertThat(KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getReplicas(), is(4));
        /*
        observed generation should be higher than before scaling -> after change of spec and successful reconciliation,
        the observed generation is increased
        */
        assertThat(mmObsGen < KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getObservedGeneration(), is(true));
        for (String pod : mmPods) {
            assertThat(pod.contains(mmGenName), is(true));
        }
    }

    @ParallelNamespaceTest
    @Tag(SCALABILITY)
    void testScaleMirrorMakerToZero(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClusterSourceName = clusterName + "-source";
        String kafkaClusterTargetName = clusterName + "-target";

        LOGGER.info("Creating kafka source cluster {}", kafkaClusterSourceName);
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        LOGGER.info("Creating kafka target cluster {}", kafkaClusterTargetName);
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, "my-group" + rng.nextInt(Integer.MAX_VALUE), 3, false).build());

        long oldObsGen = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getObservedGeneration();
        String mmDepName = KafkaMirrorMakerResources.deploymentName(clusterName);
        List<String> mmPods = kubeClient(namespaceName).listPodNames(clusterName, Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker.RESOURCE_KIND);
        assertThat(mmPods.size(), is(3));

        LOGGER.info("Scaling MirrorMaker to zero");
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(clusterName, mm -> mm.getSpec().setReplicas(0), namespaceName);

        PodUtils.waitForPodsReady(namespaceName, kubeClient(namespaceName).getDeploymentSelectors(mmDepName), 0, true);

        mmPods = kubeClient(namespaceName).listPodNames(clusterName, Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker.RESOURCE_KIND);
        KafkaMirrorMakerStatus mmStatus = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(namespaceName).withName(clusterName).get().getStatus();
        long actualObsGen = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getObservedGeneration();

        assertThat(mmPods.size(), is(0));
        assertThat(mmStatus.getConditions().get(0).getType(), is(Ready.toString()));
        assertThat(actualObsGen, is(not(oldObsGen)));
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

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, ClientUtils.generateRandomConsumerGroup(), 1, false)
            .editSpec()
                .editOrNewTemplate()
                    .editOrNewDeployment()
                        .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                    .endDeployment()
                .endTemplate()
            .endSpec()
            .build());

        String mmDepName = KafkaMirrorMakerResources.deploymentName(clusterName);

        LOGGER.info("Adding label to MirrorMaker resource, the CR should be recreateAndWaitForReadinessd");
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(clusterName,
            mm -> mm.getMetadata().setLabels(Collections.singletonMap("some", "label")), namespaceName);
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, mmDepName, 1);

        KafkaMirrorMaker kmm = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(namespaceName).withName(clusterName).get();

        LOGGER.info("Checking that observed gen. is still on 1 (recreation) and new label is present");
        assertThat(kmm.getStatus().getObservedGeneration(), is(1L));
        assertThat(kmm.getMetadata().getLabels().toString(), Matchers.containsString("some=label"));
        assertThat(kmm.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.RECREATE));

        LOGGER.info("Changing deployment strategy to {}", DeploymentStrategy.ROLLING_UPDATE);
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(clusterName,
            mm -> mm.getSpec().getTemplate().getDeployment().setDeploymentStrategy(DeploymentStrategy.ROLLING_UPDATE), namespaceName);
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(namespaceName, clusterName);

        LOGGER.info("Adding another label to MirrorMaker resource, pods should be rolled");
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(clusterName, mm -> mm.getMetadata().getLabels().put("another", "label"), namespaceName);
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, mmDepName, 1);

        LOGGER.info("Checking that observed gen. higher (rolling update) and label is changed");
        kmm = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(namespaceName).withName(clusterName).get();
        assertThat(kmm.getStatus().getObservedGeneration(), is(2L));
        assertThat(kmm.getMetadata().getLabels().toString(), Matchers.containsString("another=label"));
        assertThat(kmm.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.ROLLING_UPDATE));
    }

    @BeforeAll
    void setupEnvironment(ExtensionContext extensionContext) {
        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(NAMESPACE)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .withOperationTimeout(Constants.CO_OPERATION_TIMEOUT_MEDIUM)
            .createInstallation()
            .runInstallation();
    }
}
