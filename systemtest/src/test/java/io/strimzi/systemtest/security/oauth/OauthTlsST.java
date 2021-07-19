/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.keycloak.KeycloakInstance;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeExampleClients;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaOauthExampleClients;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.specific.KeycloakUtils;
import io.vertx.core.cli.annotations.Description;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.HTTP_BRIDGE_DEFAULT_PORT;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(OAUTH)
@Tag(REGRESSION)
@Tag(ACCEPTANCE)
public class OauthTlsST extends OauthAbstractST {
    protected static final Logger LOGGER = LogManager.getLogger(OauthTlsST.class);

    private final String oauthClusterName = "oauth-cluster-tls-name";
    private static final String NAMESPACE = "oauth2-tls-cluster-test";

    @Description(
            "As an oauth producer, I am able to produce messages to the kafka broker\n" +
            "As an oauth consumer, I am able to consumer messages from the kafka broker using encrypted communication")
    @ParallelTest
    void testProducerConsumer(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName).build());

        KafkaOauthExampleClients oauthExampleClients = new KafkaOauthExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
    }

    @Description("As an oauth KafkaConnect, I am able to sink messages from kafka broker topic using encrypted communication.")
    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testProducerConsumerConnect(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName).build());

        KafkaOauthExampleClients oauthExampleClients = new KafkaOauthExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, producerName);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, consumerName);

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, oauthClusterName + "-" + Constants.KAFKA_CLIENTS).build());

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(oauthClusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, oauthClusterName, 1)
            .editSpec()
                .withConfig(connectorConfig)
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewKafkaClientAuthenticationOAuth()
                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .withClientId("kafka-connect")
                    .withNewClientSecret()
                    .withSecretName("my-connect-oauth")
                    .withKey(OAUTH_KEY)
                    .endClientSecret()
                    .withTlsTrustedCertificates(
                        new CertSecretSourceBuilder()
                            .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                            .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                            .build())
                    .withDisableTlsHostnameVerification(true)
                .endKafkaClientAuthenticationOAuth()
                .withNewTls()
                    .addNewTrustedCertificate()
                        .withSecretName(oauthClusterName + "-cluster-ca-cert")
                        .withCertificate("ca.crt")
                    .endTrustedCertificate()
                .endTls()
                .withBootstrapServers(oauthClusterName + "-kafka-bootstrap:9093")
            .endSpec()
            .build());

        String kafkaConnectPodName = kubeClient().listPods(clusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(kafkaConnectPodName);

        KafkaConnectorUtils.createFileSinkConnector(defaultKafkaClientsPodName, topicName, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(clusterName, NAMESPACE, 8083));


        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH);
    }

    @Description("As a oauth bridge, i am able to send messages to bridge endpoint using encrypted communication")
    @ParallelTest
    @Tag(BRIDGE)
    void testProducerConsumerBridge(ExtensionContext extensionContext) {
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName).build());

        KafkaOauthExampleClients oauthExampleClients = new KafkaOauthExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, producerName);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, consumerName);

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, kafkaClientsName).build());

        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(oauthClusterName, KafkaResources.tlsBootstrapAddress(oauthClusterName), 1)
            .editSpec()
                .withNewTls()
                    .withTrustedCertificates(
                        new CertSecretSourceBuilder()
                            .withCertificate("ca.crt")
                            .withSecretName(KafkaResources.clusterCaCertificateSecretName(oauthClusterName)).build())
                .endTls()
                .withNewKafkaClientAuthenticationOAuth()
                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .withClientId("kafka-bridge")
                    .withNewClientSecret()
                        .withSecretName(BRIDGE_OAUTH_SECRET)
                        .withKey(OAUTH_KEY)
                    .endClientSecret()
                    .addNewTlsTrustedCertificate()
                        .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                        .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                    .endTlsTrustedCertificate()
                    .withDisableTlsHostnameVerification(true)
                .endKafkaClientAuthenticationOAuth()
            .endSpec()
            .build());

        producerName = "bridge-producer-" + clusterName;

        KafkaBridgeExampleClients kafkaBridgeClientJob = new KafkaBridgeExampleClients.Builder()
            .withProducerName(producerName)
            .withBootstrapAddress(KafkaBridgeResources.serviceName(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(10)
            .withPort(HTTP_BRIDGE_DEFAULT_PORT)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .build();

        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.producerStrimziBridge().build());
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, producerName);
    }

    @Description("As a oauth mirror maker, I am able to replicate topic data using using encrypted communication")
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(MIRROR_MAKER)
    @Tag(NODEPORT_SUPPORTED)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testMirrorMaker(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName).build());

        KafkaOauthExampleClients oauthExampleClients = new KafkaOauthExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, oauthExampleClients.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, producerName);

        resourceManager.createResource(extensionContext, oauthExampleClients.consumerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, consumerName);

        String targetKafkaCluster = oauthClusterName + "-target";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(targetKafkaCluster, 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationOAuth()
                                .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                .withTlsTrustedCertificates(
                                    new CertSecretSourceBuilder()
                                        .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                        .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                        .build())
                                    .withDisableTlsHostnameVerification(true)
                                .endKafkaListenerAuthenticationOAuth()
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationOAuth()
                                .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                .withTlsTrustedCertificates(
                                    new CertSecretSourceBuilder()
                                        .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                        .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                        .build())
                                    .withDisableTlsHostnameVerification(true)
                                .endKafkaListenerAuthenticationOAuth()
                                .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(oauthClusterName, oauthClusterName, targetKafkaCluster,
            ClientUtils.generateRandomConsumerGroup(), 1, true)
                .editSpec()
                    .withNewConsumer()
                        // this is for kafka tls connection
                        .withNewTls()
                            .withTrustedCertificates(new CertSecretSourceBuilder()
                                .withCertificate("ca.crt")
                                .withSecretName(KafkaResources.clusterCaCertificateSecretName(oauthClusterName))
                                .build())
                        .endTls()
                        .withBootstrapServers(KafkaResources.tlsBootstrapAddress(oauthClusterName))
                        .withGroupId(ClientUtils.generateRandomConsumerGroup())
                        .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .withNewKafkaClientAuthenticationOAuth()
                            .withNewTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                            .withClientId("kafka-mirror-maker")
                            .withNewClientSecret()
                                .withSecretName(MIRROR_MAKER_OAUTH_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                            // this is for authorization server tls connection
                            .withTlsTrustedCertificates(new CertSecretSourceBuilder()
                                .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                .build())
                            .withDisableTlsHostnameVerification(true)
                        .endKafkaClientAuthenticationOAuth()
                    .endConsumer()
                    .withNewProducer()
                        .withBootstrapServers(KafkaResources.tlsBootstrapAddress(targetKafkaCluster))
                        // this is for kafka tls connection
                        .withNewTls()
                            .withTrustedCertificates(new CertSecretSourceBuilder()
                                .withCertificate("ca.crt")
                                .withSecretName(KafkaResources.clusterCaCertificateSecretName(targetKafkaCluster))
                                .build())
                        .endTls()
                        .withNewKafkaClientAuthenticationOAuth()
                            .withNewTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                            .withClientId("kafka-mirror-maker")
                            .withNewClientSecret()
                                .withSecretName(MIRROR_MAKER_OAUTH_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                            // this is for authorization server tls connection
                            .withTlsTrustedCertificates(new CertSecretSourceBuilder()
                                .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                .build())
                            .withDisableTlsHostnameVerification(true)
                        .endKafkaClientAuthenticationOAuth()
                        .addToConfig(ProducerConfig.ACKS_CONFIG, "all")
                    .endProducer()
                .endSpec()
                .build());

        String mirrorMakerPodName = kubeClient().listPodsByPrefixInName(KafkaMirrorMakerResources.deploymentName(oauthClusterName)).get(0).getMetadata().getName();
        String kafkaMirrorMakerLogs = kubeClient().logs(mirrorMakerPodName);

        assertThat(kafkaMirrorMakerLogs,
            not(containsString("keytool error: java.io.FileNotFoundException: /opt/kafka/consumer-oauth-certs/**/* (No such file or directory)")));

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(oauthClusterName, USER_NAME).build());
        KafkaUserUtils.waitForKafkaUserCreation(USER_NAME);

        LOGGER.info("Creating new client with new consumer-group and also to point on {} cluster", targetKafkaCluster);

        KafkaOauthExampleClients kafkaOauthClientJob = new KafkaOauthExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withUserName(USER_NAME)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(targetKafkaCluster))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withOAuthClientId(OAUTH_CLIENT_NAME)
            .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, kafkaOauthClientJob.consumerStrimziOauthTls(targetKafkaCluster).build());

        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, consumerName);
    }

    @ParallelTest
    void testIntrospectionEndpoint(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName).build());

        LOGGER.info("Deploying kafka...");

        keycloakInstance.setIntrospectionEndpointUri("https://" + keycloakInstance.getHttpsUri() + "/auth/realms/internal/protocol/openid-connect/token/introspect");
        String introspectionKafka = oauthClusterName + "-intro";

        CertSecretSource cert = new CertSecretSourceBuilder()
                .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                .build();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(introspectionKafka, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withName("tls")
                        .withPort(9093)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .withNewKafkaListenerAuthenticationOAuth()
                            .withClientId(OAUTH_KAFKA_BROKER_NAME)
                            .withNewClientSecret()
                                .withSecretName(OAUTH_KAFKA_BROKER_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                            .withAccessTokenIsJwt(false)
                            .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                            .withIntrospectionEndpointUri(keycloakInstance.getIntrospectionEndpointUri())
                            .withTlsTrustedCertificates(cert)
                            .withDisableTlsHostnameVerification(true)
                        .endKafkaListenerAuthenticationOAuth()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        KafkaOauthExampleClients oauthInternalClientIntrospectionJob = new KafkaOauthExampleClients.Builder()
                .withProducerName(producerName)
                .withConsumerName(consumerName)
                .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(introspectionKafka))
                .withTopicName(topicName)
                .withMessageCount(MESSAGE_COUNT)
                .withOAuthClientId(OAUTH_CLIENT_NAME)
                .withOAuthClientSecret(OAUTH_CLIENT_SECRET)
                .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                .build();

        resourceManager.createResource(extensionContext, oauthInternalClientIntrospectionJob.producerStrimziOauthTls(introspectionKafka).build());
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);

        resourceManager.createResource(extensionContext, oauthInternalClientIntrospectionJob.consumerStrimziOauthTls(introspectionKafka).build());
        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);
    }

    @BeforeAll
    void setUp(ExtensionContext extensionContext) {
        super.beforeAllMayOverride(extensionContext);
        // for namespace
        super.setupCoAndKeycloak(extensionContext, NAMESPACE);

        keycloakInstance.setRealm("internal", true);

        LOGGER.info("Keycloak settings {}", keycloakInstance.toString());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(oauthClusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationOAuth()
                                .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                .withTlsTrustedCertificates(
                                    new CertSecretSourceBuilder()
                                        .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                        .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                        .build())
                                .withDisableTlsHostnameVerification(true)
                            .endKafkaListenerAuthenticationOAuth()
                            .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(oauthClusterName, OAUTH_CLIENT_NAME).build());
    }

    @AfterAll
    void tearDown(ExtensionContext extensionContext) throws Exception {
        // delete keycloak before namespace
        KeycloakUtils.deleteKeycloak(NAMESPACE);
        // delete namespace etc.
        super.afterAllMayOverride(extensionContext);
    }

}



