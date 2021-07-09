/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaAuthorizationKeycloak;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.keycloak.KeycloakInstance;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaOauthExampleClients;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.specific.KeycloakUtils;
import io.strimzi.test.WaitException;
import io.vertx.core.cli.annotations.Description;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(OAUTH)
@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OauthAuthorizationST extends OauthAbstractST {
    protected static final Logger LOGGER = LogManager.getLogger(OauthAuthorizationST.class);

    private final String oauthClusterName = "oauth-cluster-authz-name";
    private static final String NAMESPACE = "oauth2-authz-cluster-test";

    private static final String TEAM_A_CLIENT = "team-a-client";
    private static final String TEAM_B_CLIENT = "team-b-client";
    private static final String KAFKA_CLIENT_ID = "kafka";

    private static final String TEAM_A_CLIENT_SECRET = "team-a-client-secret";
    private static final String TEAM_B_CLIENT_SECRET = "team-b-client-secret";

    private static final String TOPIC_A = "a-topic";
    private static final String TOPIC_B = "b-topic";
    private static final String TOPIC_X = "x-topic";

    private static final String TEAM_A_PRODUCER_NAME = TEAM_A_CLIENT + "-producer";
    private static final String TEAM_A_CONSUMER_NAME = TEAM_A_CLIENT + "-consumer";
    private static final String TEAM_B_PRODUCER_NAME = TEAM_B_CLIENT + "-producer";
    private static final String TEAM_B_CONSUMER_NAME = TEAM_B_CLIENT + "-consumer";

    private static final String TEST_REALM = "kafka-authz";

    @Description("As a member of team A, I should be able to read and write to all topics starting with a-")
    @ParallelTest
    @Order(1)
    void smokeTestForClients(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String teamAProducerName = TEAM_A_PRODUCER_NAME + "-" + clusterName;
        String teamAConsumerName = TEAM_A_CONSUMER_NAME + "-" + clusterName;
        String topicName = TOPIC_A + "-" + mapWithTestTopics.get(extensionContext.getDisplayName());
        String consumerGroup = "a-consumer_group-" + clusterName;

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName).build());

        KafkaOauthExampleClients teamAOauthClientJob = new KafkaOauthExampleClients.Builder()
            .withProducerName(teamAProducerName)
            .withConsumerName(teamAConsumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroup(consumerGroup)
            .withOAuthClientId(TEAM_A_CLIENT)
            .withOAuthClientSecret(TEAM_A_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(teamAProducerName, NAMESPACE, MESSAGE_COUNT);
        resourceManager.createResource(extensionContext, teamAOauthClientJob.consumerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(teamAConsumerName, NAMESPACE, MESSAGE_COUNT);
    }

    @Description("As a member of team A, I should be able to write to topics that starts with x- on any cluster and " +
            "and should also write and read to topics starting with 'a-'")
    @ParallelTest
    @Order(2)
    void testTeamAWriteToTopic(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String teamAProducerName = TEAM_A_PRODUCER_NAME + "-" + clusterName;
        String teamAConsumerName = TEAM_A_CONSUMER_NAME + "-" + clusterName;
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String consumerGroup = "a-consumer_group-" + clusterName;

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName).build());

        KafkaOauthExampleClients teamAOauthClientJob = new KafkaOauthExampleClients.Builder()
            .withProducerName(teamAProducerName)
            .withConsumerName(teamAConsumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroup(consumerGroup)
            .withOAuthClientId(TEAM_A_CLIENT)
            .withOAuthClientSecret(TEAM_A_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, topicName);
        LOGGER.info("Producer will not produce messages because authorization topic will failed. Team A can write only to topic starting with 'x-'");

        resourceManager.createResource(extensionContext, teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName).build());
        assertThrows(WaitException.class, () -> JobUtils.waitForJobFailure(teamAProducerName, 30_000));
        JobUtils.deleteJobWithWait(NAMESPACE, teamAProducerName);

        String topicXName = TOPIC_X + "-" + clusterName;
        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, topicXName);

        teamAOauthClientJob = teamAOauthClientJob.toBuilder()
            .withConsumerGroup(consumerGroup)
            .withTopicName(topicXName)
            .build();

        resourceManager.createResource(extensionContext, teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName).build());
        assertThrows(WaitException.class, () -> JobUtils.waitForJobFailure(teamAProducerName, 30_000));
        JobUtils.deleteJobWithWait(NAMESPACE, teamAProducerName);

        // Team A can not create topic starting with 'x-' only write to existing on
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicXName).build());
        resourceManager.createResource(extensionContext, teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(teamAProducerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, teamAProducerName);

        String topicAName = TOPIC_A + "-" + clusterName;

        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, topicAName);

        teamAOauthClientJob = teamAOauthClientJob.toBuilder()
            .withConsumerGroup(consumerGroup)
            .withTopicName(topicAName)
            .build();

        resourceManager.createResource(extensionContext, teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(teamAProducerName, NAMESPACE, MESSAGE_COUNT);
    }

    @Description("As a member of team A, I should be able only read from consumer that starts with a_")
    @ParallelTest
    @Order(3)
    void testTeamAReadFromTopic(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String teamAProducerName = TEAM_A_PRODUCER_NAME + "-" + clusterName;
        String teamAConsumerName = TEAM_A_CONSUMER_NAME + "-" + clusterName;
        String topicAName = TOPIC_A + "-" + mapWithTestTopics.get(extensionContext.getDisplayName());
        String consumerGroup = "a-consumer_group-" + clusterName;

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicAName).build());

        KafkaOauthExampleClients teamAOauthClientJob = new KafkaOauthExampleClients.Builder()
            .withProducerName(teamAProducerName)
            .withConsumerName(teamAConsumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(topicAName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroup(consumerGroup)
            .withOAuthClientId(TEAM_A_CLIENT)
            .withOAuthClientSecret(TEAM_A_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, topicAName);
        resourceManager.createResource(extensionContext, teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(teamAProducerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, teamAProducerName);

        // team A client shouldn't be able to consume messages with wrong consumer group

        teamAOauthClientJob = teamAOauthClientJob.toBuilder()
            .withConsumerGroup("bad_consumer_group" + clusterName)
            .withTopicName(topicAName)
            .build();

        resourceManager.createResource(extensionContext, teamAOauthClientJob.consumerStrimziOauthTls(oauthClusterName).build());
        assertThrows(WaitException.class, () -> JobUtils.waitForJobFailure(teamAConsumerName, 30_000));
        JobUtils.deleteJobWithWait(NAMESPACE, teamAProducerName);

        // team A client should be able to consume messages with correct consumer group

        teamAOauthClientJob = teamAOauthClientJob.toBuilder()
            .withConsumerGroup("a-correct_consumer_group" + clusterName)
            .withTopicName(topicAName)
            .build();

        resourceManager.createResource(extensionContext, teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(teamAProducerName, NAMESPACE, MESSAGE_COUNT);
    }

    @Description("As a member of team B, I should be able to write and read from topics that starts with b-")
    @ParallelTest
    @Order(4)
    void testTeamBWriteToTopic(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String consumerGroup = "x-" + clusterName;

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName).build());

        KafkaOauthExampleClients teamBOauthClientJob = new KafkaOauthExampleClients.Builder()
            .withProducerName(TEAM_B_PRODUCER_NAME)
            .withConsumerName(TEAM_B_CONSUMER_NAME)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroup(consumerGroup)
            .withOAuthClientId(TEAM_B_CLIENT)
            .withOAuthClientSecret(TEAM_B_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, TOPIC_NAME);
        // Producer will not produce messages because authorization topic will failed. Team A can write only to topic starting with 'x-'
        resourceManager.createResource(extensionContext, teamBOauthClientJob.producerStrimziOauthTls(oauthClusterName).build());
        assertThrows(WaitException.class, () -> JobUtils.waitForJobFailure(TEAM_B_PRODUCER_NAME, 30_000));
        JobUtils.deleteJobWithWait(NAMESPACE, TEAM_B_PRODUCER_NAME);

        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, TOPIC_B);

        teamBOauthClientJob = teamBOauthClientJob.toBuilder()
            .withConsumerGroup("x-consumer_group_b-" + clusterName)
            .withTopicName(TOPIC_B)
            .build();

        resourceManager.createResource(extensionContext, teamBOauthClientJob.producerStrimziOauthTls(oauthClusterName).build());
        resourceManager.createResource(extensionContext, teamBOauthClientJob.consumerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitTillContinuousClientsFinish(TEAM_B_PRODUCER_NAME, TEAM_B_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);
    }

    @Description("As a member of team A, I can write to topics starting with 'x-' and " +
            "as a member of team B can read from topics starting with 'x-'")
    @ParallelTest
    @Order(5)
    void testTeamAWriteToTopicStartingWithXAndTeamBReadFromTopicStartingWithX(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String teamAProducerName = TEAM_A_PRODUCER_NAME + "-" + clusterName;
        String teamAConsumerName = TEAM_A_CONSUMER_NAME + "-" + clusterName;
        String teamBProducerName = TEAM_B_PRODUCER_NAME + "-" + clusterName;
        String teamBConsumerName = TEAM_B_CONSUMER_NAME + "-" + clusterName;
        // only write means that Team A can not create new topic 'x-.*'
        String topicXName = TOPIC_X + mapWithTestTopics.get(extensionContext.getDisplayName());
        String consumerGroup = "x-" + clusterName;

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicXName).build());

        KafkaOauthExampleClients teamAOauthClientJob = new KafkaOauthExampleClients.Builder()
            .withProducerName(teamAProducerName)
            .withConsumerName(teamAConsumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(topicXName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroup(consumerGroup)
            .withOAuthClientId(TEAM_A_CLIENT)
            .withOAuthClientSecret(TEAM_A_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        teamAOauthClientJob = teamAOauthClientJob.toBuilder()
            .withConsumerGroup("a-consumer_group" + clusterName)
            .withTopicName(topicXName)
            .build();

        resourceManager.createResource(extensionContext, teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(teamAProducerName, NAMESPACE, MESSAGE_COUNT);

        KafkaOauthExampleClients teamBOauthClientJob = new KafkaOauthExampleClients.Builder()
            .withProducerName(teamBProducerName)
            .withConsumerName(teamBConsumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(topicXName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroup("x-consumer_group_b-" + clusterName)
            .withOAuthClientId(TEAM_B_CLIENT)
            .withOAuthClientSecret(TEAM_B_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, teamBOauthClientJob.consumerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(teamBConsumerName, NAMESPACE, MESSAGE_COUNT);
    }

    @Description("As a superuser of team A and team B, i am able to break defined authorization rules")
    @ParallelTest
    @Order(6)
    void testSuperUserWithOauthAuthorization(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String teamAProducerName = TEAM_A_PRODUCER_NAME + "-" + clusterName;
        String teamAConsumerName = TEAM_A_CONSUMER_NAME + "-" + clusterName;
        String teamBProducerName = TEAM_B_PRODUCER_NAME + "-" + clusterName;
        String teamBConsumerName = TEAM_B_CONSUMER_NAME + "-" + clusterName;
        // only write means that Team A can not create new topic 'x-.*'
        String topicXName = TOPIC_X + mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicXName).build());

        LOGGER.info("Verifying that team B is not able write to topic starting with 'x-' because in kafka cluster" +
                "does not have super-users to break authorization rules");

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(oauthClusterName, userName).build());

        KafkaOauthExampleClients teamBOauthClientJob = new KafkaOauthExampleClients.Builder()
            .withProducerName(teamBProducerName)
            .withConsumerName(teamBConsumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(topicXName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroup("x-consumer_group_b-" + clusterName)
            .withOAuthClientId(TEAM_B_CLIENT)
            .withOAuthClientSecret(TEAM_B_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .withUserName(userName)
            .build();

        resourceManager.createResource(extensionContext, teamBOauthClientJob.producerStrimziOauthTls(oauthClusterName).build());
        assertThrows(WaitException.class, () -> JobUtils.waitForJobFailure(teamBProducerName, 30_000));
        JobUtils.deleteJobWithWait(NAMESPACE, teamBProducerName);

        LOGGER.info("Verifying that team A is not able read to topic starting with 'x-' because in kafka cluster" +
                "does not have super-users to break authorization rules");

        KafkaOauthExampleClients teamAOauthClientJob = new KafkaOauthExampleClients.Builder()
            .withProducerName(teamAProducerName)
            .withConsumerName(teamAConsumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(topicXName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroup("x-consumer_group_b1-" + clusterName)
            .withOAuthClientId(TEAM_A_CLIENT)
            .withOAuthClientSecret(TEAM_A_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .withUserName(userName)
            .build();

        resourceManager.createResource(extensionContext, teamAOauthClientJob.consumerStrimziOauthTls(oauthClusterName).build());
        assertThrows(WaitException.class, () -> JobUtils.waitForJobFailure(teamAConsumerName, 30_000));
        JobUtils.deleteJobWithWait(NAMESPACE, teamAConsumerName);

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(oauthClusterName));

        KafkaResource.replaceKafkaResource(oauthClusterName, kafka -> {

            List<String> superUsers = new ArrayList<>(2);
            superUsers.add("service-account-" + TEAM_A_CLIENT);
            superUsers.add("service-account-" + TEAM_B_CLIENT);

            ((KafkaAuthorizationKeycloak) kafka.getSpec().getKafka().getAuthorization()).setSuperUsers(superUsers);
        });

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(oauthClusterName), 1, kafkaPods);

        LOGGER.info("Verifying that team B is able to write to topic starting with 'x-' and break authorization rule");

        resourceManager.createResource(extensionContext, teamBOauthClientJob.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(teamBProducerName, NAMESPACE, MESSAGE_COUNT);

        LOGGER.info("Verifying that team A is able to write to topic starting with 'x-' and break authorization rule");

        teamAOauthClientJob = teamAOauthClientJob.toBuilder()
            .withConsumerGroup("x-consumer_group_b2-" + clusterName)
            .withTopicName(topicXName)
            .build();

        resourceManager.createResource(extensionContext, teamAOauthClientJob.consumerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(teamAConsumerName, NAMESPACE, MESSAGE_COUNT);
    }

    /**
     * 1) Try to send messages to topic starting with `x-` with producer from Dev Team A
     * 2) Change the Oauth listener configuration -> add the maxSecondsWithoutReauthentication set to 30s
     * 3) Try to send messages with delay of 1000ms (in the meantime, the permissions configuration will be changed)
     * 4) Get all configuration from the Keycloak (realms, policies) and change the policy so the Dev Team A producer should not be able to send messages to the topic
     *      starting with `x-` -> updating the policy through the Keycloak API
     * 5) Wait for the WaitException to appear -> as the producer doesn't have permission for sending messages, the
     *      job will be in error state
     * 6) Try to send messages to topic with `a-` -> we should still be able to sent messages, because we didn't changed the permissions
     * 6) Change the permissions back and check that the messages are correctly sent
     *
     * The re-authentication can be seen in the log of team-a-producer pod.
     */
    @IsolatedTest("Modification of shared Kafka cluster")
    @Order(7)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testSessionReAuthentication(ExtensionContext extensionContext) {
        String topicXName = TOPIC_X + "-example-topic";
        String topicAName = TOPIC_A + "-example-topic";

        LOGGER.info("Verifying that team A producer is able to send messages to the {} topic -> the topic starting with 'x'", topicXName);

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicXName).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicAName).build());

        KafkaOauthExampleClients teamAOauthClientJob = new KafkaOauthExampleClients.Builder()
            .withProducerName(TEAM_A_PRODUCER_NAME)
            .withConsumerName(TEAM_A_CONSUMER_NAME)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(topicXName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroup("a-consumer_group")
            .withUserName(TEAM_A_CLIENT)
            .withOAuthClientId(TEAM_A_CLIENT)
            .withOAuthClientSecret(TEAM_A_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        resourceManager.createResource(extensionContext, teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(TEAM_A_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, TEAM_A_PRODUCER_NAME);

        LOGGER.info("Adding the maxSecondsWithoutReauthentication to Kafka listener with OAuth authentication");
        KafkaResource.replaceKafkaResource(oauthClusterName, kafka -> {
            kafka.getSpec().getKafka().setListeners(Arrays.asList(new GenericKafkaListenerBuilder()
                    .withName("tls")
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
                        .withMaxSecondsWithoutReauthentication(30)
                    .endKafkaListenerAuthenticationOAuth()
                .build()));
        });

        KafkaUtils.waitForKafkaReady(oauthClusterName);

        String baseUri = "https://" + keycloakInstance.getHttpsUri();

        LOGGER.info("Setting the master realm token's lifespan to 3600s");

        // get admin token for all operation on realms
        String userName =  new String(Base64.getDecoder().decode(kubeClient().getSecret("credential-example-keycloak").getData().get("ADMIN_USERNAME").getBytes()));
        String password = new String(Base64.getDecoder().decode(kubeClient().getSecret("credential-example-keycloak").getData().get("ADMIN_PASSWORD").getBytes()));
        String token = KeycloakUtils.getToken(baseUri, userName, password);

        // firstly we will increase token lifespan
        JsonObject masterRealm = KeycloakUtils.getKeycloakRealm(baseUri, token, "master");
        masterRealm.put("accessTokenLifespan", "3600");
        KeycloakUtils.putConfigurationToRealm(baseUri, token, masterRealm, "master");

        // now we need to get the token with new lifespan
        token = KeycloakUtils.getToken(baseUri, userName, password);

        LOGGER.info("Getting the {} kafka client for obtaining the Dev A Team policy for the x topics", TEST_REALM);
        // we need to get clients for kafka-authz realm to access auth policies in kafka client
        JsonArray kafkaAuthzRealm = KeycloakUtils.getKeycloakRealmClients(baseUri, token, TEST_REALM);

        String kafkaClientId = "";
        for (Object client : kafkaAuthzRealm) {
            JsonObject clientObject = new JsonObject(client.toString());
            if (clientObject.getString("clientId").equals("kafka")) {
                kafkaClientId = clientObject.getString("id");
            }
        }

        JsonArray kafkaAuthzRealmPolicies = KeycloakUtils.getPoliciesFromRealmClient(baseUri, token, TEST_REALM, kafkaClientId);

        JsonObject devAPolicy = new JsonObject();
        for (Object resource : kafkaAuthzRealmPolicies) {
            JsonObject resourceObject = new JsonObject(resource.toString());
            if (resourceObject.getValue("name").toString().contains("Dev Team A can write to topics that start with x- on any cluster")) {
                devAPolicy = resourceObject;
            }
        }

        JsonObject newDevAPolicy = devAPolicy;

        Map<String, String> config = new HashMap<>();
        config.put("resources", "[\"Topic:x-*\"]");
        config.put("scopes", "[\"Describe\"]");
        config.put("applyPolicies", "[\"Dev Team A\"]");

        newDevAPolicy.put("config", config);

        LOGGER.info("Changing the Dev Team A policy for topics starting with x- and checking that job will not be successful");
        KeycloakUtils.updatePolicyOfRealmClient(baseUri, token, newDevAPolicy, TEST_REALM, kafkaClientId);
        assertThrows(WaitException.class, () -> ClientUtils.waitForClientSuccess(TEAM_A_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT));

        JobUtils.deleteJobWithWait(NAMESPACE, TEAM_A_PRODUCER_NAME);

        LOGGER.info("Sending messages to topic starting with a- -> the messages should be successfully sent");

        teamAOauthClientJob = teamAOauthClientJob.toBuilder()
            .withTopicName(topicAName)
            .build();

        resourceManager.createResource(extensionContext, teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(TEAM_A_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, TEAM_A_PRODUCER_NAME);

        LOGGER.info("Changing back to the original settings and checking, if the producer will be successful");

        config.put("scopes", "[\"Describe\",\"Write\"]");
        newDevAPolicy.put("config", config);

        KeycloakUtils.updatePolicyOfRealmClient(baseUri, token, newDevAPolicy, TEST_REALM, kafkaClientId);
        teamAOauthClientJob = teamAOauthClientJob.toBuilder()
            .withTopicName(topicXName)
            .withDelayMs(1000)
            .build();

        resourceManager.createResource(extensionContext, teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName).build());
        ClientUtils.waitForClientSuccess(TEAM_A_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, TEAM_A_PRODUCER_NAME);

        LOGGER.info("Changing configuration of Kafka back to it's original form");
        KafkaResource.replaceKafkaResource(oauthClusterName, kafka -> {
            kafka.getSpec().getKafka().setListeners(Arrays.asList(new GenericKafkaListenerBuilder()
                    .withName("tls")
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
                .build()));
        });

        KafkaUtils.waitForKafkaReady(oauthClusterName);
    }

    @Disabled("Will be implemented in next PR")
    @ParallelTest
    @Order(8)
    void testListTopics(ExtensionContext extensionContext) {
        // TODO: in the new PR add AdminClient support with operations listTopics(), etc.
    }

    @Disabled("Will be implemented in next PR")
    @ParallelTest
    @Order(9)
    void testClusterVerification(ExtensionContext extensionContext) {
        // TODO: create more examples via cluster wide stuff
    }

    @BeforeAll
    void setUp(ExtensionContext extensionContext)  {
        super.beforeAllMayOverride();
        // for namespace
        super.setupCoAndKeycloak(extensionContext, NAMESPACE);

        keycloakInstance.setRealm(TEST_REALM, true);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(oauthClusterName, 1, 1)
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
                    .withNewKafkaAuthorizationKeycloak()
                        .withClientId(KAFKA_CLIENT_ID)
                        .withDisableTlsHostnameVerification(true)
                        .withDelegateToKafkaAcls(false)
                        // ca.crt a tls.crt
                        .withTlsTrustedCertificates(
                            new CertSecretSourceBuilder()
                                .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                .build()
                        )
                        .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .endKafkaAuthorizationKeycloak()
                .endKafka()
            .endSpec()
            .build());

        LOGGER.info("Setting producer and consumer properties");

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(oauthClusterName, TEAM_A_CLIENT).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(oauthClusterName, TEAM_B_CLIENT).build());
    }

    @AfterAll
    void tearDown(ExtensionContext extensionContext) throws Exception {
        // delete keycloak before namespace
        KeycloakUtils.deleteKeycloak(NAMESPACE);
        // delete namespace etc.
        super.afterAllMayOverride(extensionContext);
    }
}
