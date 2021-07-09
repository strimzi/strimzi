/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.externalClients;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.AbstractKafkaClient;
import io.strimzi.systemtest.kafkaclients.KafkaClientOperations;
import io.strimzi.systemtest.kafkaclients.clientproperties.ConsumerProperties;
import io.strimzi.systemtest.kafkaclients.clientproperties.ProducerProperties;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.test.WaitException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntPredicate;

/**
 * The OauthExternalKafkaClient for sending and receiving messages using access token provided by authorization server.
 * The client is using an external listeners.
 */
public class OauthExternalKafkaClient extends AbstractKafkaClient<OauthExternalKafkaClient.Builder> implements KafkaClientOperations {

    private static final Logger LOGGER = LogManager.getLogger(OauthExternalKafkaClient.class);

    private String clientId;
    private String clientSecretName;
    private String oauthTokenEndpointUri;
    private String introspectionEndpointUri;

    public static class Builder extends AbstractKafkaClient.Builder<Builder> {

        private String clientId;
        private String clientSecretName;
        private String oauthTokenEndpointUri;
        private String introspectionEndpointUri;

        public Builder withOauthClientId(String oauthClientId) {

            this.clientId = oauthClientId;
            return this;
        }

        public Builder withClientSecretName(String clientSecretName) {

            this.clientSecretName = clientSecretName;
            return this;
        }

        public Builder withOauthTokenEndpointUri(String oauthTokenEndpointUri) {

            this.oauthTokenEndpointUri = oauthTokenEndpointUri;
            return this;
        }

        public Builder withIntrospectionEndpointUri(String introspectionEndpointUri) {

            this.introspectionEndpointUri = introspectionEndpointUri;
            return this;
        }

        @Override
        public OauthExternalKafkaClient build() {
            return new OauthExternalKafkaClient(this);
        }
    }

    @Override
    protected Builder newBuilder() {
        return new Builder();
    }

    @Override
    public Builder toBuilder() {
        return ((Builder) super.toBuilder())
            .withOauthClientId(clientId)
            .withClientSecretName(clientSecretName)
            .withOauthTokenEndpointUri(oauthTokenEndpointUri)
            .withIntrospectionEndpointUri(introspectionEndpointUri);
    }

    private OauthExternalKafkaClient(Builder builder) {

        super(builder);
        clientId = builder.clientId;
        clientSecretName = builder.clientSecretName;
        oauthTokenEndpointUri = builder.oauthTokenEndpointUri;
        introspectionEndpointUri = builder.introspectionEndpointUri;
    }

    public int sendMessagesPlain() {
        return sendMessagesPlain(Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    @Override
    public int sendMessagesPlain(long timeoutMs) {
        String clientName = "sender-plain-" + new Random().nextInt(Integer.MAX_VALUE);
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        IntPredicate msgCntPredicate = x -> x == messageCount;

        ProducerProperties properties = this.producerProperties;

        if (properties == null || properties.getProperties().isEmpty()) {
            properties = new ProducerProperties.ProducerPropertiesBuilder()
                .withNamespaceName(namespaceName)
                .withClusterName(clusterName)
                .withSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT)
                .withBootstrapServerConfig(getBootstrapServerFromStatus())
                .withKeySerializerConfig(StringSerializer.class)
                .withValueSerializerConfig(StringSerializer.class)
                .withClientIdConfig(kafkaUsername + "-producer")
                .withSaslMechanism(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)
                .withSaslLoginCallbackHandlerClass()
                .withSharedProperties()
                .withSaslJassConfig(this.clientId, this.clientSecretName, this.oauthTokenEndpointUri)
                .build();
        }

        try (Producer plainProducer = new Producer(properties, resultPromise, msgCntPredicate, topicName, clientName, partition)) {

            plainProducer.getVertx().deployVerticle(plainProducer);

            return plainProducer.getResultPromise().get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw new WaitException(e);
        }
    }

    public int sendMessagesTls() {
        return sendMessagesTls(Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    @Override
    public int sendMessagesTls(long timeoutMs) {
        String clientName = "sender-ssl" + new Random().nextInt(Integer.MAX_VALUE);
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        IntPredicate msgCntPredicate = x -> x == messageCount;

        this.caCertName = this.caCertName == null ?
            KafkaUtils.getKafkaExternalListenerCaCertName(namespaceName, clusterName, listenerName) :
            this.caCertName;

        LOGGER.info("Going to use the following CA certificate: {}", caCertName);

        ProducerProperties properties = this.producerProperties;

        LOGGER.info("This is client.id={}, client.secret.name={}, oauthTokenEndpointUri={}", clientId, clientSecretName, oauthTokenEndpointUri);

        if (properties == null || properties.getProperties().isEmpty()) {
            properties = new ProducerProperties.ProducerPropertiesBuilder()
                .withNamespaceName(namespaceName)
                .withClusterName(clusterName)
                .withBootstrapServerConfig(getBootstrapServerFromStatus())
                .withKeySerializerConfig(StringSerializer.class)
                .withValueSerializerConfig(StringSerializer.class)
                .withCaSecretName(caCertName)
                .withKafkaUsername(kafkaUsername)
                .withSecurityProtocol(SecurityProtocol.SASL_SSL)
                .withClientIdConfig(kafkaUsername + "-producer")
                .withSaslMechanism(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)
                .withSaslLoginCallbackHandlerClass()
                .withSharedProperties()
                .withSaslJassConfigAndTls(clientId, clientSecretName, oauthTokenEndpointUri)
                .build();
        }

        try (Producer tlsProducer = new Producer(properties, resultPromise, msgCntPredicate, topicName, clientName, partition)) {

            tlsProducer.getVertx().deployVerticle(tlsProducer);

            return tlsProducer.getResultPromise().get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw new WaitException(e);
        }
    }

    public int receiveMessagesPlain() {
        return receiveMessagesPlain(Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    @Override
    public int receiveMessagesPlain(long timeoutMs) {
        String clientName = "receiver-plain-" + new Random().nextInt(Integer.MAX_VALUE);
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        IntPredicate msgCntPredicate = x -> x == messageCount;

        ConsumerProperties properties = this.consumerProperties;

        if (properties == null || properties.getProperties().isEmpty()) {
            properties = new ConsumerProperties.ConsumerPropertiesBuilder()
                .withNamespaceName(namespaceName)
                .withClusterName(clusterName)
                .withGroupIdConfig(consumerGroup)
                .withSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT)
                .withBootstrapServerConfig(getBootstrapServerFromStatus())
                .withKeyDeserializerConfig(StringDeserializer.class)
                .withValueDeserializerConfig(StringDeserializer.class)
                .withClientIdConfig(kafkaUsername + "-consumer")
                .withAutoOffsetResetConfig(OffsetResetStrategy.EARLIEST)
                .withSaslMechanism(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)
                .withSaslLoginCallbackHandlerClass()
                .withSharedProperties()
                .withSaslJassConfig(this.clientId, this.clientSecretName, this.oauthTokenEndpointUri)
                .build();
        }

        try (Consumer plainConsumer = new Consumer(properties, resultPromise, msgCntPredicate, topicName, clientName)) {

            plainConsumer.getVertx().deployVerticle(plainConsumer);

            return plainConsumer.getResultPromise().get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw new WaitException(e);
        }
    }

    public int receiveMessagesTls() {
        return receiveMessagesTls(Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    @Override
    public int receiveMessagesTls(long timeoutMs) {

        String clientName = "receiver-ssl-" + new Random().nextInt(Integer.MAX_VALUE);
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        IntPredicate msgCntPredicate = x -> x == messageCount;

        this.caCertName = this.caCertName == null ?
            KafkaUtils.getKafkaExternalListenerCaCertName(namespaceName, clusterName, listenerName) :
            this.caCertName;

        LOGGER.info("Going to use the following CA certificate: {}", caCertName);

        ConsumerProperties properties = this.consumerProperties;

        if (properties == null || properties.getProperties().isEmpty()) {
            properties = new ConsumerProperties.ConsumerPropertiesBuilder()
                .withNamespaceName(namespaceName)
                .withClusterName(clusterName)
                .withCaSecretName(caCertName)
                .withBootstrapServerConfig(getBootstrapServerFromStatus())
                .withKeyDeserializerConfig(StringDeserializer.class)
                .withValueDeserializerConfig(StringDeserializer.class)
                .withKafkaUsername(kafkaUsername)
                .withSecurityProtocol(SecurityProtocol.SASL_SSL)
                .withGroupIdConfig(consumerGroup)
                .withAutoOffsetResetConfig(OffsetResetStrategy.EARLIEST)
                .withClientIdConfig(kafkaUsername + "-consumer")
                .withSaslMechanism(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)
                .withSaslLoginCallbackHandlerClass()
                .withSharedProperties()
                .withSaslJassConfigAndTls(this.clientId, this.clientSecretName, this.oauthTokenEndpointUri)
                .build();
        }

        try (Consumer tlsConsumer = new Consumer(properties, resultPromise, msgCntPredicate, topicName, clientName)) {

            tlsConsumer.getVertx().deployVerticle(tlsConsumer);

            return tlsConsumer.getResultPromise().get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw new WaitException(e);
        }
    }

    @Override
    public String toString() {

        return "OauthKafkaClient{" +
                "oauthTokenEndpointUri='" + oauthTokenEndpointUri + '\'' +
                ", introspectionEndpointUri='" + introspectionEndpointUri + '\'' +
                ", topicName='" + topicName + '\'' +
                ", namespaceName='" + namespaceName + '\'' +
                ", clusterName='" + clusterName + '\'' +
                ", messageCount=" + messageCount +
                ", consumerGroup='" + consumerGroup + '\'' +
                ", kafkaUsername='" + kafkaUsername + '\'' +
                ", securityProtocol='" + securityProtocol + '\'' +
                ", caCertName='" + caCertName + '\'' +
                '}';
    }
}
