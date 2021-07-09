/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients;

import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.systemtest.kafkaclients.clientproperties.ConsumerProperties;
import io.strimzi.systemtest.kafkaclients.clientproperties.ProducerProperties;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.ClientUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.InvalidParameterException;
import java.util.List;

public abstract class AbstractKafkaClient<C extends AbstractKafkaClient.Builder<C>> {

    private static final Logger LOGGER = LogManager.getLogger(AbstractKafkaClient.class);

    protected String topicName;
    protected Integer partition;
    protected String namespaceName;
    protected String clusterName;
    protected int messageCount;
    protected String consumerGroup;
    protected String kafkaUsername;
    protected SecurityProtocol securityProtocol;
    protected String caCertName;
    protected String listenerName;
    protected ProducerProperties producerProperties;
    protected ConsumerProperties consumerProperties;
    protected String secretPrefix;

    public abstract static class Builder<SELF extends Builder<SELF>> {

        private String topicName;
        protected Integer partition;
        private String namespaceName;
        private String clusterName;
        private int messageCount;
        private String consumerGroup;
        protected String kafkaUsername;
        protected SecurityProtocol securityProtocol;
        protected String caCertName;
        protected String listenerName;
        private ProducerProperties producerProperties;
        private ConsumerProperties consumerProperties;
        private String secretPrefix;

        public SELF withTopicName(String topicName) {
            this.topicName = topicName;
            return self();
        }

        public SELF withPartition(Integer partition) {
            this.partition = partition;
            return self();
        }

        public SELF withNamespaceName(String namespaceName) {
            this.namespaceName = namespaceName;
            return self();
        }

        public SELF withClusterName(String clusterName) {
            this.clusterName = clusterName;
            return self();
        }

        public SELF withMessageCount(int messageCount) {
            this.messageCount = messageCount;
            return self();
        }

        public SELF withConsumerGroupName(String consumerGroup) {
            this.consumerGroup = consumerGroup;
            return self();
        }

        public SELF withKafkaUsername(String kafkaUsername) {
            this.kafkaUsername = kafkaUsername;
            return self();
        }

        public SELF withSecurityProtocol(SecurityProtocol securityProtocol) {
            this.securityProtocol = securityProtocol;
            return self();
        }

        public SELF withCertificateAuthorityCertificateName(String caCertName) {
            this.caCertName = caCertName;
            return self();
        }

        public SELF withListenerName(String listenerName) {
            this.listenerName = listenerName;
            return self();
        }

        public SELF withProducerProperties(ProducerProperties producerProperties) {
            this.producerProperties = producerProperties;
            return self();
        }

        public SELF withConsumerProperties(ConsumerProperties consumerProperties) {
            this.consumerProperties =  consumerProperties;
            return self();
        }

        public SELF withSecretPrefix(String secretPrefix) {
            this.secretPrefix = secretPrefix;
            return self();
        }

        @SuppressWarnings("unchecked")
        protected SELF self() {
            return (SELF) this;
        }

        public AbstractKafkaClient<?> build() throws InstantiationException {
            // ensure that build() can be only invoked in sub-classes
            throw new InstantiationException();
        };
    }

    protected abstract C newBuilder();

    protected AbstractKafkaClient.Builder<C> toBuilder() {
        verifyEssentialInstanceAttributes();

        return newBuilder()
            .withTopicName(topicName)
            .withPartition(partition)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withMessageCount(messageCount)
            .withConsumerGroupName(consumerGroup)
            .withKafkaUsername(kafkaUsername)
            .withSecurityProtocol(securityProtocol)
            .withCertificateAuthorityCertificateName(caCertName)
            .withListenerName(listenerName)
            .withProducerProperties(producerProperties)
            .withConsumerProperties(consumerProperties)
            .withSecretPrefix(secretPrefix);
    }

    protected AbstractKafkaClient(Builder<?> builder) {
        verifyEssentialAttributes(builder);

        topicName = builder.topicName;
        partition = builder.partition;
        namespaceName = builder.namespaceName;
        clusterName = builder.clusterName;
        messageCount = builder.messageCount;
        consumerGroup = builder.consumerGroup;
        kafkaUsername = builder.kafkaUsername;
        securityProtocol = builder.securityProtocol;
        caCertName = builder.caCertName;
        listenerName = builder.listenerName;
        producerProperties = builder.producerProperties;
        consumerProperties = builder.consumerProperties;
        secretPrefix = builder.secretPrefix;
    }

    private void verifyEssentialInstanceAttributes() {
        if (topicName == null || topicName.isEmpty()) throw new InvalidParameterException("Topic name is not set.");
        if (namespaceName == null || namespaceName.isEmpty()) throw new InvalidParameterException("Namespace name is not set.");
        if (clusterName == null  || clusterName.isEmpty()) throw  new InvalidParameterException("Cluster name is not set.");
        if (messageCount <= 0) throw  new InvalidParameterException("Message count is less than 1");
        if (listenerName == null || listenerName.isEmpty()) throw new InvalidParameterException("Listener name is not set.");

        if (consumerGroup == null || consumerGroup.isEmpty()) {
            LOGGER.info("Consumer group were not specified going to create the random one.");
            consumerGroup = ClientUtils.generateRandomConsumerGroup();
        }
    }

    private void verifyEssentialAttributes(Builder<?> builder) {
        if (builder.topicName == null || builder.topicName.isEmpty()) throw new InvalidParameterException("Topic name is not set.");
        if (builder.namespaceName == null || builder.namespaceName.isEmpty()) throw new InvalidParameterException("Namespace name is not set.");
        if (builder.clusterName == null  || builder.clusterName.isEmpty()) throw  new InvalidParameterException("Cluster name is not set.");
        if (builder.messageCount <= 0) throw  new InvalidParameterException("Message count is less than 1");
        if (builder.listenerName == null || builder.listenerName.isEmpty()) throw new InvalidParameterException("Listener name is not set.");
        if (builder.consumerGroup == null || builder.consumerGroup.isEmpty()) {
            LOGGER.info("Consumer group were not specified going to create the random one.");
            builder.consumerGroup = ClientUtils.generateRandomConsumerGroup();
        }
    }

    public void setMessageCount(int messageCount) {
        this.messageCount = messageCount;
    }

    public void verifyProducedAndConsumedMessages(int producedMessages, int consumedMessages) {
        if (producedMessages != consumedMessages) {
            LOGGER.info("Producer produced {} messages", producedMessages);
            LOGGER.info("Consumer consumed {} messages", consumedMessages);
            throw new RuntimeException("Producer or consumer does not produce or consume required message");
        }
    }

    public String getBootstrapServerFromStatus() {

        List<ListenerStatus> listenerStatusList = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getListeners();

        if (listenerStatusList == null || listenerStatusList.size() < 1) {
            LOGGER.error("There is no Kafka external listener specified in the Kafka CR Status");
            throw new RuntimeException("There is no Kafka external listener specified in the Kafka CR Status");
        } else if (listenerName == null) {
            LOGGER.info("Listener name is not specified. Picking the first one from the Kafka Status.");
            return listenerStatusList.get(0).getBootstrapServers();
        }

        return listenerStatusList.stream().filter(listener -> listener.getType().equals(listenerName))
            .findFirst()
            .orElseThrow(RuntimeException::new)
            .getBootstrapServers();
    }

    public String getTopicName() {
        return topicName;
    }
    public int getMessageCount() {
        return messageCount;
    }
    public String getKafkaUsername() {
        return kafkaUsername;
    }
    public String getClusterName() {
        return clusterName;
    }
    public String getNamespaceName() {
        return namespaceName;
    }
    @Override
    public String toString() {
        return "AbstractKafkaClient{" +
            "topicName='" + topicName + '\'' +
            ", partition=" + partition +
            ", namespaceName='" + namespaceName + '\'' +
            ", clusterName='" + clusterName + '\'' +
            ", messageCount=" + messageCount +
            ", consumerGroup='" + consumerGroup + '\'' +
            ", kafkaUsername='" + kafkaUsername + '\'' +
            ", securityProtocol=" + securityProtocol +
            ", caCertName='" + caCertName + '\'' +
            ", listenerName='" + listenerName + '\'' +
            ", producerProperties=" + producerProperties +
            ", consumerProperties=" + consumerProperties +
            ", secretPrefix=" + secretPrefix +
            '}';
    }
}
