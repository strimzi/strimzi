/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.clientproperties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Locale;

public class ConsumerProperties extends AbstractKafkaClientProperties<ConsumerProperties> {

    public static class ConsumerPropertiesBuilder extends AbstractKafkaClientProperties.KafkaClientPropertiesBuilder<ConsumerPropertiesBuilder> {

        public ConsumerPropertiesBuilder withBootstrapServerConfig(String bootstrapServer) {

            this.properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            return this;
        }

        public ConsumerPropertiesBuilder withKeyDeserializerConfig(Class<? extends Deserializer> keyDeSerializer) {

            this.properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeSerializer.getName());
            return this;
        }

        public ConsumerPropertiesBuilder withValueDeserializerConfig(Class<? extends Deserializer> valueDeSerializer) {

            this.properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeSerializer.getName());
            return this;
        }

        public ConsumerPropertiesBuilder withGroupIdConfig(String groupIdConfig) {

            this.properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupIdConfig);
            return this;
        }

        public ConsumerPropertiesBuilder withClientIdConfig(String clientId) {

            this.properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
            return this;
        }

        public ConsumerPropertiesBuilder withAutoOffsetResetConfig(OffsetResetStrategy offsetResetConfig) {

            this.properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetConfig.name().toLowerCase(Locale.ENGLISH));
            return this;
        }

        @Override
        public ConsumerProperties build() {
            return new ConsumerProperties(this);
        }

        @Override
        protected ConsumerPropertiesBuilder self() {
            return this;
        }
    }

    private ConsumerProperties(ConsumerPropertiesBuilder builder) {
        super(builder);
        properties = builder.properties;
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public ConsumerProperties.ConsumerPropertiesBuilder toBuilder(ConsumerProperties clientProperties) {
        ConsumerPropertiesBuilder builder = new ConsumerProperties.ConsumerPropertiesBuilder();

        builder.withNamespaceName(clientProperties.getNamespaceName());
        builder.withClusterName(clientProperties.getClusterName());
        builder.withSecurityProtocol(SecurityProtocol.forName(clientProperties.getProperties().getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)));
        builder.withBootstrapServerConfig(clientProperties.getProperties().getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        builder.withGroupIdConfig(clientProperties.getProperties().getProperty(ConsumerConfig.GROUP_ID_CONFIG));

        try {
            builder.withKeyDeserializerConfig((Class<? extends Deserializer>) Class.forName(clientProperties.getProperties().getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)));
            builder.withValueDeserializerConfig((Class<? extends Deserializer>) Class.forName(clientProperties.getProperties().getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        builder.withClientIdConfig(clientProperties.getProperties().getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
        builder.withAutoOffsetResetConfig(OffsetResetStrategy.valueOf(clientProperties.getProperties().getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).toUpperCase(Locale.ENGLISH)));
        builder.withSharedProperties();

        return builder;
    }
}