/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.TestUtils;

public class KafkaTopicTemplates {

    private KafkaTopicTemplates() {}

    public static KafkaTopicBuilder topic(String clusterName, String topicName) {
        return defaultTopic(clusterName, topicName, 1, 1, 1, ResourceManager.kubeClient().getNamespace());
    }

    public static KafkaTopicBuilder topic(String clusterName, String topicName, String topicNamespace) {
        return defaultTopic(clusterName, topicName, 1, 1, 1, topicNamespace);
    }

    public static KafkaTopicBuilder topic(String clusterName, String topicName, int partitions) {
        return defaultTopic(clusterName, topicName, partitions, 1, 1, ResourceManager.kubeClient().getNamespace());
    }

    public static KafkaTopicBuilder topic(String clusterName, String topicName, int partitions, int replicas) {
        return defaultTopic(clusterName, topicName, partitions, replicas, replicas, ResourceManager.kubeClient().getNamespace());
    }

    public static KafkaTopicBuilder topic(String clusterName, String topicName, int partitions, int replicas, int minIsr) {
        return defaultTopic(clusterName, topicName, partitions, replicas, minIsr, ResourceManager.kubeClient().getNamespace());
    }

    public static KafkaTopicBuilder topic(String clusterName, String topicName, int partitions, int replicas, int minIsr, String topicNamespace) {
        return defaultTopic(clusterName, topicName, partitions, replicas, minIsr, topicNamespace);
    }

    public static KafkaTopicBuilder defaultTopic(String clusterName, String topicName, int partitions, int replicas, int minIsr, String topicNamespace) {
        KafkaTopic kafkaTopic = getKafkaTopicFromYaml(Constants.PATH_TO_KAFKA_TOPIC_CONFIG);
        return new KafkaTopicBuilder(kafkaTopic)
            .withNewMetadata()
                .withName(topicName)
                .withNamespace(topicNamespace)
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, clusterName)
            .endMetadata()
            .editSpec()
                .withPartitions(partitions)
                .withReplicas(replicas)
                .addToConfig("min.insync.replicas", minIsr)
            .endSpec();
    }

    private static KafkaTopic getKafkaTopicFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaTopic.class);
    }
}
