/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cli;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;

import java.util.Arrays;
import java.util.List;

import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

public class KafkaCmdClient {

    private static final int PORT = 9092;


    public static List<String> listTopicsUsingPodCli(String namespaceName, String clusterName, int kafkaPodId) {
        String podName = KafkaResources.kafkaPodName(clusterName, kafkaPodId);
        return Arrays.asList(cmdKubeClient(namespaceName).execInPod(podName, "/bin/bash", "-c",
            "bin/kafka-topics.sh --list --bootstrap-server localhost:" + PORT).out().split("\\s+"));
    }

    public static List<String> listTopicsUsingPodCli(String clusterName, int kafkaPodId) {
        return listTopicsUsingPodCli(kubeClient().getNamespace(), clusterName, kafkaPodId);
    }

    public static String createTopicUsingPodCli(String namespaceName, String clusterName, int kafkaPodId, String topic, int replicationFactor, int partitions) {
        String podName = KafkaResources.kafkaPodName(clusterName, kafkaPodId);
        String response = cmdKubeClient(namespaceName).execInPod(podName, "/bin/bash", "-c",
            "bin/kafka-topics.sh --bootstrap-server localhost:" + PORT + " --create " + " --topic " + topic +
                " --replication-factor " + replicationFactor + " --partitions " + partitions).out();

        KafkaTopicUtils.waitForKafkaTopicCreation(namespaceName, topic);

        return response;
    }

    public static String createTopicUsingPodCli(String clusterName, int kafkaPodId, String topic, int replicationFactor, int partitions) {
        return createTopicUsingPodCli(kubeClient().getNamespace(), clusterName, kafkaPodId, topic, replicationFactor, partitions);
    }

    public static String deleteTopicUsingPodCli(String namespaceName, String clusterName, int kafkaPodId, String topic) {
        String podName = KafkaResources.kafkaPodName(clusterName, kafkaPodId);
        return cmdKubeClient(namespaceName).execInPod(podName, "/bin/bash", "-c",
            "bin/kafka-topics.sh --bootstrap-server localhost:" + PORT + " --delete --topic " + topic).out();
    }

    public static List<String> describeTopicUsingPodCli(String clusterName, int kafkaPodId, String topic) {
        return describeTopicUsingPodCli(kubeClient().getNamespace(), clusterName, kafkaPodId, topic);
    }

    public static List<String> describeTopicUsingPodCli(String namespaceName, String clusterName, int kafkaPodId, String topic) {
        String podName = KafkaResources.kafkaPodName(clusterName, kafkaPodId);
        return Arrays.asList(cmdKubeClient(namespaceName).execInPod(podName, "/bin/bash", "-c",
            "bin/kafka-topics.sh --bootstrap-server localhost:" + PORT + " --describe --topic " + topic).out().replace(": ", ":").split("\\s+"));
    }

    public static String updateTopicPartitionsCountUsingPodCli(String namespaceName, String clusterName, int kafkaPodId, String topic, int partitions) {
        String podName = KafkaResources.kafkaPodName(clusterName, kafkaPodId);
        return cmdKubeClient(namespaceName).execInPod(podName, "/bin/bash", "-c",
            "bin/kafka-topics.sh --bootstrap-server localhost:" + PORT + " --alter --topic " + topic + " --partitions " + partitions).out();
    }

    public static String updateTopicPartitionsCountUsingPodCli(String clusterName, int kafkaPodId, String topic, int partitions) {
        return updateTopicPartitionsCountUsingPodCli(kubeClient().getNamespace(), clusterName, kafkaPodId, topic, partitions);
    }
}
