/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;

import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaTopicUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaTopicUtils.class);
    private static final String TOPIC_NAME_PREFIX = "my-topic-";
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(KafkaTopic.RESOURCE_KIND);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private KafkaTopicUtils() {}

    /**
     * Generated random name for the KafkaTopic resource
     * @return random name with additional salt
     */
    public static String generateRandomNameOfTopic() {
        String salt = new Random().nextInt(Integer.MAX_VALUE) + "-" + new Random().nextInt(Integer.MAX_VALUE);

        return  TOPIC_NAME_PREFIX + salt;
    }

    /**
     * Method which return UID for specific topic
     * @param topicName topic name
     * @return topic UID
     */
    public static String topicSnapshot(String topicName) {
        return KafkaTopicResource.kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(topicName).get().getMetadata().getUid();
    }

    /**
     * Method which wait until topic has rolled form one generation to another.
     * @param topicName topic name
     * @param topicUid topic UID
     * @return topic new UID
     */
    public static String waitTopicHasRolled(String topicName, String topicUid) {
        TestUtils.waitFor("Topic " + topicName + " has rolled", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> !topicUid.equals(topicSnapshot(topicName)));
        return topicSnapshot(topicName);
    }

    public static void waitForKafkaTopicCreation(String namespaceName, String topicName) {
        LOGGER.info("Waiting for KafkaTopic {} creation ", topicName);
        TestUtils.waitFor("KafkaTopic creation " + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName)
                    .withName(topicName).get().getStatus().getConditions().get(0).getType().equals(Ready.toString()),
            () -> LOGGER.info(KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get())
        );
    }

    public static void waitForKafkaTopicCreation(String topicName) {
        waitForKafkaTopicCreation(kubeClient().getNamespace(), topicName);
    }

    public static void waitForKafkaTopicCreationByNamePrefix(String namespaceName, String topicNamePrefix) {
        LOGGER.info("Waiting for KafkaTopic {} creation", topicNamePrefix);
        TestUtils.waitFor("KafkaTopic creation " + topicNamePrefix, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).list().getItems().stream()
                    .filter(topic -> topic.getMetadata().getName().contains(topicNamePrefix))
                    .findFirst().orElseThrow().getStatus().getConditions().get(0).getType().equals(Ready.toString())
        );
    }

    public static void waitForKafkaTopicCreationByNamePrefix(String topicNamePrefix) {
        waitForKafkaTopicCreationByNamePrefix(kubeClient().getNamespace(), topicNamePrefix);
    }

    public static void waitForKafkaTopicDeletion(String namespaceName, String topicName) {
        LOGGER.info("Waiting for KafkaTopic {} deletion", topicName);
        TestUtils.waitFor("KafkaTopic deletion " + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, DELETION_TIMEOUT,
            () -> {
                if (KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get() == null) {
                    return true;
                } else {
                    LOGGER.warn("KafkaTopic {} is not deleted yet! Triggering force delete by cmd client!", topicName);
                    cmdKubeClient(namespaceName).deleteByName(KafkaTopic.RESOURCE_KIND, topicName);
                    return false;
                }
            },
            () -> LOGGER.info(KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get())
        );
    }

    public static void waitForKafkaTopicDeletion(String topicName) {
        waitForKafkaTopicDeletion(kubeClient().getNamespace(), topicName);
    }

    public static void waitForKafkaTopicPartitionChange(String namespaceName, String topicName, int partitions) {
        LOGGER.info("Waiting for KafkaTopic change {}", topicName);
        TestUtils.waitFor("KafkaTopic change " + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.GLOBAL_TIMEOUT,
            () -> KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get().getSpec().getPartitions() == partitions,
            () -> LOGGER.error("Kafka Topic {} did not change partition", KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get())
        );
    }

    public static void waitForKafkaTopicReplicasChange(String namespaceName, String topicName, int replicas) {
        LOGGER.info("Waiting for KafkaTopic change {}", topicName);
        TestUtils.waitFor("KafkaTopic change " + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.GLOBAL_TIMEOUT,
            () -> KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get().getSpec().getReplicas() == replicas,
            () -> LOGGER.error("Kafka Topic {} did not change replicas", KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get())
        );
    }

    public static void waitForKafkaTopicPartitionChange(String topicName, int partitions) {
        waitForKafkaTopicPartitionChange(kubeClient().getNamespace(), topicName, partitions);
    }

    /**
     * Wait until KafkaTopic is in desired status
     * @param namespaceName Namespace name
     * @param topicName name of KafkaTopic
     * @param state desired state
     */
    public static boolean waitForKafkaTopicStatus(String namespaceName, String topicName, Enum<?> state) {
        return ResourceManager.waitForResourceStatus(KafkaTopicResource.kafkaTopicClient(), KafkaTopic.RESOURCE_KIND,
            namespaceName, topicName, state, ResourceOperation.getTimeoutForResourceReadiness(KafkaTopic.RESOURCE_KIND));
    }

    public static boolean waitForKafkaTopicReady(String namespaceName, String topicName) {
        return waitForKafkaTopicStatus(namespaceName, topicName, Ready);
    }

    public static boolean waitForKafkaTopicReady(String topicName) {
        return waitForKafkaTopicStatus(kubeClient().getNamespace(), topicName, Ready);
    }

    public static boolean waitForKafkaTopicNotReady(String topicName) {
        return waitForKafkaTopicStatus(kubeClient().getNamespace(), topicName, NotReady);
    }

    public static void waitForKafkaTopicsCount(int topicCount, String clusterName) {
        LOGGER.info("Wait until we create {} KafkaTopics", topicCount);
        TestUtils.waitFor(topicCount + " KafkaTopics creation",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> KafkaCmdClient.listTopicsUsingPodCli(clusterName, 0).size() == topicCount);
        LOGGER.info("{} KafkaTopics were created", topicCount);
    }

    public static String describeTopicViaKafkaPod(String topicName, String kafkaPodName, String bootstrapServer) {
        return cmdKubeClient().execInPod(kafkaPodName, "/opt/kafka/bin/kafka-topics.sh",
            "--topic",
            topicName,
            "--describe",
            "--bootstrap-server",
            bootstrapServer)
            .out();
    }

    public static void waitForKafkaTopicSpecStability(String topicName, String podName, String bootstrapServer) {
        int[] stableCounter = {0};

        String oldSpec = describeTopicViaKafkaPod(topicName, podName, bootstrapServer);

        TestUtils.waitFor("KafkaTopic's spec will be stable", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT, () -> {
            if (oldSpec.equals(describeTopicViaKafkaPod(topicName, podName, bootstrapServer))) {
                stableCounter[0]++;
                if (stableCounter[0] == Constants.GLOBAL_STABILITY_OFFSET_COUNT) {
                    LOGGER.info("KafkaTopic's spec is stable for {} polls intervals", stableCounter[0]);
                    return true;
                }
            } else {
                LOGGER.info("KafkaTopic's spec is not stable. Going to set the counter to zero.");
                stableCounter[0] = 0;
                return false;
            }
            LOGGER.info("KafkaTopic's spec gonna be stable in {} polls", Constants.GLOBAL_STABILITY_OFFSET_COUNT - stableCounter[0]);
            return false;
        });
    }
}
