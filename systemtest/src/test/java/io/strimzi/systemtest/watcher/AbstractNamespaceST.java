/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.watcher;

import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMakerUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public abstract class AbstractNamespaceST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(AbstractNamespaceST.class);

    static final String CO_NAMESPACE = "co-namespace-test";
    static final String SECOND_NAMESPACE = "second-namespace-test";
    static final String MAIN_NAMESPACE_CLUSTER_NAME = "my-cluster";
    private static final String TOPIC_EXAMPLES_DIR = Constants.PATH_TO_PACKAGING_EXAMPLES + "/topic/kafka-topic.yaml";

    void checkKafkaInDiffNamespaceThanCO(String clusterName, String namespace) {
        String previousNamespace = cluster.setNamespace(namespace);
        LOGGER.info("Check if Kafka Cluster {} in namespace {}", clusterName, namespace);

        TestUtils.waitFor("Kafka Cluster status is not in desired state: Ready", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT, () -> {
            Condition kafkaCondition = KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get()
                    .getStatus().getConditions().get(0);
            LOGGER.info("Kafka condition status: {}", kafkaCondition.getStatus());
            LOGGER.info("Kafka condition type: {}", kafkaCondition.getType());
            return kafkaCondition.getType().equals(Ready.toString());
        });

        Condition kafkaCondition = KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get()
                .getStatus().getConditions().get(0);

        assertThat(kafkaCondition.getType(), is(Ready.toString()));
        cluster.setNamespace(previousNamespace);
    }

    void checkMirrorMakerForKafkaInDifNamespaceThanCO(ExtensionContext extensionContext, String sourceClusterName) {
        String kafkaSourceName = sourceClusterName;
        String kafkaTargetName = MAIN_NAMESPACE_CLUSTER_NAME + "-target";

        String previousNamespace = cluster.setNamespace(SECOND_NAMESPACE);
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaTargetName, 1, 1).build());
        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(MAIN_NAMESPACE_CLUSTER_NAME, kafkaSourceName, kafkaTargetName, "my-group", 1, false).build());

        LOGGER.info("Waiting for creation {} in namespace {}", MAIN_NAMESPACE_CLUSTER_NAME + "-mirror-maker", SECOND_NAMESPACE);
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(MAIN_NAMESPACE_CLUSTER_NAME);
        cluster.setNamespace(previousNamespace);
    }

    void deployKafkaConnectorWithSink(ExtensionContext extensionContext, String clusterName, String namespace, String topicName, String connectLabel, String sharedKafkaClusterName) {
        // Deploy Kafka Connector
        Map<String, Object> connectorConfig = new HashMap<>();
        connectorConfig.put("topics", topicName);
        connectorConfig.put("file", Constants.DEFAULT_SINK_FILE_PATH);
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .withConfig(connectorConfig)
            .endSpec()
            .build());
        KafkaConnectorUtils.waitForConnectorReady(clusterName);

        String kafkaConnectPodName = kubeClient().listPods(clusterName, Labels.STRIMZI_KIND_LABEL, connectLabel).get(0).getMetadata().getName();
        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(kafkaConnectPodName);

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, clusterName + "-" + Constants.KAFKA_CLIENTS).build());

        final String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(namespace)
            .withClusterName(sharedKafkaClusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        int sent = internalKafkaClient.sendMessagesPlain();
        assertThat(sent, is(MESSAGE_COUNT));

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");
    }
}
