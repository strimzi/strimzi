/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectorBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.TestUtils;

public class KafkaConnectorTemplates {

    public static final String PATH_TO_KAFKA_CONNECTOR_CONFIG = Constants.PATH_TO_PACKAGING_EXAMPLES + "/connect/source-connector.yaml";

    private KafkaConnectorTemplates() {}

    public static KafkaConnectorBuilder kafkaConnector(String name) {
        return kafkaConnector(name, name, 2);
    }

    public static KafkaConnectorBuilder kafkaConnector(String name, String clusterName) {
        return kafkaConnector(name, clusterName, 2);
    }

    public static KafkaConnectorBuilder kafkaConnector(String name, String clusterName, int maxTasks) {
        KafkaConnector kafkaConnector = getKafkaConnectorFromYaml(PATH_TO_KAFKA_CONNECTOR_CONFIG);
        return defaultKafkaConnector(kafkaConnector, name, clusterName, maxTasks);
    }

    public static KafkaConnectorBuilder defaultKafkaConnector(String name, String clusterName, int maxTasks) {
        KafkaConnector kafkaConnector = getKafkaConnectorFromYaml(PATH_TO_KAFKA_CONNECTOR_CONFIG);
        return defaultKafkaConnector(kafkaConnector, name, clusterName, maxTasks);
    }

    public static KafkaConnectorBuilder defaultKafkaConnector(KafkaConnector kafkaConnector, String name, String kafkaConnectClusterName, int maxTasks) {
        return new KafkaConnectorBuilder(kafkaConnector)
            .editOrNewMetadata()
                .withName(name)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, kafkaConnectClusterName)
            .endMetadata()
            .editOrNewSpec()
                .withTasksMax(maxTasks)
            .endSpec();
    }

    private static KafkaConnector getKafkaConnectorFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaConnector.class);
    }
}
