/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;

import java.io.File;
import java.util.Collections;

import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

public class KafkaTemplates {

    private KafkaTemplates() {};

    public static MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaClient() {
        return Crds.kafkaOperation(ResourceManager.kubeClient().getClient());
    }

    public static KafkaBuilder kafkaEphemeral(String name, int kafkaReplicas) {
        return kafkaEphemeral(name, kafkaReplicas, Math.min(kafkaReplicas, 3));
    }

    public static KafkaBuilder kafkaEphemeral(String name, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(Constants.PATH_TO_KAFKA_EPHEMERAL_CONFIG);
        return defaultKafka(kafka, name, kafkaReplicas, zookeeperReplicas);
    }

    public static KafkaBuilder kafkaPersistent(String name, int kafkaReplicas) {
        return kafkaPersistent(name, kafkaReplicas, Math.min(kafkaReplicas, 3));
    }

    public static KafkaBuilder kafkaPersistent(String name, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(Constants.PATH_TO_KAFKA_PERSISTENT_CONFIG);
        return defaultKafka(kafka, name, kafkaReplicas, zookeeperReplicas)
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withSize("100")
                        .withDeleteClaim(true)
                    .endPersistentClaimStorage()
                .endKafka()
                .editZookeeper()
                    .withNewPersistentClaimStorage()
                        .withSize("100")
                        .withDeleteClaim(true)
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec();
    }

    public static KafkaBuilder kafkaJBOD(String name, int kafkaReplicas, JbodStorage jbodStorage) {
        return kafkaJBOD(name, kafkaReplicas, 3, jbodStorage);
    }

    public static KafkaBuilder kafkaJBOD(String name, int kafkaReplicas, int zookeeperReplicas, JbodStorage jbodStorage) {
        Kafka kafka = getKafkaFromYaml(Constants.PATH_TO_KAFKA_PERSISTENT_CONFIG);
        return defaultKafka(kafka, name, kafkaReplicas, zookeeperReplicas)
            .editSpec()
                .editKafka()
                    .withStorage(jbodStorage)
                .endKafka()
                .editZookeeper().
                    withReplicas(zookeeperReplicas)
                .endZookeeper()
            .endSpec();
    }

    public static KafkaBuilder kafkaWithMetrics(String name, int kafkaReplicas, int zookeeperReplicas) {
        return kafkaWithMetrics(name, kubeClient().getNamespace(), kafkaReplicas, zookeeperReplicas);
    }

    public static KafkaBuilder kafkaWithMetrics(String name, String namespace, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(Constants.PATH_TO_KAFKA_METRICS_CONFIG);

        ConfigMap metricsCm = TestUtils.configMapFromYaml(Constants.PATH_TO_KAFKA_METRICS_CONFIG, "kafka-metrics");
        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(namespace).createOrReplace(metricsCm);
        return defaultKafka(kafka, name, kafkaReplicas, zookeeperReplicas)
            .editOrNewMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .editSpec()
                .withNewKafkaExporter()
                .endKafkaExporter()
            .endSpec();
    }

    public static KafkaBuilder kafkaWithCruiseControl(String name, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(Constants.PATH_TO_KAFKA_CRUISE_CONTROL_CONFIG);
        return defaultKafka(kafka, name, kafkaReplicas, zookeeperReplicas);
    }

    public static KafkaBuilder kafkaAndCruiseControlWithMetrics(String name, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(Constants.PATH_TO_KAFKA_CRUISE_CONTROL_METRICS_CONFIG);
        ConfigMap kafkaMetricsCm = TestUtils.configMapFromYaml(Constants.PATH_TO_KAFKA_METRICS_CONFIG, "kafka-metrics");
        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(kubeClient().getNamespace()).createOrReplace(kafkaMetricsCm);
        ConfigMap zkMetricsCm = TestUtils.configMapFromYaml(Constants.PATH_TO_KAFKA_METRICS_CONFIG, "kafka-metrics");
        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(kubeClient().getNamespace()).createOrReplace(zkMetricsCm);
        ConfigMap ccMetricsCm = TestUtils.configMapFromYaml(Constants.PATH_TO_KAFKA_CRUISE_CONTROL_CONFIG, "cruise-control-metrics");
        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(kubeClient().getNamespace()).createOrReplace(ccMetricsCm);
        return defaultKafka(kafka, name, kafkaReplicas, zookeeperReplicas);
    }

    public static KafkaBuilder kafkaWithMetricsAndCruiseControlWithMetrics(String name, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(Constants.PATH_TO_KAFKA_METRICS_CONFIG);
        ConfigMap kafkaMetricsCm = TestUtils.configMapFromYaml(Constants.PATH_TO_KAFKA_METRICS_CONFIG, "kafka-metrics");
        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(kubeClient().getNamespace()).createOrReplace(kafkaMetricsCm);
        ConfigMap zkMetricsCm = TestUtils.configMapFromYaml(Constants.PATH_TO_KAFKA_METRICS_CONFIG, "kafka-metrics");
        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(kubeClient().getNamespace()).createOrReplace(zkMetricsCm);

        ConfigMap ccCm = new ConfigMapBuilder()
                .withApiVersion("v1")
                .withNewMetadata()
                    .withName("cruise-control-metrics-test")
                    .withLabels(Collections.singletonMap("app", "strimzi"))
                .endMetadata()
                .withData(Collections.singletonMap("metrics-config.yml",
                        "lowercaseOutputName: true\n" +
                        "rules:\n" +
                        "- pattern: kafka.cruisecontrol<name=(.+)><>(\\w+)\n" +
                        "  name: kafka_cruisecontrol_$1_$2\n" +
                        "  type: GAUGE"))
                .build();
        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(kubeClient().getNamespace()).createOrReplace(ccCm);

        ConfigMapKeySelector cmks = new ConfigMapKeySelectorBuilder()
                .withName("cruise-control-metrics-test")
                .withKey("metrics-config.yml")
                .build();
        JmxPrometheusExporterMetrics jmxPrometheusExporterMetrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(cmks)
                .endValueFrom()
                .build();

        return defaultKafka(kafka, name, kafkaReplicas, zookeeperReplicas)
            .editSpec()
                .withNewKafkaExporter()
                .endKafkaExporter()
                .withNewCruiseControl()
                    .withMetricsConfig(jmxPrometheusExporterMetrics)
                .endCruiseControl()
            .endSpec();
    }

    public static KafkaBuilder defaultKafka(String name, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(Constants.PATH_TO_KAFKA_EPHEMERAL_CONFIG);
        return defaultKafka(kafka, name, kafkaReplicas, zookeeperReplicas);
    }

    private static KafkaBuilder defaultKafka(Kafka kafka, String name, int kafkaReplicas, int zookeeperReplicas) {
        return new KafkaBuilder(kafka)
            .withNewMetadata()
                .withName(name)
                .withNamespace(kubeClient().getNamespace())
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withVersion(Environment.ST_KAFKA_VERSION)
                    .withReplicas(kafkaReplicas)
                    .addToConfig("log.message.format.version", TestKafkaVersion.getKafkaVersionsInMap().get(Environment.ST_KAFKA_VERSION).protocolVersion())
                    .addToConfig("inter.broker.protocol.version", TestKafkaVersion.getKafkaVersionsInMap().get(Environment.ST_KAFKA_VERSION).protocolVersion())
                    .addToConfig("offsets.topic.replication.factor", Math.min(kafkaReplicas, 3))
                    .addToConfig("transaction.state.log.min.isr", Math.min(kafkaReplicas, 2))
                    .addToConfig("transaction.state.log.replication.factor", Math.min(kafkaReplicas, 3))
                    .withListeners(new GenericKafkaListenerBuilder()
                                .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .build())
                    .withNewInlineLogging()
                        .addToLoggers("kafka.root.logger.level", "DEBUG")
                    .endInlineLogging()
                .endKafka()
                .editZookeeper()
                    .withReplicas(zookeeperReplicas)
                    .withNewInlineLogging()
                        .addToLoggers("zookeeper.root.logger", "DEBUG")
                    .endInlineLogging()
                .endZookeeper()
                .editEntityOperator()
                    .editUserOperator()
                        .withNewInlineLogging()
                            .addToLoggers("rootLogger.level", "DEBUG")
                        .endInlineLogging()
                    .endUserOperator()
                    .editTopicOperator()
                        .withNewInlineLogging()
                            .addToLoggers("rootLogger.level", "DEBUG")
                        .endInlineLogging()
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec();
    }

    public static KafkaBuilder kafkaFromYaml(File yamlFile, String clusterName, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(yamlFile);
        return new KafkaBuilder(kafka)
            .withNewMetadata()
                .withName(clusterName)
                .withNamespace(kubeClient().getNamespace())
            .endMetadata()
            .editOrNewSpec()
                .editKafka()
                    .withReplicas(kafkaReplicas)
                .endKafka()
                .editZookeeper()
                    .withReplicas(zookeeperReplicas)
                .endZookeeper()
            .endSpec();
    }

    /**
     * This method is used for delete specific Kafka cluster without wait for all resources deletion.
     * It can be use for example for delete Kafka cluster CR with unsupported Kafka version.
     * @param resourceName kafka cluster name
     */
    public static void deleteKafkaWithoutWait(String resourceName) {
        kafkaClient().inNamespace(ResourceManager.kubeClient().getNamespace()).withName(resourceName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    private static Kafka getKafkaFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, Kafka.class);
    }

    private static Kafka getKafkaFromYaml(File yamlFile) {
        return TestUtils.configFromYaml(yamlFile, Kafka.class);
    }

}
