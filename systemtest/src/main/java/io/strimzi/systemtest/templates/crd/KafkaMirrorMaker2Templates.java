/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaMirrorMaker2List;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;

import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

public class KafkaMirrorMaker2Templates {

    private KafkaMirrorMaker2Templates() {}

    public static MixedOperation<KafkaMirrorMaker2, KafkaMirrorMaker2List, Resource<KafkaMirrorMaker2>> kafkaMirrorMaker2Client() {
        return Crds.kafkaMirrorMaker2Operation(ResourceManager.kubeClient().getClient());
    }

    public static KafkaMirrorMaker2Builder kafkaMirrorMaker2(String name, String targetClusterName, String sourceClusterName, int kafkaMirrorMaker2Replicas, boolean tlsListener) {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = getKafkaMirrorMaker2FromYaml(Constants.PATH_TO_KAFKA_MIRROR_MAKER_2_CONFIG);
        return defaultKafkaMirrorMaker2(kafkaMirrorMaker2, name, targetClusterName, sourceClusterName, kafkaMirrorMaker2Replicas, tlsListener);
    }

    public static KafkaMirrorMaker2Builder kafkaMirrorMaker2WithMetrics(String name, String targetClusterName, String sourceClusterName, int kafkaMirrorMaker2Replicas) {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = getKafkaMirrorMaker2FromYaml(Constants.PATH_TO_KAFKA_MIRROR_MAKER_2_METRICS_CONFIG);
        ConfigMap metricsCm = TestUtils.configMapFromYaml(Constants.PATH_TO_KAFKA_MIRROR_MAKER_2_METRICS_CONFIG, "mirror-maker-2-metrics");
        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(kubeClient().getNamespace()).createOrReplace(metricsCm);
        return defaultKafkaMirrorMaker2(kafkaMirrorMaker2, name, targetClusterName, sourceClusterName, kafkaMirrorMaker2Replicas, false);
    }

    public static KafkaMirrorMaker2Builder defaultKafkaMirrorMaker2(String name, String targetClusterName, String sourceClusterName, int kafkaMirrorMaker2Replicas, boolean tlsListener) {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = getKafkaMirrorMaker2FromYaml(Constants.PATH_TO_KAFKA_MIRROR_MAKER_2_CONFIG);
        return defaultKafkaMirrorMaker2(kafkaMirrorMaker2, name, targetClusterName, sourceClusterName, kafkaMirrorMaker2Replicas, tlsListener);
    }

    private static KafkaMirrorMaker2Builder defaultKafkaMirrorMaker2(KafkaMirrorMaker2 kafkaMirrorMaker2, String name, String kafkaTargetClusterName, String kafkaSourceClusterName, int kafkaMirrorMaker2Replicas, boolean tlsListener) {

        KafkaMirrorMaker2ClusterSpec targetClusterSpec = new KafkaMirrorMaker2ClusterSpecBuilder()
            .withAlias(kafkaTargetClusterName)
            .withBootstrapServers(KafkaResources.plainBootstrapAddress(kafkaTargetClusterName))
            .addToConfig("config.storage.replication.factor", 1)
            .addToConfig("offset.storage.replication.factor", 1)
            .addToConfig("status.storage.replication.factor", 1)
            .build();

        KafkaMirrorMaker2ClusterSpec sourceClusterSpec = new KafkaMirrorMaker2ClusterSpecBuilder()
            .withAlias(kafkaSourceClusterName)
            .withBootstrapServers(KafkaResources.plainBootstrapAddress(kafkaSourceClusterName))
            .build();

        if (tlsListener) {
            targetClusterSpec = new KafkaMirrorMaker2ClusterSpecBuilder(targetClusterSpec)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaTargetClusterName))
                .withNewTls()
                    .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaTargetClusterName)).withCertificate("ca.crt").build())
                .endTls()
                .build();

            sourceClusterSpec = new KafkaMirrorMaker2ClusterSpecBuilder(sourceClusterSpec)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaSourceClusterName))
                .withNewTls()
                    .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaSourceClusterName)).withCertificate("ca.crt").build())
                .endTls()
                .build();
        }

        return new KafkaMirrorMaker2Builder(kafkaMirrorMaker2)
            .withNewMetadata()
                .withName(name)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withClusterName(kafkaTargetClusterName)
            .endMetadata()
            .editOrNewSpec()
                .withVersion(Environment.ST_KAFKA_VERSION)
                .withReplicas(kafkaMirrorMaker2Replicas)
                .withConnectCluster(kafkaTargetClusterName)
                .withClusters(targetClusterSpec, sourceClusterSpec)
                .editFirstMirror()
                    .withSourceCluster(kafkaSourceClusterName)
                    .withTargetCluster(kafkaTargetClusterName)
                .endMirror()
                .withNewInlineLogging()
                    .addToLoggers("connect.root.logger.level", "DEBUG")
                .endInlineLogging()
            .endSpec();
    }

    public static void deleteKafkaMirrorMaker2WithoutWait(String resourceName) {
        kafkaMirrorMaker2Client().inNamespace(ResourceManager.kubeClient().getNamespace()).withName(resourceName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }
    private static KafkaMirrorMaker2 getKafkaMirrorMaker2FromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaMirrorMaker2.class);
    }
}
