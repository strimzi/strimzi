package io.strimzi.systemtest.templates;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectS2IList;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.extension.ExtensionContext;

public class KafkaConnectS2ITemplates {

    public static final String PATH_TO_KAFKA_CONNECT_S2I_CONFIG = TestUtils.USER_PATH + "/../examples/connect/kafka-connect-s2i.yaml";

    private KafkaConnectS2ITemplates() {}

    public static MixedOperation<KafkaConnectS2I, KafkaConnectS2IList, Resource<KafkaConnectS2I>> kafkaConnectS2IClient() {
        return Crds.kafkaConnectS2iOperation(ResourceManager.kubeClient().getClient());
    }

    public static KafkaConnectS2IBuilder kafkaConnectS2I(ExtensionContext extensionContext, String name, String clusterName, int kafkaConnectS2IReplicas, boolean allowNP) {
        KafkaConnectS2I kafkaConnectS2I = getKafkaConnectS2IFromYaml(PATH_TO_KAFKA_CONNECT_S2I_CONFIG);
        kafkaConnectS2I = defaultKafkaConnectS2I(kafkaConnectS2I, name, clusterName, kafkaConnectS2IReplicas).build();
        return allowNP ? deployKafkaConnectS2IWithNetworkPolicy(extensionContext, kafkaConnectS2I) : new KafkaConnectS2IBuilder(kafkaConnectS2I);
    }

    public static KafkaConnectS2IBuilder kafkaConnectS2I(ExtensionContext extensionContext, String name, String clusterName, int kafkaConnectReplicas) {
        return kafkaConnectS2I(extensionContext, name, clusterName, kafkaConnectReplicas, true);
    }

    private static KafkaConnectS2IBuilder deployKafkaConnectS2IWithNetworkPolicy(ExtensionContext extensionContext, KafkaConnectS2I kafkaConnectS2I) {
        if (Environment.DEFAULT_TO_DENY_NETWORK_POLICIES) {
            NetworkPolicyResource.allowNetworkPolicySettingsForResource(extensionContext, kafkaConnectS2I, KafkaConnectResources.deploymentName(kafkaConnectS2I.getMetadata().getName()));
        }
        return new KafkaConnectS2IBuilder(kafkaConnectS2I);
    }

    public static KafkaConnectS2IBuilder defaultKafkaConnectS2I(String name, String kafkaClusterName, int kafkaConnectReplicas) {
        KafkaConnectS2I kafkaConnectS2I = getKafkaConnectS2IFromYaml(PATH_TO_KAFKA_CONNECT_S2I_CONFIG);
        return defaultKafkaConnectS2I(kafkaConnectS2I, name, kafkaClusterName, kafkaConnectReplicas);
    }

    public static KafkaConnectS2IBuilder defaultKafkaConnectS2I(KafkaConnectS2I kafkaConnectS2I, String name, String kafkaClusterName, int kafkaConnectReplicas) {
        return new KafkaConnectS2IBuilder(kafkaConnectS2I)
            .withNewMetadata()
                .withName(name)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withClusterName(kafkaClusterName)
            .endMetadata()
            .editSpec()
                .withVersion(Environment.ST_KAFKA_VERSION)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaClusterName))
                .withReplicas(kafkaConnectReplicas)
                // Try it without TLS
                .withNewTls()
                    .withTrustedCertificates(new CertSecretSourceBuilder().withNewSecretName(kafkaClusterName + "-cluster-ca-cert").withCertificate("ca.crt").build())
                .endTls()
                .withInsecureSourceRepository(true)
                .withNewInlineLogging()
                    .addToLoggers("connect.root.logger.level", "DEBUG")
                .endInlineLogging()
            .endSpec();
    }

//    private static DoneableKafkaConnectS2I deployKafkaConnectS2I(ExtensionContext extensionContext, KafkaConnectS2I kafkaConnectS2I) {
//        // TODO: same as KafkaBridge and KafkaConnect
//        if (Environment.DEFAULT_TO_DENY_NETWORK_POLICIES) {
//            NetworkPolicyResource.allowNetworkPolicySettingsForResource(extensionContext, kafkaConnectS2I, KafkaConnectS2IResources.deploymentName(kafkaConnectS2I.getMetadata().getName()));
//        }
//        return new DoneableKafkaConnectS2I(kafkaConnectS2I, kC -> {
//            TestUtils.waitFor("KafkaConnect creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, CR_CREATION_TIMEOUT,
//                () -> {
//                    try {
//                        kafkaConnectS2IClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kC);
//                        return true;
//                    } catch (KubernetesClientException e) {
//                        if (e.getMessage().contains("object is being deleted")) {
//                            return false;
//                        } else {
//                            throw e;
//                        }
//                    }
//                }
//            );
//            return kC;
//        });
//    }

    public static void deleteKafkaConnectS2IWithoutWait(String resourceName) {
        kafkaConnectS2IClient().inNamespace(ResourceManager.kubeClient().getNamespace()).withName(resourceName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    private static KafkaConnectS2I getKafkaConnectS2IFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaConnectS2I.class);
    }
}
