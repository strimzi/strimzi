/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LoadBalancerIngressBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.api.model.RouteBuilder;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.KafkaBridgeConsumerSpec;
import io.strimzi.api.kafka.model.KafkaBridgeHttpConfig;
import io.strimzi.api.kafka.model.KafkaBridgeProducerSpec;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaExporterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerConsumerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerProducerSpec;
import io.strimzi.api.kafka.model.KafkaSpec;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.MetricsConfig;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.ZookeeperClusterSpec;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ZookeeperScaler;
import io.strimzi.operator.cluster.operator.resource.ZookeeperScalerProvider;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.cluster.operator.resource.ZookeeperSetOperator;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.BuildConfigOperator;
import io.strimzi.operator.common.operator.resource.BuildOperator;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.IngressOperator;
import io.strimzi.operator.common.operator.resource.IngressV1Beta1Operator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.NodeOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;
import io.strimzi.operator.common.operator.resource.RoleOperator;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.operator.common.operator.resource.StorageClassOperator;
import io.strimzi.test.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({
        "checkstyle:ClassDataAbstractionCoupling",
        "checkstyle:ClassFanOutComplexity"
})
public class ResourceUtils {

    private ResourceUtils() {

    }

    public static Kafka createKafka(String namespace, String name, int replicas,
                                    String image, int healthDelay, int healthTimeout) {
        Probe probe = new ProbeBuilder()
                .withInitialDelaySeconds(healthDelay)
                .withTimeoutSeconds(healthTimeout)
                .withFailureThreshold(10)
                .withSuccessThreshold(4)
                .withPeriodSeconds(33)
                .build();

        ObjectMeta meta = new ObjectMetaBuilder()
            .withNamespace(namespace)
            .withName(name)
            .withLabels(Labels.fromMap(singletonMap("my-user-label", "cromulent")).toMap())
            .build();

        KafkaBuilder builder = new KafkaBuilder();
        return builder.withMetadata(meta)
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(replicas)
                        .withImage(image)
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .build())
                        .withLivenessProbe(probe)
                        .withReadinessProbe(probe)
                        .withStorage(new EphemeralStorage())
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(replicas)
                        .withImage(image + "-zk")
                        .withLivenessProbe(probe)
                        .withReadinessProbe(probe)
                    .endZookeeper()
                .endSpec()
                .build();
    }

    public static Kafka createKafka(String namespace, String name, int replicas,
                                    String image, int healthDelay, int healthTimeout,
                                    MetricsConfig metricsConfig,
                                    Map<String, Object> kafkaConfigurationJson,
                                    Map<String, Object> zooConfigurationJson) {
        return new KafkaBuilder(createKafka(namespace, name, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .editKafka()
                        .withConfig(kafkaConfigurationJson)
                        .withMetricsConfig(metricsConfig)
                    .endKafka()
                    .editZookeeper()
                        .withConfig(zooConfigurationJson)
                        .withMetricsConfig(metricsConfig)
                    .endZookeeper()
                .endSpec()
                .build();
    }

    public static Secret createSecret(String name, String namespace, Map<String, String> data) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                .endMetadata()
                .withData(data)
                .build();
    }

    public static List<Secret> createKafkaInitialSecrets(String namespace, String name) {
        List<Secret> secrets = new ArrayList<>();
        secrets.add(createInitialCaCertSecret(namespace, name,
                AbstractModel.clusterCaCertSecretName(name), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"));
        secrets.add(createInitialCaKeySecret(namespace, name,
                AbstractModel.clusterCaKeySecretName(name), MockCertManager.clusterCaKey()));
        return secrets;
    }

    public static ClusterCa createInitialClusterCa(Reconciliation reconciliation, String clusterName, Secret initialClusterCaCert, Secret initialClusterCaKey) {
        return new ClusterCa(reconciliation, new MockCertManager(), new PasswordGenerator(10, "a", "a"), clusterName, initialClusterCaCert, initialClusterCaKey);
    }

    public static ClientsCa createInitialClientsCa(Reconciliation reconciliation, String clusterName, Secret initialClientsCaCert, Secret initialClientsCaKey) {
        return new ClientsCa(reconciliation, new MockCertManager(),
                new PasswordGenerator(10, "a", "a"),
                KafkaCluster.clientsCaCertSecretName(clusterName),
                initialClientsCaCert,
                KafkaCluster.clientsCaKeySecretName(clusterName),
                initialClientsCaKey, 365, 30, true, null);
    }

    public static Secret createInitialCaCertSecret(String clusterNamespace, String clusterName, String secretName,
                                                   String caCert, String caStore, String caStorePassword) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(secretName)
                    .withNamespace(clusterNamespace)
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "0")
                    .withLabels(Labels.forStrimziCluster(clusterName).withStrimziKind(Kafka.RESOURCE_KIND).toMap())
                .endMetadata()
                .addToData("ca.crt", caCert)
                .addToData("ca.p12", caStore)
                .addToData("ca.password", caStorePassword)
                .build();
    }

    public static Secret createInitialCaKeySecret(String clusterNamespace, String clusterName, String secretName, String caKey) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(secretName)
                    .withNamespace(clusterNamespace)
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "0")
                    .withLabels(Labels.forStrimziCluster(clusterName).withStrimziKind(Kafka.RESOURCE_KIND).toMap())
                .endMetadata()
                .addToData("ca.key", caKey)
                .build();
    }

    public static List<Secret> createKafkaSecretsWithReplicas(String namespace, String name, int kafkaReplicas, int zkReplicas) {
        List<Secret> secrets = new ArrayList<>();

        secrets.add(createInitialCaKeySecret(namespace, name,
                AbstractModel.clusterCaKeySecretName(name), MockCertManager.clusterCaKey()));
        secrets.add(createInitialCaCertSecret(namespace, name,
                AbstractModel.clusterCaCertSecretName(name), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"));

        secrets.add(
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(KafkaCluster.clientsCaKeySecretName(name))
                        .withNamespace(namespace)
                        .withLabels(Labels.forStrimziCluster(name).toMap())
                        .endMetadata()
                        .addToData("clients-ca.key", MockCertManager.clientsCaKey())
                        .addToData("clients-ca.crt", MockCertManager.clientsCaCert())
                        .build()
        );

        secrets.add(
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(KafkaCluster.clientsCaCertSecretName(name))
                        .withNamespace(namespace)
                        .withLabels(Labels.forStrimziCluster(name).toMap())
                        .endMetadata()
                        .addToData("clients-ca.crt", MockCertManager.clientsCaCert())
                        .build()
        );

        SecretBuilder builder =
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(KafkaCluster.brokersSecretName(name))
                        .withNamespace(namespace)
                        .withLabels(Labels.forStrimziCluster(name).toMap())
                        .endMetadata()
                        .addToData("cluster-ca.crt", MockCertManager.clusterCaCert());

        for (int i = 0; i < kafkaReplicas; i++) {
            builder.addToData(KafkaCluster.kafkaPodName(name, i) + ".key", Base64.getEncoder().encodeToString("brokers-internal-base64key".getBytes()))
                    .addToData(KafkaCluster.kafkaPodName(name, i) + ".crt", Base64.getEncoder().encodeToString("brokers-internal-base64crt".getBytes()));
        }
        secrets.add(builder.build());

        builder = new SecretBuilder()
                        .withNewMetadata()
                            .withName(KafkaCluster.clusterCaCertSecretName(name))
                            .withNamespace(namespace)
                            .withLabels(Labels.forStrimziCluster(name).toMap())
                        .endMetadata()
                        .addToData("ca.crt", Base64.getEncoder().encodeToString("cluster-ca-base64crt".getBytes()));

        for (int i = 0; i < kafkaReplicas; i++) {
            builder.addToData(KafkaCluster.kafkaPodName(name, i) + ".key", Base64.getEncoder().encodeToString("brokers-clients-base64key".getBytes()))
                    .addToData(KafkaCluster.kafkaPodName(name, i) + ".crt", Base64.getEncoder().encodeToString("brokers-clients-base64crt".getBytes()));
        }
        secrets.add(builder.build());

        builder = new SecretBuilder()
                        .withNewMetadata()
                            .withName(ZookeeperCluster.nodesSecretName(name))
                            .withNamespace(namespace)
                            .withLabels(Labels.forStrimziCluster(name).toMap())
                        .endMetadata()
                        .addToData("cluster-ca.crt", Base64.getEncoder().encodeToString("cluster-ca-base64crt".getBytes()));

        for (int i = 0; i < zkReplicas; i++) {
            builder.addToData(ZookeeperCluster.zookeeperPodName(name, i) + ".key", Base64.getEncoder().encodeToString("nodes-base64key".getBytes()))
                    .addToData(ZookeeperCluster.zookeeperPodName(name, i) + ".crt", Base64.getEncoder().encodeToString("nodes-base64crt".getBytes()));
        }
        secrets.add(builder.build());

        return secrets;
    }

    public static Kafka createKafka(String namespace, String name, int replicas,
                                    String image, int healthDelay, int healthTimeout,
                                    MetricsConfig metricsConfig,
                                    Map<String, Object> kafkaConfigurationJson,
                                    Logging kafkaLogging, Logging zkLogging) {
        return new KafkaBuilder(createKafka(namespace, name, replicas, image, healthDelay,
                    healthTimeout, metricsConfig, kafkaConfigurationJson, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withLogging(kafkaLogging)
                    .endKafka()
                    .editZookeeper()
                        .withLogging(zkLogging)
                    .endZookeeper()
                .endSpec()
                .build();
    }

    public static Kafka createKafka(String namespace, String name, int replicas,
                                    String image, int healthDelay, int healthTimeout,
                                    MetricsConfig metricsConfig,
                                    Map<String, Object> kafkaConfiguration,
                                    Map<String, Object> zooConfiguration,
                                    Storage kafkaStorage,
                                    SingleVolumeStorage zkStorage,
                                    Logging kafkaLogging, Logging zkLogging,
                                    KafkaExporterSpec keSpec,
                                    CruiseControlSpec ccSpec) {
        Kafka result = new Kafka();
        ObjectMeta meta = new ObjectMetaBuilder()
            .withNamespace(namespace)
            .withName(name)
            .withLabels(Labels.fromMap(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests", "my-user-label", "cromulent")).toMap())
            .build();
        result.setMetadata(meta);

        KafkaSpec spec = new KafkaSpec();

        KafkaClusterSpec kafkaClusterSpec = new KafkaClusterSpec();
        kafkaClusterSpec.setReplicas(replicas);
        kafkaClusterSpec.setListeners(singletonList(new GenericKafkaListenerBuilder().withName("plain").withPort(9092).withTls(false).withType(KafkaListenerType.INTERNAL).build()));
        kafkaClusterSpec.setImage(image);
        if (kafkaLogging != null) {
            kafkaClusterSpec.setLogging(kafkaLogging);
        }
        Probe livenessProbe = new Probe();
        livenessProbe.setInitialDelaySeconds(healthDelay);
        livenessProbe.setTimeoutSeconds(healthTimeout);
        livenessProbe.setSuccessThreshold(4);
        livenessProbe.setFailureThreshold(10);
        livenessProbe.setPeriodSeconds(33);
        kafkaClusterSpec.setLivenessProbe(livenessProbe);
        kafkaClusterSpec.setReadinessProbe(livenessProbe);
        kafkaClusterSpec.setMetricsConfig(metricsConfig);

        if (kafkaConfiguration != null) {
            kafkaClusterSpec.setConfig(kafkaConfiguration);
        }
        kafkaClusterSpec.setStorage(kafkaStorage);
        spec.setKafka(kafkaClusterSpec);

        ZookeeperClusterSpec zk = new ZookeeperClusterSpec();
        zk.setReplicas(replicas);
        zk.setImage(image + "-zk");
        if (zkLogging != null) {
            zk.setLogging(zkLogging);
        }
        zk.setLivenessProbe(livenessProbe);
        zk.setReadinessProbe(livenessProbe);
        if (zooConfiguration != null) {
            zk.setConfig(zooConfiguration);
        }
        zk.setStorage(zkStorage);
        zk.setMetricsConfig(metricsConfig);

        spec.setKafkaExporter(keSpec);
        spec.setCruiseControl(ccSpec);
        spec.setZookeeper(zk);

        result.setSpec(spec);
        return result;
    }

    /**
     * Create an empty Kafka Connect custom resource
     */
    public static KafkaConnect createEmptyKafkaConnect(String namespace, String name) {
        return new KafkaConnectBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .withAnnotations(emptyMap())
                        .build())
                .withNewSpec()
                .endSpec()
                .build();
    }

    /**
     * Create an empty Kafka Bridge custom resource
     */
    public static KafkaBridge createEmptyKafkaBridge(String namespace, String name) {
        return new KafkaBridgeBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec()
                    .withNewHttp(8080)
                .endSpec()
                .build();
    }

    /**
     * Create an empty Kafka MirrorMaker custom resource
     */
    public static KafkaMirrorMaker createEmptyKafkaMirrorMaker(String namespace, String name) {
        return new KafkaMirrorMakerBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec()
                .endSpec()
                .build();
    }

    public static KafkaMirrorMaker createKafkaMirrorMaker(String namespace, String name, String image, KafkaMirrorMakerProducerSpec producer,
                                                          KafkaMirrorMakerConsumerSpec consumer, String include) {
        return createKafkaMirrorMaker(namespace, name, image, null, producer, consumer, include);
    }

    public static KafkaMirrorMaker createKafkaMirrorMaker(String namespace, String name, String image, Integer replicas,
                                                          KafkaMirrorMakerProducerSpec producer, KafkaMirrorMakerConsumerSpec consumer,
                                                          String include) {

        KafkaMirrorMakerBuilder builder = new KafkaMirrorMakerBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec()
                    .withImage(image)
                    .withProducer(producer)
                    .withConsumer(consumer)
                    .withInclude(include)
                .endSpec();

        if (replicas != null) {
            builder.editOrNewSpec()
                        .withReplicas(replicas)
                    .endSpec();
        }

        return builder.build();
    }

    public static KafkaBridge createKafkaBridge(String namespace, String name, String image, int replicas, String bootstrapservers, KafkaBridgeProducerSpec producer, KafkaBridgeConsumerSpec consumer, KafkaBridgeHttpConfig http, boolean enableMetrics) {
        return new KafkaBridgeBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec()
                    .withImage(image)
                    .withReplicas(replicas)
                    .withBootstrapServers(bootstrapservers)
                    .withProducer(producer)
                    .withConsumer(consumer)
                    .withEnableMetrics(enableMetrics)
                    .withHttp(http)
                .endSpec()
                .build();
    }

    /**
     * Create an empty Kafka MirrorMaker 2.0 custom resource
     */
    public static KafkaMirrorMaker2 createEmptyKafkaMirrorMaker2(String namespace, String name, Integer replicas) {
        KafkaMirrorMaker2Builder kafkaMirrorMaker2Builder = new KafkaMirrorMaker2Builder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec().endSpec();

        if (replicas != null) {
            kafkaMirrorMaker2Builder
                    .editOrNewSpec()
                        .withReplicas(replicas)
                    .endSpec();
        }

        return kafkaMirrorMaker2Builder.build();
    }

    public static KafkaMirrorMaker2 createEmptyKafkaMirrorMaker2(String namespace, String name) {
        return createEmptyKafkaMirrorMaker2(namespace, name, null);
    }

    public static void cleanUpTemporaryTLSFiles() {
        String tmpString = "/tmp";
        try {
            Files.list(Paths.get(tmpString)).filter(path -> path.toString().startsWith(tmpString + "/tls")).forEach(delPath -> {
                try {
                    Files.deleteIfExists(delPath);
                } catch (IOException e) {
                }
            });
        } catch (IOException e) {
        }
    }

    public static ZookeeperLeaderFinder zookeeperLeaderFinder(Vertx vertx, KubernetesClient client) {
        return new ZookeeperLeaderFinder(vertx, new SecretOperator(vertx, client),
            () -> new BackOff(5_000, 2, 4)) {
                @Override
                protected Future<Boolean> isLeader(Reconciliation reconciliation, Pod pod, NetClientOptions options) {
                    return Future.succeededFuture(true);
                }

                @Override
                protected PemTrustOptions trustOptions(Reconciliation reconciliation, Secret s) {
                    return new PemTrustOptions();
                }

                @Override
                protected PemKeyCertOptions keyCertOptions(Secret s) {
                    return new PemKeyCertOptions();
                }
            };
    }

    public static AdminClientProvider adminClientProvider() {
        return new AdminClientProvider() {
            @Override
            public Admin createAdminClient(String bootstrapHostnames, Secret clusterCaCertSecret, Secret keyCertSecret, String keyCertName) {
                Admin mock = mock(AdminClient.class);
                DescribeClusterResult dcr;
                try {
                    Constructor<DescribeClusterResult> declaredConstructor = DescribeClusterResult.class.getDeclaredConstructor(KafkaFuture.class, KafkaFuture.class, KafkaFuture.class, KafkaFuture.class);
                    declaredConstructor.setAccessible(true);
                    KafkaFuture<Node> objectKafkaFuture = KafkaFutureImpl.completedFuture(new Node(0, "localhost", 9091));
                    KafkaFuture<String> stringKafkaFuture = KafkaFutureImpl.completedFuture("CLUSTERID");
                    dcr = declaredConstructor.newInstance(null, objectKafkaFuture, stringKafkaFuture, null);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
                when(mock.describeCluster()).thenReturn(dcr);

                ListTopicsResult ltr;
                try {
                    Constructor<ListTopicsResult> declaredConstructor = ListTopicsResult.class.getDeclaredConstructor(KafkaFuture.class);
                    declaredConstructor.setAccessible(true);
                    KafkaFuture<Map<String, TopicListing>> future = KafkaFutureImpl.completedFuture(emptyMap());
                    ltr = declaredConstructor.newInstance(future);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
                when(mock.listTopics(any())).thenReturn(ltr);

                DescribeTopicsResult dtr;
                try {
                    Constructor<DescribeTopicsResult> declaredConstructor = DescribeTopicsResult.class.getDeclaredConstructor(Map.class);
                    declaredConstructor.setAccessible(true);
                    dtr = declaredConstructor.newInstance(emptyMap());
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
                when(mock.describeTopics(any())).thenReturn(dtr);

                DescribeConfigsResult dcfr;
                try {
                    Constructor<DescribeConfigsResult> declaredConstructor = DescribeConfigsResult.class.getDeclaredConstructor(Map.class);
                    declaredConstructor.setAccessible(true);
                    dcfr = declaredConstructor.newInstance(emptyMap());
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
                when(mock.describeConfigs(any())).thenReturn(dcfr);
                return mock;
            }
        };
    }

    public static ZookeeperScalerProvider zookeeperScalerProvider() {
        return new ZookeeperScalerProvider() {
            @Override
            public ZookeeperScaler createZookeeperScaler(Reconciliation reconciliation, Vertx vertx, String zookeeperConnectionString, Function<Integer, String> zkNodeAddress, Secret clusterCaCertSecret, Secret coKeySecret, long operationTimeoutMs) {
                ZookeeperScaler mockZooScaler = mock(ZookeeperScaler.class);
                when(mockZooScaler.scale(anyInt())).thenReturn(Future.succeededFuture());
                return mockZooScaler;
            }
        };
    }

    public static MetricsProvider metricsProvider() {
        return new MetricsProvider() {
            @Override
            public MeterRegistry meterRegistry() {
                MeterRegistry mockRegistry = mock(MeterRegistry.class);
                MeterRegistry.Config mockConfig = mock(MeterRegistry.Config.class);
                Clock mockClock = mock(Clock.class);
                when(mockConfig.clock()).thenReturn(mockClock);
                when(mockRegistry.config()).thenReturn(mockConfig);

                return mockRegistry;
            }

            @Override
            public Counter counter(String name, String description, Tags tags) {
                return mock(Counter.class);
            }

            @Override
            public Timer timer(String name, String description, Tags tags) {
                return mock(Timer.class);
            }

            @Override
            public AtomicInteger gauge(String name, String description, Tags tags) {
                return new AtomicInteger(0);
            }
        };
    }

    public static ResourceOperatorSupplier supplierWithMocks(boolean openShift) {
        RouteOperator routeOps = openShift ? mock(RouteOperator.class) : null;

        ResourceOperatorSupplier supplier = new ResourceOperatorSupplier(
                mock(ServiceOperator.class),
                routeOps,
                mock(ZookeeperSetOperator.class),
                mock(KafkaSetOperator.class),
                mock(ConfigMapOperator.class),
                mock(SecretOperator.class),
                mock(PvcOperator.class),
                mock(DeploymentOperator.class),
                mock(ServiceAccountOperator.class),
                mock(RoleBindingOperator.class),
                mock(RoleOperator.class),
                mock(ClusterRoleBindingOperator.class),
                mock(NetworkPolicyOperator.class),
                mock(PodDisruptionBudgetOperator.class),
                mock(PodOperator.class),
                mock(IngressOperator.class),
                mock(IngressV1Beta1Operator.class),
                mock(BuildConfigOperator.class),
                mock(BuildOperator.class),
                mock(CrdOperator.class),
                mock(CrdOperator.class),
                mock(CrdOperator.class),
                mock(CrdOperator.class),
                mock(CrdOperator.class),
                mock(CrdOperator.class),
                mock(CrdOperator.class),
                mock(StorageClassOperator.class),
                mock(NodeOperator.class),
                zookeeperScalerProvider(),
                metricsProvider(),
                adminClientProvider());

        when(supplier.secretOperations.getAsync(any(), any())).thenReturn(Future.succeededFuture());
        
        when(supplier.serviceAccountOperations.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(supplier.roleBindingOperations.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(supplier.roleOperations.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(supplier.clusterRoleBindingOperator.reconcile(any(), anyString(), any())).thenReturn(Future.succeededFuture());

        if (openShift) {
            when(supplier.routeOperations.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
            when(supplier.routeOperations.hasAddress(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
            when(supplier.routeOperations.get(anyString(), anyString())).thenAnswer(i -> {
                return new RouteBuilder()
                        .withNewStatus()
                        .addNewIngress()
                        .withHost(i.getArgument(0) + "." + i.getArgument(1) + ".mydomain.com")
                        .endIngress()
                        .endStatus()
                        .build();
            });
        }

        when(supplier.serviceOperations.hasIngressAddress(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(supplier.serviceOperations.hasNodePort(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(supplier.serviceOperations.get(anyString(), anyString())).thenAnswer(i ->
             new ServiceBuilder()
                    .withNewStatus()
                        .withNewLoadBalancer()
                            .withIngress(new LoadBalancerIngressBuilder().withHostname(i.getArgument(0) + "." + i.getArgument(1) + ".mydomain.com").build())
                        .endLoadBalancer()
                    .endStatus()
                    .withNewSpec()
                        .withPorts(new ServicePortBuilder().withNodePort(31245).build())
                    .endSpec()
                    .build());

        return supplier;
    }

    public static ClusterOperatorConfig dummyClusterOperatorConfig(KafkaVersion.Lookup versions, long operationTimeoutMs) {
        return new ClusterOperatorConfig(
                singleton("dummy"),
                60_000,
                operationTimeoutMs,
                300_000,
                false,
                versions,
                null,
                null,
                null,
                null,
                ClusterOperatorConfig.RbacScope.CLUSTER,
                null,
                "",
                10);
    }

    public static ClusterOperatorConfig dummyClusterOperatorConfigRolesOnly(KafkaVersion.Lookup versions, long operationTimeoutMs) {
        return new ClusterOperatorConfig(
                singleton("dummy"),
                60_000,
                operationTimeoutMs,
                300_000,
                false,
                versions,
                null,
                null,
                null,
                null,
                ClusterOperatorConfig.RbacScope.NAMESPACE,
                null,
                "",
                10);
    }

    public static ClusterOperatorConfig dummyClusterOperatorConfig(KafkaVersion.Lookup versions) {
        return dummyClusterOperatorConfig(versions, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS);
    }

    public static ClusterOperatorConfig dummyClusterOperatorConfig(long operationTimeoutMs) {
        return dummyClusterOperatorConfig(new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap()), operationTimeoutMs);
    }

    public static ClusterOperatorConfig dummyClusterOperatorConfig() {
        return dummyClusterOperatorConfig(new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap()));
    }

    /**
     * Find the first resource in the given resources with the given name.
     * @param resources The resources to search.
     * @param name The name of the resource.
     * @return The first resource with that name. Names should be unique per namespace.
     */
    public static <T extends HasMetadata> T findResourceWithName(List<T> resources, String name) {
        return resources.stream()
                .filter(s -> s.getMetadata().getName().equals(name)).findFirst()
                .orElse(null);
    }
}
