/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerConsumerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerConsumerSpecBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerProducerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerProducerSpecBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.MetricsConfig;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationTlsBuilder;
import io.strimzi.api.kafka.model.template.ContainerTemplate;
import io.strimzi.api.kafka.model.template.DeploymentStrategy;
import io.strimzi.kafka.oauth.client.ClientConfig;
import io.strimzi.kafka.oauth.server.ServerConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.test.TestUtils.LINE_SEPARATOR;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling"})
@ParallelSuite
public class KafkaMirrorMakerClusterTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String namespace = "test";
    private final String cluster = "mirror";
    private final int replicas = 2;
    private final String image = "my-image:latest";
    private final String metricsCmJson = "{\"animal\":\"wombat\"}";
    private final String metricsCMName = "metrics-cm";
    private final ConfigMap metricsCM = io.strimzi.operator.cluster.TestUtils.getJmxMetricsCm(metricsCmJson, metricsCMName, "metrics-config.yml");
    private final JmxPrometheusExporterMetrics jmxMetricsConfig = io.strimzi.operator.cluster.TestUtils.getJmxPrometheusExporterMetrics("metrics-config.yml", metricsCMName);

    private final String producerConfigurationJson = "{\"foo\":\"bar\"}";
    private final String consumerConfigurationJson = "{\"foo\":\"buz\"}";
    private final String defaultProducerConfiguration = "";
    private final String defaultConsumerConfiguration = "";
    private final String expectedProducerConfiguration = "foo=bar" + LINE_SEPARATOR;
    private final String expectedConsumerConfiguration = "foo=buz" + LINE_SEPARATOR;
    private final String producerBootstrapServers = "target-kafka:9092";
    private final String consumerBootstrapServers = "source-kafka:9092";
    private final String groupId = "my-group-id";
    private final int numStreams = 2;
    private final String include = ".*";
    private final int offsetCommitInterval = 42000;
    private final boolean abortOnSendFailure = false;
    private final String kafkaHeapOpts = "-Xms" + AbstractModel.DEFAULT_JVM_XMS;

    private KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
            .withBootstrapServers(producerBootstrapServers)
            .withAbortOnSendFailure(abortOnSendFailure)
            .withConfig((Map<String, Object>) TestUtils.fromJson(producerConfigurationJson, Map.class))
            .build();

    private KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
            .withBootstrapServers(consumerBootstrapServers)
            .withGroupId(groupId)
            .withNumStreams(numStreams)
            .withOffsetCommitInterval(offsetCommitInterval)
            .withConfig((Map<String, Object>) TestUtils.fromJson(consumerConfigurationJson, Map.class))
            .build();

    private final KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(ResourceUtils.createEmptyKafkaMirrorMaker(namespace, cluster))
            .withNewSpec()
                .withImage(image)
                .withReplicas(replicas)
                .withProducer(producer)
                .withConsumer(consumer)
                .withInclude(include)
            .endSpec()
            .build();

    private final KafkaMirrorMaker resourceWithMetrics = new KafkaMirrorMakerBuilder(resource)
            .editSpec()
                .withMetricsConfig(jmxMetricsConfig)
            .endSpec()
            .build();

    private final KafkaMirrorMakerCluster mm = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resourceWithMetrics, VERSIONS);

    @ParallelTest
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = mm.generateMetricsAndLogConfigMap(new MetricsAndLogging(metricsCM, null));
        checkMetricsConfigMap(metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertThat(metricsCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_METRICS), is(metricsCmJson));
    }

    private Map<String, String> expectedLabels(String name)    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                "my-user-label", "cromulent",
                Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker.RESOURCE_KIND,
                Labels.STRIMZI_NAME_LABEL, name,
                Labels.KUBERNETES_NAME_LABEL, KafkaMirrorMakerCluster.APPLICATION_NAME,
                Labels.KUBERNETES_INSTANCE_LABEL, this.cluster,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + this.cluster,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziSelectorLabels().toMap();
    }

    private Map<String, String> expectedLabels()    {
        return expectedLabels(KafkaMirrorMakerResources.deploymentName(cluster));
    }

    protected List<EnvVar> getExpectedEnvVars() {

        List<EnvVar> expected = new ArrayList<>();

        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_CONFIGURATION_CONSUMER).withValue(expectedConsumerConfiguration).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_CONFIGURATION_PRODUCER).withValue(expectedProducerConfiguration).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_METRICS_ENABLED).withValue("true").build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_CONSUMER).withValue(consumerBootstrapServers).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_PRODUCER).withValue(producerBootstrapServers).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_INCLUDE).withValue(include).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_GROUPID_CONSUMER).withValue(groupId).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_NUMSTREAMS).withValue(Integer.toString(numStreams)).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OFFSET_COMMIT_INTERVAL).withValue(Integer.toString(offsetCommitInterval)).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_ABORT_ON_SEND_FAILURE).withValue(Boolean.toString(abortOnSendFailure)).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED).withValue(Boolean.toString(AbstractModel.DEFAULT_JVM_GC_LOGGING_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_HEAP_OPTS).withValue(kafkaHeapOpts).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_STRIMZI_LIVENESS_PERIOD).withValue("10").build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_STRIMZI_READINESS_PERIOD).withValue("10").build());

        return expected;
    }

    @ParallelTest
    public void testDefaultValues() {
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(ResourceUtils.createEmptyKafkaMirrorMaker(namespace, cluster))
                .withNewSpec()
                    .withReplicas(replicas)
                    .withProducer(producer)
                    .withConsumer(consumer)
                    .withInclude(".*")
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mm = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        assertThat(mm.image, is(KafkaVersionTestUtils.DEFAULT_KAFKA_MIRROR_MAKER_IMAGE));
        assertThat(new KafkaMirrorMakerConsumerConfiguration(Reconciliation.DUMMY_RECONCILIATION, mm.consumer.getConfig().entrySet()).getConfiguration(), is(defaultConsumerConfiguration));
        assertThat(new KafkaMirrorMakerProducerConfiguration(Reconciliation.DUMMY_RECONCILIATION, mm.producer.getConfig().entrySet()).getConfiguration(), is(defaultProducerConfiguration));
    }

    @ParallelTest
    public void testFromCrd() {
        assertThat(mm.getReplicas(), is(replicas));
        assertThat(mm.getImage(), is(image));
        assertThat(mm.consumer.getBootstrapServers(), is(consumerBootstrapServers));
        assertThat(mm.producer.getBootstrapServers(), is(producerBootstrapServers));
        assertThat(mm.getInclude(), is(include));
        assertThat(mm.consumer.getGroupId(), is(groupId));
    }

    // Tests handling of the new include field and the deprecated whitelist field
    @ParallelTest
    public void testIncludeHandling() {
        KafkaMirrorMaker both = new KafkaMirrorMakerBuilder(resource)
                .editSpec()
                    .withInclude(include)
                    .withWhitelist("alternative.*")
                .endSpec()
                .build();
        KafkaMirrorMakerCluster cluster = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, both, VERSIONS);

        assertThat(cluster.getInclude(), is(include));

        KafkaMirrorMaker legacy = new KafkaMirrorMakerBuilder(resource)
                .editSpec()
                    .withWhitelist("alternative.*")
                    .withInclude(null)
                .endSpec()
                .build();
        cluster = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, legacy, VERSIONS);

        assertThat(cluster.getInclude(), is("alternative.*"));

        KafkaMirrorMaker none = new KafkaMirrorMakerBuilder(resource)
                .editSpec()
                    .withWhitelist(null)
                    .withInclude(null)
                .endSpec()
                .build();

        assertThrows(InvalidResourceException.class, () -> KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, none, VERSIONS));
    }

    @ParallelTest
    public void testEnvVars()   {
        assertThat(mm.getEnvVars(), is(getExpectedEnvVars()));
    }

    @ParallelTest
    public void testGenerateDeployment()   {
        Deployment dep = mm.generateDeployment(new HashMap<String, String>(), true, null, null);

        assertThat(dep.getMetadata().getName(), is(KafkaMirrorMakerResources.deploymentName(cluster)));
        assertThat(dep.getMetadata().getNamespace(), is(namespace));
        Map<String, String> expectedLabels = expectedLabels();
        assertThat(dep.getMetadata().getLabels(), is(expectedLabels));
        assertThat(dep.getSpec().getSelector().getMatchLabels(), is(expectedSelectorLabels()));
        assertThat(dep.getSpec().getReplicas(), is(Integer.valueOf(replicas)));
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels(), is(expectedLabels));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getName(), is(KafkaMirrorMakerResources.deploymentName(this.cluster)));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage(), is(mm.image));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv(), is(getExpectedEnvVars()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().size(), is(1));
        assertThat(dep.getSpec().getStrategy().getType(), is("RollingUpdate"));
        assertThat(dep.getSpec().getStrategy().getRollingUpdate().getMaxSurge().getIntVal(), is(Integer.valueOf(1)));
        assertThat(dep.getSpec().getStrategy().getRollingUpdate().getMaxUnavailable().getIntVal(), is(Integer.valueOf(0)));
        checkOwnerReference(mm.createOwnerReference(), dep);
    }

    @ParallelTest
    public void testGenerateDeploymentWithTls() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                .editOrNewConsumer()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret-c").withCertificate("cert.crt").build())
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret-c").withCertificate("new-cert.crt").build())
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-another-secret-c").withCertificate("another-cert.crt").build())
                .endTls()
                .endConsumer()
                .editOrNewProducer()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret-p").withCertificate("cert.crt").build())
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret-p").withCertificate("new-cert.crt").build())
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-another-secret-p").withCertificate("another-cert.crt").build())
                .endTls()
                .endProducer()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("my-secret-p"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("my-another-secret-p"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMakerCluster.TLS_CERTS_VOLUME_MOUNT_PRODUCER + "my-secret-p"));
        assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMakerCluster.TLS_CERTS_VOLUME_MOUNT_PRODUCER + "my-another-secret-p"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_TLS_PRODUCER), is("true"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_TRUSTED_CERTS_PRODUCER),
                is("my-secret-p/cert.crt;my-secret-p/new-cert.crt;my-another-secret-p/another-cert.crt"));

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(4).getName(), is("my-secret-c"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(5).getName(), is("my-another-secret-c"));

        assertThat(containers.get(0).getVolumeMounts().get(4).getMountPath(), is(KafkaMirrorMakerCluster.TLS_CERTS_VOLUME_MOUNT_CONSUMER + "my-secret-c"));
        assertThat(containers.get(0).getVolumeMounts().get(5).getMountPath(), is(KafkaMirrorMakerCluster.TLS_CERTS_VOLUME_MOUNT_CONSUMER + "my-another-secret-c"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_TLS_CONSUMER), is("true"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_TRUSTED_CERTS_CONSUMER),
                is("my-secret-c/cert.crt;my-secret-c/new-cert.crt;my-another-secret-c/another-cert.crt"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithTlsAuth() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                .editOrNewConsumer()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret-c").withCertificate("cert.crt").build())
                .endTls()
                .withAuthentication(
                        new KafkaClientAuthenticationTlsBuilder()
                                .withNewCertificateAndKey()
                                .withSecretName("user-secret-c")
                                .withCertificate("user.crt")
                                .withKey("user.key")
                                .endCertificateAndKey()
                                .build())
                .endConsumer()
                .editOrNewProducer()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret-p").withCertificate("cert.crt").build())
                .endTls()
                .withAuthentication(
                        new KafkaClientAuthenticationTlsBuilder()
                                .withNewCertificateAndKey()
                                .withSecretName("user-secret-p")
                                .withCertificate("user.crt")
                                .withKey("user.key")
                                .endCertificateAndKey()
                                .build())
                .endProducer()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(5).getName(), is("user-secret-c"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().get(5).getMountPath(), is(KafkaMirrorMakerCluster.TLS_CERTS_VOLUME_MOUNT_CONSUMER + "user-secret-c"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_CERT_CONSUMER), is("user-secret-c/user.crt"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_KEY_CONSUMER), is("user-secret-c/user.key"));

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("user-secret-p"));

        assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMakerCluster.TLS_CERTS_VOLUME_MOUNT_PRODUCER + "user-secret-p"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_CERT_PRODUCER), is("user-secret-p/user.crt"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_TLS_AUTH_KEY_PRODUCER), is("user-secret-p/user.key"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithTlsSameSecret() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                .editOrNewConsumer()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret-c").withCertificate("cert.crt").build())
                .endTls()
                .withAuthentication(
                        new KafkaClientAuthenticationTlsBuilder()
                                .withNewCertificateAndKey()
                                .withSecretName("my-secret-c")
                                .withCertificate("user.crt")
                                .withKey("user.key")
                                .endCertificateAndKey()
                                .build())
                .endConsumer()
                .editOrNewProducer()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret-p").withCertificate("cert.crt").build())
                .endTls()
                .withAuthentication(
                        new KafkaClientAuthenticationTlsBuilder()
                                .withNewCertificateAndKey()
                                .withSecretName("my-secret-p")
                                .withCertificate("user.crt")
                                .withKey("user.key")
                                .endCertificateAndKey()
                                .build())
                .endProducer()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().size(), is(4));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(0).getName(), is(AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("my-secret-p"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("my-secret-c"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithScramSha512Auth() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                .editOrNewProducer()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername("producer")
                        .withNewPasswordSecret()
                            .withSecretName("producer-secret")
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha512()
                .endProducer()
                .editOrNewConsumer()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername("consumer")
                        .withNewPasswordSecret()
                            .withSecretName("consumer-secret")
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha512()
                .endConsumer()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("producer-secret"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMakerCluster.PASSWORD_VOLUME_MOUNT_PRODUCER + "producer-secret"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER), is("scram-sha-512"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_PRODUCER), is("producer-secret/password"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_USERNAME_PRODUCER), is("producer"));

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("consumer-secret"));


        assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMakerCluster.PASSWORD_VOLUME_MOUNT_CONSUMER + "consumer-secret"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER), is("scram-sha-512"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_CONSUMER), is("consumer-secret/password"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_USERNAME_CONSUMER), is("consumer"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithPlain() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editOrNewProducer()
                        .withNewKafkaClientAuthenticationPlain()
                            .withUsername("producer")
                            .withNewPasswordSecret()
                                .withSecretName("producer-secret")
                                .withPassword("password")
                            .endPasswordSecret()
                        .endKafkaClientAuthenticationPlain()
                    .endProducer()
                    .editOrNewConsumer()
                        .withNewKafkaClientAuthenticationPlain()
                            .withUsername("consumer")
                            .withNewPasswordSecret()
                                .withSecretName("consumer-secret")
                                .withPassword("password")
                            .endPasswordSecret()
                        .endKafkaClientAuthenticationPlain()
                    .endConsumer()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("producer-secret"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaMirrorMakerCluster.PASSWORD_VOLUME_MOUNT_PRODUCER + "producer-secret"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER), is("plain"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_PRODUCER), is("producer-secret/password"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_USERNAME_PRODUCER), is("producer"));

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("consumer-secret"));


        assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaMirrorMakerCluster.PASSWORD_VOLUME_MOUNT_CONSUMER + "consumer-secret"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER), is("plain"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_CONSUMER), is("consumer-secret/password"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_USERNAME_CONSUMER), is("consumer"));
    }

    public void checkOwnerReference(OwnerReference ownerRef, HasMetadata resource)  {
        assertThat(resource.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(resource.getMetadata().getOwnerReferences().get(0), is(ownerRef));
    }

    @ParallelTest
    public void testTemplate() {
        Map<String, String> depLabels = TestUtils.map("l1", "v1", "l2", "v2",
                Labels.KUBERNETES_PART_OF_LABEL, "custom-part",
                Labels.KUBERNETES_MANAGED_BY_LABEL, "custom-managed-by");
        Map<String, String> expectedDepLabels = new HashMap<>(depLabels);
        expectedDepLabels.remove(Labels.KUBERNETES_MANAGED_BY_LABEL);
        Map<String, String> depAnots = TestUtils.map("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = TestUtils.map("l3", "v3", "l4", "v4");
        Map<String, String> podAnots = TestUtils.map("a3", "v3", "a4", "v4");

        Map<String, String> pdbLabels = TestUtils.map("l5", "v5", "l6", "v6");
        Map<String, String> pdbAnots = TestUtils.map("a5", "v5", "a6", "v6");

        Map<String, String> saLabels = TestUtils.map("l7", "v7", "l8", "v8");
        Map<String, String> saAnots = TestUtils.map("a7", "v7", "a8", "v8");

        HostAlias hostAlias1 = new HostAliasBuilder()
                .withHostnames("my-host-1", "my-host-2")
                .withIp("192.168.1.86")
                .build();
        HostAlias hostAlias2 = new HostAliasBuilder()
                .withHostnames("my-host-3")
                .withIp("192.168.1.87")
                .build();

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewDeployment()
                            .withNewMetadata()
                                .withLabels(depLabels)
                                .withAnnotations(depAnots)
                            .endMetadata()
                            .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                        .endDeployment()
                        .withNewPod()
                            .withNewMetadata()
                                .withLabels(podLabels)
                                .withAnnotations(podAnots)
                            .endMetadata()
                            .withPriorityClassName("top-priority")
                            .withSchedulerName("my-scheduler")
                            .withHostAliases(hostAlias1, hostAlias2)
                            .withEnableServiceLinks(false)
                        .endPod()
                        .withNewPodDisruptionBudget()
                            .withNewMetadata()
                                .withLabels(pdbLabels)
                                .withAnnotations(pdbAnots)
                            .endMetadata()
                        .endPodDisruptionBudget()
                        .withNewServiceAccount()
                            .withNewMetadata()
                                .withLabels(saLabels)
                                .withAnnotations(saAnots)
                            .endMetadata()
                        .endServiceAccount()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getMetadata().getLabels().entrySet().containsAll(expectedDepLabels.entrySet()), is(true));
        assertThat(dep.getMetadata().getAnnotations().entrySet().containsAll(depAnots.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));
        assertThat(dep.getSpec().getStrategy().getType(), is("Recreate"));
        assertThat(dep.getSpec().getStrategy().getRollingUpdate(), is(nullValue()));

        // Check Pods
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(dep.getSpec().getTemplate().getSpec().getHostAliases(), containsInAnyOrder(hostAlias1, hostAlias2));
        assertThat(dep.getSpec().getTemplate().getSpec().getEnableServiceLinks(), is(false));

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = mmc.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnots.entrySet()), is(true));

        // Check Service Account
        ServiceAccount sa = mmc.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnots.entrySet()), is(true));
    }

    @ParallelTest
    public void testGracePeriod() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withTerminationGracePeriodSeconds(123)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(Long.valueOf(123)));
    }

    @ParallelTest
    public void testDefaultGracePeriod() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource).build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(Long.valueOf(30)));
    }

    @ParallelTest
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret1, secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testImagePullSecretsFromCo() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        List<LocalObjectReference> secrets = new ArrayList<>(2);
        secrets.add(secret1);
        secrets.add(secret2);

        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, this.resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, secrets);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testImagePullSecretsFromBoth() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, singletonList(secret1));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testDefaultImagePullSecrets() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource).build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets(), is(nullValue()));
    }

    @ParallelTest
    public void testSecurityContext() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(notNullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(Long.valueOf(123)));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup(), is(Long.valueOf(456)));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser(), is(Long.valueOf(789)));
    }

    @ParallelTest
    public void testDefaultSecurityContext() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource).build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
    }

    @ParallelTest
    public void testPodDisruptionBudget() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPodDisruptionBudget()
                            .withMaxUnavailable(2)
                        .endPodDisruptionBudget()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        PodDisruptionBudget pdb = mmc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(2)));
    }

    @ParallelTest
    public void testDefaultPodDisruptionBudget() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource).build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        PodDisruptionBudget pdb = mmc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(1)));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kc.generateDeployment(Collections.EMPTY_MAP, true, ImagePullPolicy.ALWAYS, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        dep = kc.generateDeployment(Collections.EMPTY_MAP, true, ImagePullPolicy.IFNOTPRESENT, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
    }

    @ParallelTest
    public void testResources() {
        Map<String, Quantity> requests = new HashMap<>(2);
        requests.put("cpu", new Quantity("250m"));
        requests.put("memory", new Quantity("512Mi"));

        Map<String, Quantity> limits = new HashMap<>(2);
        limits.put("cpu", new Quantity("500m"));
        limits.put("memory", new Quantity("1024Mi"));

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(limits).withRequests(requests).build())
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertThat(cont.getResources().getLimits(), is(limits));
        assertThat(cont.getResources().getRequests(), is(requests));
    }

    @ParallelTest
    public void testJvmOptions() {
        Map<String, String> xx = new HashMap<>(2);
        xx.put("UseG1GC", "true");
        xx.put("MaxGCPauseMillis", "20");

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewJvmOptions()
                        .withNewXms("512m")
                        .withNewXmx("1024m")
                        .withXx(xx)
                    .endJvmOptions()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(Collections.EMPTY_MAP, true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertThat(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:+UseG1GC"), is(true));
        assertThat(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:MaxGCPauseMillis=20"), is(true));
        assertThat(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xmx1024m"), is(true));
        assertThat(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xms512m"), is(true));
    }

    @ParallelTest
    public void testDefaultProbes() {
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, this.resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(Collections.EMPTY_MAP, true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        Probe livenessProbe = cont.getLivenessProbe();
        Probe readinessProbe = cont.getReadinessProbe();

        assertThat(livenessProbe.getExec().getCommand().get(0), is("/opt/kafka/kafka_mirror_maker_liveness.sh"));
        assertThat(livenessProbe.getInitialDelaySeconds(), is(Integer.valueOf(60)));
        assertThat(livenessProbe.getTimeoutSeconds(), is(Integer.valueOf(5)));

        assertThat(readinessProbe.getExec().getCommand().size(), is(3));
        assertThat(readinessProbe.getExec().getCommand().get(0), is("test"));
        assertThat(readinessProbe.getExec().getCommand().get(1), is("-f"));
        assertThat(readinessProbe.getExec().getCommand().get(2), is("/tmp/mirror-maker-ready"));
        assertThat(readinessProbe.getInitialDelaySeconds(), is(Integer.valueOf(60)));
        assertThat(readinessProbe.getTimeoutSeconds(), is(Integer.valueOf(5)));

        assertThat(cont.getEnv().stream().filter(env -> "STRIMZI_READINESS_PERIOD".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").equals("10"), is(true));
        assertThat(cont.getEnv().stream().filter(env -> "STRIMZI_LIVENESS_PERIOD".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").equals("10"), is(true));
    }

    @ParallelTest
    public void testConfiguredProbes() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewLivenessProbe()
                        .withInitialDelaySeconds(120)
                        .withTimeoutSeconds(10)
                        .withPeriodSeconds(60)
                    .endLivenessProbe()
                    .withNewReadinessProbe()
                        .withInitialDelaySeconds(121)
                        .withTimeoutSeconds(11)
                        .withPeriodSeconds(61)
                    .endReadinessProbe()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(Collections.EMPTY_MAP, true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        Probe livenessProbe = cont.getLivenessProbe();
        Probe readinessProbe = cont.getReadinessProbe();

        assertThat(livenessProbe.getExec().getCommand().get(0), is("/opt/kafka/kafka_mirror_maker_liveness.sh"));
        assertThat(livenessProbe.getInitialDelaySeconds(), is(Integer.valueOf(120)));
        assertThat(livenessProbe.getTimeoutSeconds(), is(Integer.valueOf(10)));
        assertThat(livenessProbe.getPeriodSeconds(), is(Integer.valueOf(60)));

        assertThat(readinessProbe.getExec().getCommand().size(), is(3));
        assertThat(readinessProbe.getExec().getCommand().get(0), is("test"));
        assertThat(readinessProbe.getExec().getCommand().get(1), is("-f"));
        assertThat(readinessProbe.getExec().getCommand().get(2), is("/tmp/mirror-maker-ready"));
        assertThat(readinessProbe.getInitialDelaySeconds(), is(Integer.valueOf(121)));
        assertThat(readinessProbe.getTimeoutSeconds(), is(Integer.valueOf(11)));
        assertThat(readinessProbe.getPeriodSeconds(), is(Integer.valueOf(61)));

        assertThat(cont.getEnv().stream().filter(env -> "STRIMZI_READINESS_PERIOD".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").equals("61"), is(true));
        assertThat(cont.getEnv().stream().filter(env -> "STRIMZI_LIVENESS_PERIOD".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").equals("60"), is(true));
    }

    @ParallelTest
    public void testKafkaMMContainerEnvVars() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = "TEST_ENV_2";
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate kafkaMMContainer = new ContainerTemplate();
        kafkaMMContainer.setEnv(testEnvs);

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withMirrorMakerContainer(kafkaMMContainer)
                    .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS).getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));
    }

    @ParallelTest
    public void testKafkaMMContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_CONSUMER;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_INCLUDE;
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate kafkaMMContainer = new ContainerTemplate();
        kafkaMMContainer.setEnv(testEnvs);

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                .withNewTemplate()
                .withMirrorMakerContainer(kafkaMMContainer)
                .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS).getEnvVars();

        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));
        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(false));
    }

    @ParallelTest
    public void testTracing() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                .endSpec()
                .build();
        KafkaMirrorMakerCluster mmc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = mmc.generateDeployment(Collections.EMPTY_MAP, true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertThat(cont.getEnv().stream().filter(env -> KafkaMirrorMakerCluster.ENV_VAR_STRIMZI_TRACING.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").equals("jaeger"), is(true));
        assertThat(cont.getEnv().stream().filter(env -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_CONFIGURATION_CONSUMER.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("interceptor.classes=io.opentracing.contrib.kafka.TracingConsumerInterceptor"), is(true));
        assertThat(cont.getEnv().stream().filter(env -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_CONFIGURATION_PRODUCER.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("interceptor.classes=io.opentracing.contrib.kafka.TracingProducerInterceptor"), is(true));
    }

    @ParallelTest
    public void testGenerateDeploymentWithConsumerOAuthWithAccessToken() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editConsumer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withNewAccessToken()
                                            .withSecretName("my-token-secret")
                                            .withKey("my-token-key")
                                        .endAccessToken()
                                        .build())
                        .endConsumer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_ACCESS_TOKEN_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-token-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_ACCESS_TOKEN_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-token-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValue().isEmpty(), is(true));
    }

    @ParallelTest
    public void testGenerateDeploymentWithConsumerOAuthWithRefreshToken() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editConsumer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewRefreshToken()
                                            .withSecretName("my-token-secret")
                                            .withKey("my-token-key")
                                        .endRefreshToken()
                                        .build())
                    .endConsumer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_REFRESH_TOKEN_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-token-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_REFRESH_TOKEN_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-token-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server")));
    }

    @ParallelTest
    public void testGenerateDeploymentWithConsumerOAuthWithClientSecret() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editConsumer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewClientSecret()
                                        .withSecretName("my-secret-secret")
                                        .withKey("my-secret-key")
                                        .endClientSecret()
                                        .build())
                    .endConsumer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server")));
    }

    @ParallelTest
    public void testGenerateDeploymentWithConsumerOAuthWithMissingClientSecret() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editConsumer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .build())
                    .endConsumer()
                .endSpec()
                .build();

            KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        });
    }

    @ParallelTest
    public void testGenerateDeploymentWithConsumerOAuthWithMissingUri() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editConsumer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withNewClientSecret()
                                        .withSecretName("my-secret-secret")
                                        .withKey("my-secret-key")
                                        .endClientSecret()
                                        .build())
                    .endConsumer()
                .endSpec()
                .build();

            KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        });
    }

    @ParallelTest
    public void testGenerateDeploymentWithConsumerOAuthWithTls() {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editConsumer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewClientSecret()
                                        .withSecretName("my-secret-secret")
                                        .withKey("my-secret-key")
                                        .endClientSecret()
                                        .withDisableTlsHostnameVerification(true)
                                        .withTlsTrustedCertificates(cert1, cert2, cert3)
                                        .build())
                    .endConsumer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "")));

        // Volume mounts
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "consumer-oauth-certs-0".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_CONSUMER + "/first-certificate-0"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "consumer-oauth-certs-1".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_CONSUMER + "/second-certificate-1"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "consumer-oauth-certs-2".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_CONSUMER + "/first-certificate-2"));

        // Volumes
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("ca.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("tls.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("ca2.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithProducerOAuthWithAccessToken() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editProducer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withNewAccessToken()
                                            .withSecretName("my-token-secret")
                                            .withKey("my-token-key")
                                        .endAccessToken()
                                        .build())
                    .endProducer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_ACCESS_TOKEN_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-token-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_ACCESS_TOKEN_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-token-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValue().isEmpty(), is(true));
    }

    @ParallelTest
    public void testGenerateDeploymentWithProducerOAuthWithRefreshToken() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editProducer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewRefreshToken()
                                            .withSecretName("my-token-secret")
                                            .withKey("my-token-key")
                                        .endRefreshToken()
                                        .build())
                    .endProducer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_REFRESH_TOKEN_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-token-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_REFRESH_TOKEN_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-token-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server")));
    }

    @ParallelTest
    public void testGenerateDeploymentWithProducerOAuthWithClientSecret() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editProducer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewClientSecret()
                                            .withSecretName("my-secret-secret")
                                            .withKey("my-secret-key")
                                        .endClientSecret()
                                        .build())
                    .endProducer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server")));
    }

    @ParallelTest
    public void testGenerateDeploymentWithProducerOAuthWithMissingClientSecret() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editProducer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .build())
                    .endProducer()
                .endSpec()
                .build();

            KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        });
    }

    @ParallelTest
    public void testGenerateDeploymentWithProducerOAuthWithMissingUri() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editProducer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withNewClientSecret()
                                            .withSecretName("my-secret-secret")
                                            .withKey("my-secret-key")
                                        .endClientSecret()
                                        .build())
                    .endProducer()
                .endSpec()
                .build();

            KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        });
    }

    @ParallelTest
    public void testGenerateDeploymentWithProducerOAuthWithTls() {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editProducer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewClientSecret()
                                            .withSecretName("my-secret-secret")
                                            .withKey("my-secret-key")
                                        .endClientSecret()
                                        .withDisableTlsHostnameVerification(true)
                                        .withTlsTrustedCertificates(cert1, cert2, cert3)
                                        .build())
                    .endProducer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "")));

        // Volume mounts
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "producer-oauth-certs-0".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_PRODUCER + "/first-certificate-0"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "producer-oauth-certs-1".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_PRODUCER + "/second-certificate-1"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "producer-oauth-certs-2".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_PRODUCER + "/first-certificate-2"));

        // Volumes
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("ca.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("tls.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("ca2.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithBothSidedOAuthWithTls() {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .editConsumer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewClientSecret()
                                            .withSecretName("my-secret-secret")
                                            .withKey("my-secret-key")
                                        .endClientSecret()
                                        .withDisableTlsHostnameVerification(true)
                                        .withTlsTrustedCertificates(cert1, cert2, cert3)
                                        .build())
                    .endConsumer()
                    .editProducer()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .withNewClientSecret()
                                            .withSecretName("my-secret-secret")
                                            .withKey("my-secret-key")
                                        .endClientSecret()
                                        .withDisableTlsHostnameVerification(true)
                                        .withTlsTrustedCertificates(cert1, cert2, cert3)
                                        .build())
                    .endProducer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "")));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "")));

        // Volume mounts
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "producer-oauth-certs-0".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_PRODUCER + "/first-certificate-0"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "producer-oauth-certs-1".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_PRODUCER + "/second-certificate-1"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "producer-oauth-certs-2".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_PRODUCER + "/first-certificate-2"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "consumer-oauth-certs-0".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_CONSUMER + "/first-certificate-0"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "consumer-oauth-certs-1".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_CONSUMER + "/second-certificate-1"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "consumer-oauth-certs-2".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaMirrorMakerCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT_CONSUMER + "/first-certificate-2"));


        // Volumes
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("ca.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("tls.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("ca2.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "producer-oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("ca.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("tls.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("ca2.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "consumer-oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthUsingOpaqueTokens() {
        KafkaMirrorMaker resource = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                .editConsumer()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withNewClientSecret()
                                .withSecretName("my-secret-secret")
                                .withKey("my-secret-key")
                                .endClientSecret()
                                .withAccessTokenIsJwt(false)
                                .withMaxTokenExpirySeconds(600)
                                .build())
                .endConsumer()
                .editProducer()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withNewClientSecret()
                                .withSecretName("my-secret-secret")
                                .withKey("my-secret-key")
                                .endClientSecret()
                                .withAccessTokenIsJwt(false)
                                .withMaxTokenExpirySeconds(600)
                                .build())
                .endProducer()
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kc = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_CONSUMER.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ClientConfig.OAUTH_ACCESS_TOKEN_IS_JWT, "false", ClientConfig.OAUTH_MAX_TOKEN_EXPIRY_SECONDS, "600")));

        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaMirrorMakerCluster.ENV_VAR_KAFKA_MIRRORMAKER_OAUTH_CONFIG_PRODUCER.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ClientConfig.OAUTH_ACCESS_TOKEN_IS_JWT, "false", ClientConfig.OAUTH_MAX_TOKEN_EXPIRY_SECONDS, "600")));
    }

    @ParallelTest
    public void testMetricsParsingFromConfigMap() {
        MetricsConfig metrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-metrics-configuration").withKey("config.yaml").build())
                .endValueFrom()
                .build();

        KafkaMirrorMaker mirrorMaker = new KafkaMirrorMakerBuilder(this.resource)
                .editSpec()
                    .withMetricsConfig(metrics)
                .endSpec()
                .build();

        KafkaMirrorMakerCluster kmm = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, mirrorMaker, VERSIONS);

        assertThat(kmm.isMetricsEnabled(), is(true));
        assertThat(kmm.getMetricsConfigInCm(), is(metrics));
    }

    @ParallelTest
    public void testMetricsParsingNoMetrics() {
        KafkaMirrorMakerCluster kmm = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, this.resource, VERSIONS);

        assertThat(kmm.isMetricsEnabled(), is(false));
        assertThat(kmm.getMetricsConfigInCm(), is(nullValue()));
    }
}
