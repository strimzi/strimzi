/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.SecretKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.MetricsConfig;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.Rack;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationTlsBuilder;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnv;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnvBuilder;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationVolumeSource;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationVolumeSourceBuilder;
import io.strimzi.api.kafka.model.template.ContainerTemplate;
import io.strimzi.api.kafka.model.template.DeploymentStrategy;
import io.strimzi.api.kafka.model.template.IpFamily;
import io.strimzi.api.kafka.model.template.IpFamilyPolicy;
import io.strimzi.kafka.oauth.client.ClientConfig;
import io.strimzi.kafka.oauth.server.ServerConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity", "checkstyle:ClassDataAbstractionCoupling"})
@ParallelSuite
public class KafkaConnectClusterTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 2;
    private final String image = "my-image:latest";
    private final int healthDelay = 100;
    private final int healthTimeout = 10;
    private final String metricsCmJson = "{\"animal\":\"wombat\"}";
    private final String metricsCMName = "metrics-cm";
    private final ConfigMap metricsCM = io.strimzi.operator.cluster.TestUtils.getJmxMetricsCm(metricsCmJson, metricsCMName, "metrics-config.yml");
    private final JmxPrometheusExporterMetrics jmxMetricsConfig = io.strimzi.operator.cluster.TestUtils.getJmxPrometheusExporterMetrics("metrics-config.yml", metricsCMName);
    private final String configurationJson = "{\"foo\":\"bar\"}";
    private final String bootstrapServers = "foo-kafka:9092";
    private final String kafkaHeapOpts = "-Xms" + AbstractModel.DEFAULT_JVM_XMS;

    private final OrderedProperties defaultConfiguration = new OrderedProperties()
            .addPair("offset.storage.topic", "connect-cluster-offsets")
            .addPair("value.converter", "org.apache.kafka.connect.json.JsonConverter")
            .addPair("config.storage.topic", "connect-cluster-configs")
            .addPair("key.converter", "org.apache.kafka.connect.json.JsonConverter")
            .addPair("group.id", "connect-cluster")
            .addPair("status.storage.topic", "connect-cluster-status");

    private final OrderedProperties expectedConfiguration = new OrderedProperties()
            .addMapPairs(defaultConfiguration.asMap())
            .addPair("foo", "bar");

    private final KafkaConnect resource = new KafkaConnectBuilder(ResourceUtils.createEmptyKafkaConnect(namespace, cluster))
            .withNewSpec()
                .withConfig((Map<String, Object>) TestUtils.fromJson(configurationJson, Map.class))
                .withImage(image)
                .withReplicas(replicas)
                .withReadinessProbe(new Probe(healthDelay, healthTimeout))
                .withLivenessProbe(new Probe(healthDelay, healthTimeout))
                .withBootstrapServers(bootstrapServers)
            .endSpec()
            .build();

    private final KafkaConnect resourceWithMetrics = new KafkaConnectBuilder(resource)
            .editSpec()
                .withMetricsConfig(jmxMetricsConfig)
            .endSpec()
            .build();

    private final KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resourceWithMetrics, VERSIONS);

    @ParallelTest
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = kc.generateMetricsAndLogConfigMap(new MetricsAndLogging(metricsCM, null));
        checkMetricsConfigMap(metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertThat(metricsCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_METRICS), is(metricsCmJson));
    }

    private Map<String, String> expectedLabels(String name)    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, name,
                Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND,
                Labels.KUBERNETES_NAME_LABEL, KafkaConnectCluster.APPLICATION_NAME,
                Labels.KUBERNETES_INSTANCE_LABEL, this.cluster,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + this.cluster,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziSelectorLabels().toMap();
    }

    private Map<String, String> expectedLabels()    {
        return expectedLabels(KafkaConnectResources.deploymentName(cluster));
    }

    protected List<EnvVar> getExpectedEnvVars() {

        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_CONFIGURATION).withValue(expectedConfiguration.asPairs()).build());
        expected.add(new EnvVarBuilder().withName(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_METRICS_ENABLED).withValue(String.valueOf(true)).build());
        expected.add(new EnvVarBuilder().withName(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_BOOTSTRAP_SERVERS).withValue(bootstrapServers).build());
        expected.add(new EnvVarBuilder().withName(KafkaConnectCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED).withValue(Boolean.toString(AbstractModel.DEFAULT_JVM_GC_LOGGING_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS).withValue(kafkaHeapOpts).build());
        return expected;
    }

    @ParallelTest
    public void testDefaultValues() {
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ResourceUtils.createEmptyKafkaConnect(namespace, cluster), VERSIONS);

        assertThat(kc.image, is(KafkaVersionTestUtils.DEFAULT_KAFKA_CONNECT_IMAGE));
        assertThat(kc.replicas, is(KafkaConnectCluster.DEFAULT_REPLICAS));
        assertThat(kc.readinessProbeOptions.getInitialDelaySeconds(), is(KafkaConnectCluster.DEFAULT_HEALTHCHECK_DELAY));
        assertThat(kc.readinessProbeOptions.getTimeoutSeconds(), is(KafkaConnectCluster.DEFAULT_HEALTHCHECK_TIMEOUT));
        assertThat(kc.livenessProbeOptions.getInitialDelaySeconds(), is(KafkaConnectCluster.DEFAULT_HEALTHCHECK_DELAY));
        assertThat(kc.livenessProbeOptions.getTimeoutSeconds(), is(KafkaConnectCluster.DEFAULT_HEALTHCHECK_TIMEOUT));
        assertThat(kc.getConfiguration().asOrderedProperties(), is(defaultConfiguration));
    }

    @ParallelTest
    public void testFromCrd() {
        assertThat(kc.replicas, is(replicas));
        assertThat(kc.image, is(image));
        assertThat(kc.readinessProbeOptions.getInitialDelaySeconds(), is(healthDelay));
        assertThat(kc.readinessProbeOptions.getTimeoutSeconds(), is(healthTimeout));
        assertThat(kc.livenessProbeOptions.getInitialDelaySeconds(), is(healthDelay));
        assertThat(kc.livenessProbeOptions.getTimeoutSeconds(), is(healthTimeout));
        assertThat(kc.getConfiguration().asOrderedProperties(), is(expectedConfiguration));
        assertThat(kc.bootstrapServers, is(bootstrapServers));
    }

    @ParallelTest
    public void testEnvVars()   {
        assertThat(kc.getEnvVars(), is(getExpectedEnvVars()));
    }

    @ParallelTest
    public void testGenerateService()   {
        Service svc = kc.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedLabels(kc.getServiceName())));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(Integer.valueOf(KafkaConnectCluster.REST_API_PORT)));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(KafkaConnectCluster.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(svc.getMetadata().getAnnotations().size(), is(0));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(svc.getSpec().getIpFamilies(), is(emptyList()));

        checkOwnerReference(kc.createOwnerReference(), svc);
    }

    @ParallelTest
    public void testGenerateServiceWithoutMetrics()   {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withMetricsConfig(null)
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Service svc = kc.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedLabels(kc.getServiceName())));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(Integer.valueOf(KafkaConnectCluster.REST_API_PORT)));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(KafkaConnectCluster.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));

        assertThat(svc.getMetadata().getAnnotations().containsKey("prometheus.io/port"), is(false));
        assertThat(svc.getMetadata().getAnnotations().containsKey("prometheus.io/scrape"), is(false));
        assertThat(svc.getMetadata().getAnnotations().containsKey("prometheus.io/path"), is(false));

        checkOwnerReference(kc.createOwnerReference(), svc);
    }

    @ParallelTest
    public void testGenerateDeployment()   {
        Deployment dep = kc.generateDeployment(new HashMap<>(), true, null, null);

        checkDeployment(dep, resource);
    }

    @ParallelTest
    public void testGenerateDeploymentWithRack() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resourceWithMetrics)
                .editOrNewSpec()
                    .withNewRack()
                        .withTopologyKey("topology-key")
                    .endRack()
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment deployment = kc.generateDeployment(new HashMap<>(), false, null, null);

        checkDeployment(deployment, resource);
    }

    @ParallelTest
    public void withAffinity() throws IOException {
        ResourceTester<KafkaConnect, KafkaConnectCluster> resourceTester = new ResourceTester<>(KafkaConnect.class, VERSIONS, (connect, lookup) -> KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, connect, lookup), this.getClass().getSimpleName() + ".withAffinity");
        resourceTester
            .assertDesiredResource("-Deployment.yaml", kcc -> kcc.generateDeployment(new HashMap<String, String>(), true, null, null).getSpec().getTemplate().getSpec().getAffinity());
    }

    @ParallelTest
    public void withTolerations() throws IOException {
        ResourceTester<KafkaConnect, KafkaConnectCluster> resourceTester = new ResourceTester<>(KafkaConnect.class, VERSIONS, (connect, lookup) -> KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, connect, lookup), this.getClass().getSimpleName() + ".withTolerations");
        resourceTester
            .assertDesiredResource("-Deployment.yaml", kcc -> kcc.generateDeployment(new HashMap<String, String>(), true, null, null).getSpec().getTemplate().getSpec().getTolerations());
    }

    @ParallelTest
    public void testGenerateDeploymentWithTls() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("new-cert.crt").build())
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-another-secret").withCertificate("another-cert.crt").build())
                .endTls()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("my-secret"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("my-another-secret"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
        assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-another-secret"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TRUSTED_CERTS),
                is("my-secret/cert.crt;my-secret/new-cert.crt;my-another-secret/another-cert.crt"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS), is("true"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithTlsAuth() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .endTls()
                .withAuthentication(
                        new KafkaClientAuthenticationTlsBuilder()
                                .withNewCertificateAndKey()
                                .withSecretName("user-secret")
                                .withCertificate("user.crt")
                                .withKey("user.key")
                                .endCertificateAndKey()
                                .build())
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("user-secret"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "user-secret"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS_AUTH_CERT), is("user-secret/user.crt"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS_AUTH_KEY), is("user-secret/user.key"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS), is("true"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithTlsSameSecret() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .editOrNewTls()
                .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .endTls()
                .withAuthentication(
                        new KafkaClientAuthenticationTlsBuilder()
                                .withNewCertificateAndKey()
                                .withSecretName("my-secret")
                                .withCertificate("user.crt")
                                .withKey("user.key")
                                .endCertificateAndKey()
                                .build())
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);

        // 3 = 1 temp volume + 1 volume from logging/metrics + just 1 from above certs Secret
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().size(), is(3));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("my-secret"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithScramSha512Auth() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername("user1")
                        .withNewPasswordSecret()
                            .withSecretName("user1-secret")
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha512()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("user1-secret"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE), is("user1-secret/password"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME), is("user1"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM), is("scram-sha-512"));
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for SCRAM SHA 512 client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @ParallelTest
    public void testGenerateDeploymentWithScramSha512AuthAndTLSSameSecret() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
            .editSpec()
                .editOrNewTls()
                    .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .endTls()
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername("user1")
                    .withNewPasswordSecret()
                        .withSecretName("my-secret")
                        .withPassword("user1.password")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationScramSha512()
            .endSpec()
            .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().size(), is(3));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(0).getName(), is(AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName(), is("kafka-metrics-and-logging"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("my-secret"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().size(), is(4));
        assertThat(containers.get(0).getVolumeMounts().get(0).getName(), is(AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(containers.get(0).getVolumeMounts().get(0).getMountPath(), is(AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
        assertThat(containers.get(0).getVolumeMounts().get(1).getName(), is("kafka-metrics-and-logging"));
        assertThat(containers.get(0).getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
        assertThat(containers.get(0).getVolumeMounts().get(2).getName(), is("my-secret"));
        assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
        assertThat(containers.get(0).getVolumeMounts().get(3).getName(), is("my-secret"));
        assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "my-secret"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE, "my-secret/user1.password"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME, "user1"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM, "scram-sha-512"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS, "true"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithPlainAuth() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withNewKafkaClientAuthenticationPlain()
                    .withUsername("user1")
                    .withNewPasswordSecret()
                        .withSecretName("user1-secret")
                        .withPassword("password")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationPlain()
            .endSpec()
            .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("user1-secret"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE), is("user1-secret/password"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME), is("user1"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM), is("plain"));
    }

    /**
     * This test uses the same secret to hold the certs for TLS and the credentials for plain client authentication. It checks that
     * the volumes and volume mounts that reference the secret are correctly created and that each volume name is only created once - volumes
     * with duplicate names will cause Kubernetes to reject the deployment.
     */
    @ParallelTest
    public void testGenerateDeploymentWithPlainAuthAndTLSSameSecret() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
            .editSpec()
                .editOrNewTls()
                    .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                .endTls()
                .withNewKafkaClientAuthenticationPlain()
                    .withUsername("user1")
                    .withNewPasswordSecret()
                        .withSecretName("my-secret")
                        .withPassword("user1.password")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationPlain()
            .endSpec()
            .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().size(), is(3));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(0).getName(), is(AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(1).getName(), is("kafka-metrics-and-logging"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("my-secret"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().size(), is(4));
        assertThat(containers.get(0).getVolumeMounts().get(0).getName(), is(AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(containers.get(0).getVolumeMounts().get(0).getMountPath(), is(AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
        assertThat(containers.get(0).getVolumeMounts().get(1).getName(), is("kafka-metrics-and-logging"));
        assertThat(containers.get(0).getVolumeMounts().get(1).getMountPath(), is("/opt/kafka/custom-config/"));
        assertThat(containers.get(0).getVolumeMounts().get(2).getName(), is("my-secret"));
        assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaConnectCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
        assertThat(containers.get(0).getVolumeMounts().get(3).getName(), is("my-secret"));
        assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaConnectCluster.PASSWORD_VOLUME_MOUNT + "my-secret"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE, "my-secret/user1.password"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_USERNAME, "user1"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM, "plain"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)), hasEntry(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS, "true"));
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

        Map<String, String> svcLabels = TestUtils.map("l5", "v5", "l6", "v6");
        Map<String, String> svcAnots = TestUtils.map("a5", "v5", "a6", "v6");

        Map<String, String> pdbLabels = TestUtils.map("l7", "v7", "l8", "v8");
        Map<String, String> pdbAnots = TestUtils.map("a7", "v7", "a8", "v8");

        Map<String, String> crbLabels = TestUtils.map("l9", "v9", "l10", "v10");
        Map<String, String> crbAnots = TestUtils.map("a9", "v9", "a10", "v10");

        Map<String, String> saLabels = TestUtils.map("l11", "v11", "l12", "v12");
        Map<String, String> saAnots = TestUtils.map("a11", "v11", "a12", "v12");

        HostAlias hostAlias1 = new HostAliasBuilder()
                .withHostnames("my-host-1", "my-host-2")
                .withIp("192.168.1.86")
                .build();
        HostAlias hostAlias2 = new HostAliasBuilder()
                .withHostnames("my-host-3")
                .withIp("192.168.1.87")
                .build();

        TopologySpreadConstraint tsc1 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/zone")
                .withMaxSkew(1)
                .withWhenUnsatisfiable("DoNotSchedule")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(singletonMap("label", "value")).build())
                .build();

        TopologySpreadConstraint tsc2 = new TopologySpreadConstraintBuilder()
                .withTopologyKey("kubernetes.io/hostname")
                .withMaxSkew(2)
                .withWhenUnsatisfiable("ScheduleAnyway")
                .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(singletonMap("label", "value")).build())
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewRack("my-topology-key")
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
                            .withTopologySpreadConstraints(tsc1, tsc2)
                            .withEnableServiceLinks(false)
                        .endPod()
                        .withNewApiService()
                            .withNewMetadata()
                                .withLabels(svcLabels)
                                .withAnnotations(svcAnots)
                            .endMetadata()
                            .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
                            .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                        .endApiService()
                        .withNewPodDisruptionBudget()
                            .withNewMetadata()
                                .withLabels(pdbLabels)
                                .withAnnotations(pdbAnots)
                            .endMetadata()
                        .endPodDisruptionBudget()
                        .withNewClusterRoleBinding()
                            .withNewMetadata()
                                .withLabels(crbLabels)
                                .withAnnotations(crbAnots)
                            .endMetadata()
                        .endClusterRoleBinding()
                        .withNewServiceAccount()
                            .withNewMetadata()
                                .withLabels(saLabels)
                                .withAnnotations(saAnots)
                            .endMetadata()
                        .endServiceAccount()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
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
        assertThat(dep.getSpec().getTemplate().getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1, tsc2));
        assertThat(dep.getSpec().getTemplate().getSpec().getEnableServiceLinks(), is(false));

        // Check Service
        Service svc = kc.generateService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnots.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnots.entrySet()), is(true));

        // Check ClusterRoleBinding
        ClusterRoleBinding crb = kc.generateClusterRoleBinding();
        assertThat(crb.getMetadata().getLabels().entrySet().containsAll(crbLabels.entrySet()), is(true));
        assertThat(crb.getMetadata().getAnnotations().entrySet().containsAll(crbAnots.entrySet()), is(true));

        // Check Service Account
        ServiceAccount sa = kc.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnots.entrySet()), is(true));
    }

    @ParallelTest
    public void testExternalConfigurationSecretEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withSecretKeyRef(new SecretKeySelectorBuilder().withName("my-secret").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                    .withEnv(env)
                .endExternalConfiguration()

                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        List<EnvVar> envs = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).collect(Collectors.toList());
        assertThat(selected.size(), is(1));
        assertThat(selected.get(0).getName(), is("MY_ENV_VAR"));
        assertThat(selected.get(0).getValueFrom().getSecretKeyRef(), is(env.getValueFrom().getSecretKeyRef()));
    }

    @ParallelTest
    public void testExternalConfigurationConfigEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-map").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                    .withEnv(env)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        List<EnvVar> envs = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).collect(Collectors.toList());
        assertThat(selected.size(), is(1));
        assertThat(selected.get(0).getName(), is("MY_ENV_VAR"));
        assertThat(selected.get(0).getValueFrom().getConfigMapKeyRef(), is(env.getValueFrom().getConfigMapKeyRef()));
    }

    @ParallelTest
    public void testExternalConfigurationSecretVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .withSecret(new SecretVolumeSourceBuilder().withSecretName("my-secret").build())
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(1));
        assertThat(selected.get(0).getName(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
        assertThat(selected.get(0).getSecret(), is(volume.getSecret()));

        List<VolumeMount> volumeMounts = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(1));
        assertThat(selectedVolumeMounts.get(0).getName(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
        assertThat(selectedVolumeMounts.get(0).getMountPath(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my-volume"));
    }

    @ParallelTest
    public void testExternalConfigurationSecretVolumesWithDots() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my.volume")
                .withSecret(new SecretVolumeSourceBuilder().withSecretName("my.secret").build())
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        List<Volume> selected = volumes.stream().filter(vol -> vol.getName().startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(1));
        assertThat(selected.get(0).getName(), startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
        assertThat(selected.get(0).getSecret(), is(volume.getSecret()));

        List<VolumeMount> volumeMounts = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(1));
        assertThat(selectedVolumeMounts.get(0).getName(), startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
        assertThat(selectedVolumeMounts.get(0).getMountPath(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my.volume"));
    }

    @ParallelTest
    public void testExternalConfigurationConfigVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .withConfigMap(new ConfigMapVolumeSourceBuilder().withName("my-map").build())
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(1));
        assertThat(selected.get(0).getName(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
        assertThat(selected.get(0).getConfigMap(), is(volume.getConfigMap()));

        List<VolumeMount> volumeMounts = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(1));
        assertThat(selectedVolumeMounts.get(0).getName(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
        assertThat(selectedVolumeMounts.get(0).getMountPath(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my-volume"));
    }

    @ParallelTest
    public void testExternalConfigurationConfigVolumesWithDots() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my.volume")
                .withConfigMap(new ConfigMapVolumeSourceBuilder().withName("my.map").build())
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        List<Volume> selected = volumes.stream().filter(vol -> vol.getName().startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(1));
        assertThat(selected.get(0).getName(), startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
        assertThat(selected.get(0).getConfigMap(), is(volume.getConfigMap()));

        List<VolumeMount> volumeMounts = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        List<VolumeMount> selectedVolumeMounts = volumeMounts.stream().filter(vol -> vol.getName().startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(1));
        assertThat(selectedVolumeMounts.get(0).getName(), startsWith(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume"));
        assertThat(selectedVolumeMounts.get(0).getMountPath(), is(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + "my.volume"));
    }

    @ParallelTest
    public void testExternalConfigurationInvalidVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .withConfigMap(new ConfigMapVolumeSourceBuilder().withName("my-map").build())
                .withSecret(new SecretVolumeSourceBuilder().withSecretName("my-secret").build())
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewExternalConfiguration()
                        .withVolumes(volume)
                    .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(0));

        List<VolumeMount> volumeMounths = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        List<VolumeMount> selectedVolumeMounts = volumeMounths.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(0));
    }

    @ParallelTest
    public void testNoExternalConfigurationVolumes() {
        ExternalConfigurationVolumeSource volume = new ExternalConfigurationVolumeSourceBuilder()
                .withName("my-volume")
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                .withVolumes(volume)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        List<Volume> selected = volumes.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(0));

        List<VolumeMount> volumeMounths = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        List<VolumeMount> selectedVolumeMounths = volumeMounths.stream().filter(vol -> vol.getName().equals(KafkaConnectCluster.EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + "my-volume")).collect(Collectors.toList());
        assertThat(selected.size(), is(0));
    }

    @ParallelTest
    public void testInvalidExternalConfigurationEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-map").withKey("my-key").withOptional(false).build())
                    .withSecretKeyRef(new SecretKeySelectorBuilder().withName("my-secret").withKey("my-key").withOptional(false).build())
                .endValueFrom()
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                .withEnv(env)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        List<EnvVar> envs = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).collect(Collectors.toList());
        assertThat(selected.size(), is(0));
    }

    @ParallelTest
    public void testNoExternalConfigurationEnvs() {
        ExternalConfigurationEnv env = new ExternalConfigurationEnvBuilder()
                .withName("MY_ENV_VAR")
                .withNewValueFrom()
                .endValueFrom()
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withNewExternalConfiguration()
                .withEnv(env)
                .endExternalConfiguration()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        List<EnvVar> envs = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        List<EnvVar> selected = envs.stream().filter(var -> var.getName().equals("MY_ENV_VAR")).collect(Collectors.toList());
        assertThat(selected.size(), is(0));
    }

    @ParallelTest
    public void testGracePeriod() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withTerminationGracePeriodSeconds(123)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(Long.valueOf(123)));
    }

    @ParallelTest
    public void testDefaultGracePeriod() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource).build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(Long.valueOf(30)));
    }

    @ParallelTest
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret1, secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testImagePullSecretsCO() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        List<LocalObjectReference> secrets = new ArrayList<>(2);
        secrets.add(secret1);
        secrets.add(secret2);

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, this.resource, VERSIONS);

        Deployment dep = kc.generateDeployment(emptyMap(), true, null, secrets);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testImagePullSecretsBoth() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kc.generateDeployment(emptyMap(), true, null, singletonList(secret1));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testDefaultImagePullSecrets() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource).build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets(), is(nullValue()));
    }

    @ParallelTest
    public void testSecurityContext() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(notNullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(Long.valueOf(123)));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup(), is(Long.valueOf(456)));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser(), is(Long.valueOf(789)));
    }

    @ParallelTest
    public void testDefaultSecurityContext() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource).build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
    }

    @ParallelTest
    public void testPodDisruptionBudget() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPodDisruptionBudget()
                            .withMaxUnavailable(2)
                        .endPodDisruptionBudget()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(2)));
    }

    @ParallelTest
    public void testDefaultPodDisruptionBudget() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource).build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        PodDisruptionBudget pdb = kc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(1)));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

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

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(limits).withRequests(requests).build())
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kc.generateDeployment(Collections.EMPTY_MAP, true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertThat(cont.getResources().getLimits(), is(limits));
        assertThat(cont.getResources().getRequests(), is(requests));
    }

    @ParallelTest
    public void testJvmOptions() {
        Map<String, String> xx = new HashMap<>(2);
        xx.put("UseG1GC", "true");
        xx.put("MaxGCPauseMillis", "20");

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewJvmOptions()
                        .withNewXms("512m")
                        .withNewXmx("1024m")
                        .withXx(xx)
                    .endJvmOptions()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kc.generateDeployment(Collections.EMPTY_MAP, true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertThat(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:+UseG1GC"), is(true));
        assertThat(cont.getEnv().stream().filter(env -> "KAFKA_JVM_PERFORMANCE_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-XX:MaxGCPauseMillis=20"), is(true));
        assertThat(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xmx1024m"), is(true));
        assertThat(cont.getEnv().stream().filter(env -> "KAFKA_HEAP_OPTS".equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("-Xms512m"), is(true));
    }

    @ParallelTest
    public void testKafkaConnectContainerEnvVars() {

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
        ContainerTemplate kafkaConnectContainer = new ContainerTemplate();
        kafkaConnectContainer.setEnv(testEnvs);

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withConnectContainer(kafkaConnectContainer)
                    .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS).getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));
    }

    @ParallelTest
    public void testKafkaContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_CONFIGURATION;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_BOOTSTRAP_SERVERS;
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate kafkaConnectContainer = new ContainerTemplate();
        kafkaConnectContainer.setEnv(testEnvs);

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withConnectContainer(kafkaConnectContainer)
                    .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS).getEnvVars();

        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));
        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(false));
    }

    @ParallelTest
    public void testKafkaConnectContainerSecurityContext() {

        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addNewDrop("ALL")
                .endCapabilities()
                .build();

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .editOrNewTemplate()
                        .withNewConnectContainer()
                            .withSecurityContext(securityContext)
                        .endConnectContainer()
                    .endTemplate()
                .endSpec()
                .build();

        KafkaConnectCluster kcc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment deployment = kcc.generateDeployment(null, false, null, null);

        assertThat(deployment.getSpec().getTemplate().getSpec().getContainers(),
                hasItem(allOf(
                        hasProperty("name", equalTo(cluster + "-connect")),
                        hasProperty("securityContext", equalTo(securityContext))
                )));
    }

    @ParallelTest
    public void testTracing() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                .endSpec()
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kc.generateDeployment(Collections.EMPTY_MAP, true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertThat(cont.getEnv().stream().filter(env -> KafkaConnectCluster.ENV_VAR_STRIMZI_TRACING.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").equals("jaeger"), is(true));
        assertThat(cont.getEnv().stream().filter(env -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_CONFIGURATION.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("consumer.interceptor.classes=io.opentracing.contrib.kafka.TracingConsumerInterceptor"), is(true));
        assertThat(cont.getEnv().stream().filter(env -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_CONFIGURATION.equals(env.getName())).map(EnvVar::getValue).findFirst().orElse("").contains("producer.interceptor.classes=io.opentracing.contrib.kafka.TracingProducerInterceptor"), is(true));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithAccessToken() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withAuthentication(
                            new KafkaClientAuthenticationOAuthBuilder()
                                    .withNewAccessToken()
                                        .withSecretName("my-token-secret")
                                        .withKey("my-token-key")
                                    .endAccessToken()
                                    .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_ACCESS_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-token-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_ACCESS_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-token-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().isEmpty(), is(true));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithRefreshToken() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withNewRefreshToken()
                                    .withSecretName("my-token-secret")
                                    .withKey("my-token-key")
                                .endRefreshToken()
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_REFRESH_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-token-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_REFRESH_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-token-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server")));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithClientSecret() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
                .withAuthentication(
                        new KafkaClientAuthenticationOAuthBuilder()
                                .withClientId("my-client-id")
                                .withTokenEndpointUri("http://my-oauth-server")
                                .withAudience("kafka")
                                .withScope("all")
                                .withNewClientSecret()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                .endClientSecret()
                                .build())
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ClientConfig.OAUTH_SCOPE, "all", ClientConfig.OAUTH_AUDIENCE, "kafka")));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithMissingClientSecret() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                    .editSpec()
                    .withAuthentication(
                            new KafkaClientAuthenticationOAuthBuilder()
                                    .withClientId("my-client-id")
                                    .withTokenEndpointUri("http://my-oauth-server")
                                    .build())
                    .endSpec()
                    .build();

            KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        });
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithMissingUri() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                    .editSpec()
                    .withAuthentication(
                            new KafkaClientAuthenticationOAuthBuilder()
                                    .withClientId("my-client-id")
                                    .withNewClientSecret()
                                        .withSecretName("my-secret-secret")
                                        .withKey("my-secret-key")
                                    .endClientSecret()
                                    .build())
                    .endSpec()
                    .build();

            KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        });
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithTls() {
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

        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .editSpec()
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
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kc.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "")));

        // Volume mounts
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-certs-0".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaConnectCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + "/first-certificate-0"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-certs-1".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaConnectCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + "/second-certificate-1"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-certs-2".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaConnectCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + "/first-certificate-2"));

        // Volumes
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("ca.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("tls.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("ca2.crt"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("tls.crt"));
    }

    @ParallelTest
    public void testNetworkPolicyWithConnectorOperator() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resourceWithMetrics)
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        NetworkPolicy np = kc.generateNetworkPolicy(true, "operator-namespace", null);

        assertThat(np.getMetadata().getName(), is(kc.getName()));
        assertThat(np.getSpec().getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(np.getSpec().getIngress().get(0).getFrom().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getPodSelector().getMatchLabels(), is(singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator")));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getNamespaceSelector().getMatchLabels(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(1).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(1).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.METRICS_PORT));
    }

    @ParallelTest
    public void testNetworkPolicyWithConnectorOperatorSameNamespace() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resourceWithMetrics)
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        NetworkPolicy np = kc.generateNetworkPolicy(true, namespace, null);

        assertThat(np.getMetadata().getName(), is(kc.getName()));
        assertThat(np.getSpec().getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(np.getSpec().getIngress().get(0).getFrom().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getPodSelector().getMatchLabels(), is(singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator")));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(1).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(1).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.METRICS_PORT));
    }

    @ParallelTest
    public void testNetworkPolicyWithConnectorOperatorWithNamespaceLabels() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resourceWithMetrics)
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        NetworkPolicy np = kc.generateNetworkPolicy(true, "operator-namespace", Labels.fromMap(Collections.singletonMap("nsLabelKey", "nsLabelValue")));

        assertThat(np.getMetadata().getName(), is(kc.getName()));
        assertThat(np.getSpec().getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(0).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(np.getSpec().getIngress().get(0).getFrom().size(), is(2));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getPodSelector().getMatchLabels(), is(kc.getSelectorLabels().toMap()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(0).getNamespaceSelector(), is(nullValue()));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getPodSelector().getMatchLabels(), is(singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator")));
        assertThat(np.getSpec().getIngress().get(0).getFrom().get(1).getNamespaceSelector().getMatchLabels(), is(Collections.singletonMap("nsLabelKey", "nsLabelValue")));
        assertThat(np.getSpec().getIngress().get(1).getPorts().size(), is(1));
        assertThat(np.getSpec().getIngress().get(1).getPorts().get(0).getPort().getIntVal(), is(KafkaConnectCluster.METRICS_PORT));
    }

    @ParallelTest
    public void testNetworkPolicyWithoutConnectorOperator() {
        KafkaConnect resource = new KafkaConnectBuilder(this.resource)
                .build();
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        assertThat(kc.generateNetworkPolicy(false, null, null), is(nullValue()));
    }

    @ParallelTest
    public void testClusterRoleBindingRack() {
        String testNamespace = "other-namespace";

        KafkaConnect kafkaConnect = new KafkaConnectBuilder(this.resource)
                    .editOrNewMetadata()
                        .withNamespace(testNamespace)
                    .endMetadata()
                    .editOrNewSpec()
                        .withNewRack("my-topology-label")
                    .endSpec()
                .build();

        KafkaConnectCluster kafkaConnectCluster = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaConnect, VERSIONS);
        ClusterRoleBinding crb = kafkaConnectCluster.generateClusterRoleBinding();

        assertThat(crb.getMetadata().getName(), is(KafkaConnectResources.initContainerClusterRoleBindingName(cluster, testNamespace)));
        assertThat(crb.getMetadata().getNamespace(), is(nullValue()));
        assertThat(crb.getSubjects().get(0).getNamespace(), is(testNamespace));
        assertThat(crb.getSubjects().get(0).getName(), is(kafkaConnectCluster.getServiceAccountName()));
    }

    @ParallelTest
    public void testNullClusterRoleBinding() {
        String testNamespace = "other-namespace";

        KafkaConnect kafkaConnect = new KafkaConnectBuilder(this.resource)
                .editOrNewMetadata()
                    .withNamespace(testNamespace)
                .endMetadata()
                .build();

        KafkaConnectCluster kafkaConnectCluster = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaConnect, VERSIONS);
        ClusterRoleBinding crb = kafkaConnectCluster.generateClusterRoleBinding();

        assertThat(crb, is(nullValue()));
    }

    private void checkDeployment(Deployment dep, KafkaConnect resource) {
        assertThat(dep.getMetadata().getName(), is(KafkaConnectResources.deploymentName(cluster)));
        assertThat(dep.getMetadata().getNamespace(), is(namespace));
        Map<String, String> expectedDeploymentLabels = expectedLabels(KafkaConnectResources.deploymentName(cluster));
        assertThat(dep.getMetadata().getLabels(), is(expectedDeploymentLabels));
        assertThat(dep.getSpec().getSelector().getMatchLabels(), is(expectedSelectorLabels()));
        assertThat(dep.getSpec().getReplicas(), is(replicas));
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels(), is(expectedDeploymentLabels));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getName(), is(KafkaConnectResources.deploymentName(this.cluster)));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage(), is(kc.image));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv(), is(getExpectedEnvVars()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds(), is(healthDelay));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds(), is(healthTimeout));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds(), is(healthDelay));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds(), is(healthTimeout));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().size(), is(2));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getContainerPort(), is(KafkaConnectCluster.REST_API_PORT));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getName(), is(KafkaConnectCluster.REST_API_PORT_NAME));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(dep.getSpec().getStrategy().getType(), is("RollingUpdate"));
        assertThat(dep.getSpec().getStrategy().getRollingUpdate().getMaxSurge().getIntVal(), is(1));
        assertThat(dep.getSpec().getStrategy().getRollingUpdate().getMaxUnavailable().getIntVal(), is(0));
        assertThat(AbstractModel.containerEnvVars(dep.getSpec().getTemplate().getSpec().getContainers().get(0)).get(KafkaConnectCluster.ENV_VAR_KAFKA_CONNECT_TLS), is(nullValue()));
        checkOwnerReference(kc.createOwnerReference(), dep);
        checkRack(dep, resource);
    }

    private void checkOwnerReference(OwnerReference ownerRef, HasMetadata resource)  {
        assertThat(resource.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(resource.getMetadata().getOwnerReferences().get(0), is(ownerRef));
    }

    private void checkRack(Deployment deployment, KafkaConnect resource) {

        Rack rack = resource.getSpec().getRack();

        if (rack != null) {
            PodSpec podSpec = deployment.getSpec().getTemplate().getSpec();

            // check that pod spec contains the init Kafka container
            List<Container> initContainers = podSpec.getInitContainers();
            assertThat(initContainers, is(notNullValue()));
            assertThat(initContainers.size() > 0, is(true));

            boolean isInitKafkaConnect =
                    initContainers.stream().anyMatch(container -> container.getName().equals(KafkaConnectCluster.INIT_NAME));
            assertThat(isInitKafkaConnect, is(true));
        }
    }

    @ParallelTest
    public void testMetricsParsingFromConfigMap() {
        MetricsConfig metrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-metrics-configuration").withKey("config.yaml").build())
                .endValueFrom()
                .build();

        KafkaConnect kafkaConnect = new KafkaConnectBuilder(this.resource)
                .editSpec()
                    .withMetricsConfig(metrics)
                .endSpec()
                .build();

        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaConnect, VERSIONS);

        assertThat(kc.isMetricsEnabled(), is(true));
        assertThat(kc.getMetricsConfigInCm(), is(metrics));
    }

    @ParallelTest
    public void testMetricsParsingNoMetrics() {
        KafkaConnectCluster kc = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, this.resource, VERSIONS);

        assertThat(kc.isMetricsEnabled(), is(false));
        assertThat(kc.getMetricsConfigInCm(), is(nullValue()));
    }
}
