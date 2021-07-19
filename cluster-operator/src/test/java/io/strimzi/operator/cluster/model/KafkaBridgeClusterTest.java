/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.KafkaBridgeHttpConfig;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationTlsBuilder;
import io.strimzi.api.kafka.model.template.ContainerTemplate;
import io.strimzi.api.kafka.model.template.DeploymentStrategy;
import io.strimzi.api.kafka.model.template.IpFamily;
import io.strimzi.api.kafka.model.template.IpFamilyPolicy;
import io.strimzi.api.kafka.model.tracing.JaegerTracing;
import io.strimzi.kafka.oauth.client.ClientConfig;
import io.strimzi.kafka.oauth.server.ServerConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
public class KafkaBridgeClusterTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 1;
    private final String image = "my-image:latest";
    private final int healthDelay = 15;
    private final int healthTimeout = 5;
    private final String bootstrapServers = "foo-kafka:9092";
    private final String kafkaHeapOpts = "-Xms" + AbstractModel.DEFAULT_JVM_XMS;
    private final String defaultAdminclientConfiguration = "";
    private final String defaultProducerConfiguration = "";
    private final String defaultConsumerConfiguration = "";

    private final KafkaBridge resource = new KafkaBridgeBuilder(ResourceUtils.createEmptyKafkaBridge(namespace, cluster))
            .withNewSpec()
                .withEnableMetrics(true)
                .withImage(image)
                .withReplicas(replicas)
                .withBootstrapServers(bootstrapServers)
                .withNewHttp(8080)
            .endSpec()
            .build();
    private final KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

    private Map<String, String> expectedLabels(String name)    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                "my-user-label", "cromulent",
                Labels.STRIMZI_NAME_LABEL, name,
                Labels.STRIMZI_KIND_LABEL, KafkaBridge.RESOURCE_KIND,
                Labels.KUBERNETES_NAME_LABEL, KafkaBridgeCluster.APPLICATION_NAME,
                Labels.KUBERNETES_INSTANCE_LABEL, this.cluster,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + this.cluster,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private Map<String, String> expectedServiceLabels(String name)    {
        Map<String, String> serviceLabels = expectedLabels(name);
        serviceLabels.put(Labels.STRIMZI_DISCOVERY_LABEL, "true");

        return serviceLabels;
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels(KafkaBridgeResources.deploymentName(cluster))).strimziSelectorLabels().toMap();
    }

    protected List<EnvVar> getExpectedEnvVars() {

        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_METRICS_ENABLED).withValue(String.valueOf(true)).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_STRIMZI_GC_LOG_ENABLED).withValue(String.valueOf(AbstractModel.DEFAULT_JVM_GC_LOGGING_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_BOOTSTRAP_SERVERS).withValue(bootstrapServers).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_ADMIN_CLIENT_CONFIG).withValue(defaultAdminclientConfiguration).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_CONSUMER_CONFIG).withValue(defaultConsumerConfiguration).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_PRODUCER_CONFIG).withValue(defaultProducerConfiguration).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_ID).withValue(cluster).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_HTTP_ENABLED).withValue(String.valueOf(true)).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_HTTP_HOST).withValue(KafkaBridgeHttpConfig.HTTP_DEFAULT_HOST).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_HTTP_PORT).withValue(String.valueOf(KafkaBridgeHttpConfig.HTTP_DEFAULT_PORT)).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_CORS_ENABLED).withValue(String.valueOf(false)).build());
        expected.add(new EnvVarBuilder().withName(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_AMQP_ENABLED).withValue(String.valueOf(false)).build());
        return expected;
    }

    @ParallelTest
    public void testDefaultValues() {
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ResourceUtils.createEmptyKafkaBridge(namespace, cluster), VERSIONS);

        assertThat(kbc.image, is("quay.io/strimzi/kafka-bridge:latest"));
        assertThat(kbc.replicas, is(KafkaBridgeCluster.DEFAULT_REPLICAS));
        assertThat(kbc.readinessProbeOptions.getInitialDelaySeconds(), is(KafkaBridgeCluster.DEFAULT_HEALTHCHECK_DELAY));
        assertThat(kbc.readinessProbeOptions.getTimeoutSeconds(), is(KafkaBridgeCluster.DEFAULT_HEALTHCHECK_TIMEOUT));
        assertThat(kbc.livenessProbeOptions.getInitialDelaySeconds(), is(KafkaBridgeCluster.DEFAULT_HEALTHCHECK_DELAY));
        assertThat(kbc.livenessProbeOptions.getTimeoutSeconds(), is(KafkaBridgeCluster.DEFAULT_HEALTHCHECK_TIMEOUT));
    }

    @ParallelTest
    public void testFromCrd() {
        assertThat(kbc.replicas, is(replicas));
        assertThat(kbc.image, is(image));
        assertThat(kbc.readinessProbeOptions.getInitialDelaySeconds(), is(healthDelay));
        assertThat(kbc.readinessProbeOptions.getTimeoutSeconds(), is(healthTimeout));
        assertThat(kbc.livenessProbeOptions.getInitialDelaySeconds(), is(healthDelay));
        assertThat(kbc.livenessProbeOptions.getTimeoutSeconds(), is(healthTimeout));
    }

    @ParallelTest
    public void testEnvVars()   {
        assertThat(kbc.getEnvVars(), is(getExpectedEnvVars()));
    }

    @ParallelTest
    public void testGenerateService()   {
        Service svc = kbc.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedServiceLabels(kbc.getName())));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(Integer.valueOf(KafkaBridgeCluster.DEFAULT_REST_API_PORT)));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(KafkaBridgeCluster.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(svc.getSpec().getIpFamilies(), is(emptyList()));

        assertThat(svc.getMetadata().getAnnotations(), is(kbc.getDiscoveryAnnotation(KafkaBridgeCluster.DEFAULT_REST_API_PORT)));

        checkOwnerReference(kbc.createOwnerReference(), svc);
    }

    @ParallelTest
    public void testGenerateDeployment()   {
        Deployment dep = kbc.generateDeployment(new HashMap<String, String>(), true, null, null);

        assertThat(dep.getMetadata().getName(), is(KafkaBridgeResources.deploymentName(cluster)));
        assertThat(dep.getMetadata().getNamespace(), is(namespace));
        Map<String, String> expectedDeploymentLabels = expectedLabels(KafkaBridgeResources.deploymentName(cluster));
        assertThat(dep.getMetadata().getLabels(), is(expectedDeploymentLabels));
        assertThat(dep.getSpec().getSelector().getMatchLabels(), is(expectedSelectorLabels()));
        assertThat(dep.getSpec().getReplicas(), is(Integer.valueOf(replicas)));
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels(), is(expectedDeploymentLabels));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getName(), is(KafkaBridgeResources.deploymentName(cluster)));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage(), is(kbc.image));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv(), is(getExpectedEnvVars()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getInitialDelaySeconds(), is(Integer.valueOf(healthDelay)));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe().getTimeoutSeconds(), is(Integer.valueOf(healthTimeout)));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds(), is(Integer.valueOf(healthDelay)));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds(), is(Integer.valueOf(healthTimeout)));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getContainerPort(), is(Integer.valueOf(KafkaBridgeCluster.DEFAULT_REST_API_PORT)));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getName(), is(KafkaBridgeCluster.REST_API_PORT_NAME));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(dep.getSpec().getStrategy().getType(), is("RollingUpdate"));
        assertThat(dep.getSpec().getStrategy().getRollingUpdate().getMaxSurge().getIntVal(), is(Integer.valueOf(1)));
        assertThat(dep.getSpec().getStrategy().getRollingUpdate().getMaxUnavailable().getIntVal(), is(Integer.valueOf(0)));
        assertThat(AbstractModel.containerEnvVars(dep.getSpec().getTemplate().getSpec().getContainers().get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_TLS), is(nullValue()));
        checkOwnerReference(kbc.createOwnerReference(), dep);
    }

    @ParallelTest
    public void testGenerateDeploymentWithTls() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .editOrNewTls()
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("cert.crt").build())
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-secret").withCertificate("new-cert.crt").build())
                        .addToTrustedCertificates(new CertSecretSourceBuilder().withSecretName("my-another-secret").withCertificate("another-cert.crt").build())
                    .endTls()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("my-secret"));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("my-another-secret"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaBridgeCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-secret"));
        assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaBridgeCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "my-another-secret"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_TRUSTED_CERTS), is("my-secret/cert.crt;my-secret/new-cert.crt;my-another-secret/another-cert.crt"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_TLS), is("true"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithTlsAuth() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(3).getName(), is("user-secret"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(KafkaBridgeCluster.TLS_CERTS_BASE_VOLUME_MOUNT + "user-secret"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_TLS_AUTH_CERT), is("user-secret/user.crt"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_TLS_AUTH_KEY), is("user-secret/user.key"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_TLS), is("true"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithTlsSameSecret() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);

        // 2 = 1 volume from logging/metrics + just 1 from above certs Secret
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().size(), is(3));
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("my-secret"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithScramSha512Auth() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("user1-secret"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaBridgeCluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_PASSWORD_FILE), is("user1-secret/password"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_USERNAME), is("user1"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM), is("scram-sha-512"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithPlainAuth() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(2).getName(), is("user1-secret"));

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.get(0).getVolumeMounts().get(2).getMountPath(), is(KafkaBridgeCluster.PASSWORD_VOLUME_MOUNT + "user1-secret"));

        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_PASSWORD_FILE), is("user1-secret/password"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_USERNAME), is("user1"));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM), is("plain"));
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

        Map<String, String> saLabels = TestUtils.map("l9", "v9", "l10", "v10");
        Map<String, String> saAnots = TestUtils.map("a9", "v9", "a10", "v10");

        Affinity affinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("key1")
                                    .withOperator("In")
                                    .withValues("value1", "value2")
                                .endMatchExpression()
                                .build())
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .build();

        List<Toleration> tolerations = singletonList(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key1")
                .withOperator("Equal")
                .withValue("value1")
                .build());

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

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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
                            .withAffinity(affinity)
                            .withTolerations(tolerations)
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
                        .withNewServiceAccount()
                            .withNewMetadata()
                                .withLabels(saLabels)
                                .withAnnotations(saAnots)
                            .endMetadata()
                        .endServiceAccount()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getMetadata().getLabels().entrySet().containsAll(expectedDepLabels.entrySet()), is(true));
        assertThat(dep.getMetadata().getAnnotations().entrySet().containsAll(depAnots.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));
        assertThat(dep.getSpec().getStrategy().getType(), is("Recreate"));
        assertThat(dep.getSpec().getStrategy().getRollingUpdate(), is(nullValue()));

        // Check Pods
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(dep.getSpec().getTemplate().getSpec().getAffinity(), is(affinity));
        assertThat(dep.getSpec().getTemplate().getSpec().getTolerations(), is(tolerations));
        assertThat(dep.getSpec().getTemplate().getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1, tsc2));
        assertThat(dep.getSpec().getTemplate().getSpec().getEnableServiceLinks(), is(false));

        // Check Service
        Service svc = kbc.generateService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnots.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = kbc.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnots.entrySet()), is(true));

        // Check Service Account
        ServiceAccount sa = kbc.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnots.entrySet()), is(true));
    }

    public void checkOwnerReference(OwnerReference ownerRef, HasMetadata resource)  {
        assertThat(resource.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(resource.getMetadata().getOwnerReferences().get(0), is(ownerRef));
    }

    @ParallelTest
    public void testGracePeriod() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withTerminationGracePeriodSeconds(123)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(Long.valueOf(123)));
    }

    @ParallelTest
    public void testDefaultGracePeriod() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource).build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(Long.valueOf(30)));
    }

    @ParallelTest
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret1, secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
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

        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, this.resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, secrets);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testImagePullSecretsBoth() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret2)
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, singletonList(secret1));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testDefaultImagePullSecrets() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource).build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets(), is(nullValue()));
    }

    @ParallelTest
    public void testSecurityContext() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPod()
                            .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                        .endPod()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(notNullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(Long.valueOf(123)));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup(), is(Long.valueOf(456)));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser(), is(Long.valueOf(789)));
    }

    @ParallelTest
    public void testDefaultSecurityContext() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource).build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(emptyMap(), true, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
    }

    @ParallelTest
    public void testPodDisruptionBudget() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewTemplate()
                        .withNewPodDisruptionBudget()
                            .withMaxUnavailable(2)
                        .endPodDisruptionBudget()
                    .endTemplate()
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        PodDisruptionBudget pdb = kbc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(2)));
    }

    @ParallelTest
    public void testDefaultPodDisruptionBudget() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource).build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        PodDisruptionBudget pdb = kbc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(1)));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(Collections.EMPTY_MAP, true, ImagePullPolicy.ALWAYS, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        dep = kbc.generateDeployment(Collections.EMPTY_MAP, true, ImagePullPolicy.IFNOTPRESENT, null);
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

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withResources(new ResourceRequirementsBuilder().withLimits(limits).withRequests(requests).build())
                .endSpec()
                .build();
        KafkaBridgeCluster kbc = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = kbc.generateDeployment(Collections.EMPTY_MAP, true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertThat(cont.getResources().getLimits(), is(limits));
        assertThat(cont.getResources().getRequests(), is(requests));
    }

    @ParallelTest
    public void testKafkaBridgeContainerEnvVars() {

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
        ContainerTemplate kafkaBridgeContainer = new ContainerTemplate();
        kafkaBridgeContainer.setEnv(testEnvs);

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                .withNewTemplate()
                .withBridgeContainer(kafkaBridgeContainer)
                .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS).getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));
    }

    @ParallelTest
    public void testKafkaBridgeContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_BOOTSTRAP_SERVERS;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_CONSUMER_CONFIG;
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate kafkaBridgeContainer = new ContainerTemplate();
        kafkaBridgeContainer.setEnv(testEnvs);

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                .withNewTemplate()
                .withBridgeContainer(kafkaBridgeContainer)
                .endTemplate()
                .endSpec()
                .build();

        List<EnvVar> kafkaEnvVars = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS).getEnvVars();

        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                kafkaEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));
        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvTwoKey,
                kafkaEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(false));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithAccessToken() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_ACCESS_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-token-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_ACCESS_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-token-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().isEmpty(), is(true));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithRefreshToken() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_REFRESH_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-token-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_REFRESH_TOKEN.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-token-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server")));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithClientSecret() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ClientConfig.OAUTH_SCOPE, "all", ClientConfig.OAUTH_AUDIENCE, "kafka")));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithMissingClientSecret() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                    .editSpec()
                        .withAuthentication(
                                new KafkaClientAuthenticationOAuthBuilder()
                                        .withClientId("my-client-id")
                                        .withTokenEndpointUri("http://my-oauth-server")
                                        .build())
                    .endSpec()
                    .build();

            KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        });
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthWithMissingUri() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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

            KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
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

        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
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

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "")));

        // Volume mounts
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-certs-0".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaBridgeCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + "/first-certificate-0"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-certs-1".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaBridgeCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + "/second-certificate-1"));
        assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-certs-2".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaBridgeCluster.OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + "/first-certificate-2"));


        // Volumes
        List<KeyToPath> cert1Items = dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems();
        assertThat(cert1Items.size(), is(1));
        assertThat(cert1Items.get(0).getKey(), is("ca.crt"));
        assertThat(cert1Items.get(0).getPath(), is("tls.crt"));

        List<KeyToPath> cert2Items = dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems();
        assertThat(cert2Items.size(), is(1));
        assertThat(cert2Items.get(0).getKey(), is("tls.crt"));
        assertThat(cert2Items.get(0).getPath(), is("tls.crt"));

        List<KeyToPath> cert3Items = dep.getSpec().getTemplate().getSpec().getVolumes().stream().filter(vol -> "oauth-certs-2".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems();
        assertThat(cert3Items.size(), is(1));
        assertThat(cert3Items.get(0).getKey(), is("ca2.crt"));
        assertThat(cert3Items.get(0).getPath(), is("tls.crt"));
    }

    @ParallelTest
    public void testGenerateDeploymentWithOAuthUsingOpaqueTokens() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
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
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM.equals(var.getName())).findFirst().orElseThrow().getValue(), is("oauth"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CLIENT_SECRET.equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
        assertThat(cont.getEnv().stream().filter(var -> KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_OAUTH_CONFIG.equals(var.getName())).findFirst().orElseThrow().getValue().trim(),
                is(String.format("%s=\"%s\" %s=\"%s\" %s=\"%s\" %s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, "my-client-id", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, "http://my-oauth-server", ClientConfig.OAUTH_ACCESS_TOKEN_IS_JWT, "false", ClientConfig.OAUTH_MAX_TOKEN_EXPIRY_SECONDS, "600")));
    }

    @ParallelTest
    public void testDifferentHttpPort()   {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewHttp(1874)
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check ports in container
        Deployment dep = kb.generateDeployment(emptyMap(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getLivenessProbe().getHttpGet().getPort(), is(new IntOrString(KafkaBridgeCluster.REST_API_PORT_NAME)));
        assertThat(cont.getReadinessProbe().getHttpGet().getPort(), is(new IntOrString(KafkaBridgeCluster.REST_API_PORT_NAME)));
        assertThat(cont.getPorts().get(0).getContainerPort(), is(Integer.valueOf(1874)));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getName(), is(KafkaBridgeCluster.REST_API_PORT_NAME));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getPorts().get(0).getProtocol(), is("TCP"));

        // Check ports on Service
        Service svc = kb.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedServiceLabels(kb.getName())));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(Integer.valueOf(1874)));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(KafkaBridgeCluster.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(svc.getMetadata().getAnnotations(), is(kbc.getDiscoveryAnnotation(1874)));
        checkOwnerReference(kbc.createOwnerReference(), svc);
    }

    @ParallelTest
    public void testProbeConfiguration()   {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withNewLivenessProbe()
                        .withInitialDelaySeconds(20)
                        .withPeriodSeconds(21)
                        .withTimeoutSeconds(22)
                    .endLivenessProbe()
                    .withNewReadinessProbe()
                        .withInitialDelaySeconds(30)
                        .withPeriodSeconds(31)
                        .withTimeoutSeconds(32)
                    .endReadinessProbe()
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = kb.generateDeployment(new HashMap<String, String>(), true, null, null);
        Container cont = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(cont.getLivenessProbe().getInitialDelaySeconds(), is(Integer.valueOf(20)));
        assertThat(cont.getLivenessProbe().getPeriodSeconds(), is(Integer.valueOf(21)));
        assertThat(cont.getLivenessProbe().getTimeoutSeconds(), is(Integer.valueOf(22)));
        assertThat(cont.getReadinessProbe().getInitialDelaySeconds(), is(Integer.valueOf(30)));
        assertThat(cont.getReadinessProbe().getPeriodSeconds(), is(Integer.valueOf(31)));
        assertThat(cont.getReadinessProbe().getTimeoutSeconds(), is(Integer.valueOf(32)));
    }

    @ParallelTest
    public void testTracingConfiguration() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .withJaegerTracing(new JaegerTracing())
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment deployment = kb.generateDeployment(new HashMap<>(), true, null, null);
        Container container = deployment.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(AbstractModel.containerEnvVars(container).get(KafkaBridgeCluster.ENV_VAR_STRIMZI_TRACING), is("jaeger"));
    }

    @ParallelTest
    public void testCorsConfiguration() {
        KafkaBridge resource = new KafkaBridgeBuilder(this.resource)
                .editSpec()
                    .editHttp()
                        .withNewCors()
                            .withAllowedOrigins("https://strimzi.io", "https://cncf.io")
                            .withAllowedMethods("GET", "POST", "PUT", "DELETE", "PATCH")
                        .endCors()
                    .endHttp()
                .endSpec()
                .build();

        KafkaBridgeCluster kb = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment deployment = kb.generateDeployment(new HashMap<>(), true, null, null);
        Container container = deployment.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(AbstractModel.containerEnvVars(container).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_CORS_ENABLED), is("true"));
        assertThat(AbstractModel.containerEnvVars(container).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_CORS_ALLOWED_ORIGINS), is("https://strimzi.io,https://cncf.io"));
        assertThat(AbstractModel.containerEnvVars(container).get(KafkaBridgeCluster.ENV_VAR_KAFKA_BRIDGE_CORS_ALLOWED_METHODS), is("GET,POST,PUT,DELETE,PATCH"));
    }
}
