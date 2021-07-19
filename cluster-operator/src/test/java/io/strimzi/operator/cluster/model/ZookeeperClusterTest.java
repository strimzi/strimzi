/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeerBuilder;
import io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.MetricsConfig;
import io.strimzi.api.kafka.model.storage.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.RackBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageOverrideBuilder;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.template.ContainerTemplate;
import io.strimzi.api.kafka.model.template.IpFamily;
import io.strimzi.api.kafka.model.template.IpFamilyPolicy;
import io.strimzi.api.kafka.model.template.PodManagementPolicy;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.io.IOException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static io.strimzi.test.TestUtils.set;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
@ParallelSuite
public class ZookeeperClusterTest {

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "image";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final int tlsHealthDelay = 120;
    private final int tlsHealthTimeout = 30;
    private final Map<String, Object> metricsCm = singletonMap("animal", "wombat");
    private final String metricsCmJson = "{\"animal\":\"wombat\"}";
    private final String metricsCMName = "metrics-cm";
    private final ConfigMap metricsCM = io.strimzi.operator.cluster.TestUtils.getJmxMetricsCm(metricsCmJson, metricsCMName, "metrics-config.yml");
    private final JmxPrometheusExporterMetrics jmxMetricsConfig = io.strimzi.operator.cluster.TestUtils.getJmxPrometheusExporterMetrics("metrics-config.yml", metricsCMName);
    private final Map<String, Object> configurationJson = emptyMap();
    private final InlineLogging kafkaLogConfigJson = new InlineLogging();
    private final InlineLogging zooLogConfigJson = new InlineLogging();
    {
        kafkaLogConfigJson.setLoggers(Collections.singletonMap("kafka.root.logger.level", "OFF"));
        zooLogConfigJson.setLoggers(Collections.singletonMap("zookeeper.root.logger", "OFF"));
    }

    private final Map<String, Object> zooConfigurationJson = singletonMap("foo", "bar");

    private final Kafka ka = ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, zooConfigurationJson, null, null, kafkaLogConfigJson, zooLogConfigJson, null, null);

    private final ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);

    @ParallelTest
    public void testMetricsConfigMap() {
        ConfigMap metricsCm = zc.generateConfigurationConfigMap(new MetricsAndLogging(metricsCM, null));
        checkMetricsConfigMap(metricsCm);
        checkOwnerReference(zc.createOwnerReference(), metricsCm);
    }

    private void checkMetricsConfigMap(ConfigMap metricsCm) {
        assertThat(metricsCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_METRICS), is(TestUtils.toJsonString(this.metricsCm)));
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziSelectorLabels().toMap();
    }

    private Map<String, String> expectedLabels()    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, cluster,
            "my-user-label", "cromulent",
            Labels.STRIMZI_NAME_LABEL, ZookeeperCluster.zookeeperClusterName(cluster),
            Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND,
            Labels.KUBERNETES_NAME_LABEL, ZookeeperCluster.APPLICATION_NAME,
            Labels.KUBERNETES_INSTANCE_LABEL, this.cluster,
            Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + this.cluster,
            Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    @ParallelTest
    public void testGenerateService() {
        Service headful = zc.generateService();

        assertThat(headful.getSpec().getType(), is("ClusterIP"));
        assertThat(headful.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(headful.getSpec().getPorts().size(), is(1));
        assertThat(headful.getSpec().getPorts().get(0).getName(), is(ZookeeperCluster.CLIENT_TLS_PORT_NAME));
        assertThat(headful.getSpec().getPorts().get(0).getPort(), is(Integer.valueOf(ZookeeperCluster.CLIENT_TLS_PORT)));
        assertThat(headful.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(headful.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(headful.getSpec().getIpFamilies(), is(emptyList()));
        assertThat(headful.getMetadata().getAnnotations(), is(nullValue()));

        checkOwnerReference(zc.createOwnerReference(), headful);
    }

    @ParallelTest
    public void testGenerateServiceWithoutMetrics() {
        Kafka kafka = new KafkaBuilder(ka)
                .editSpec()
                    .editZookeeper()
                        .withMetricsConfig(null)
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);
        Service headful = zc.generateService();

        assertThat(headful.getSpec().getType(), is("ClusterIP"));
        assertThat(headful.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(headful.getSpec().getPorts().size(), is(1));
        assertThat(headful.getSpec().getPorts().get(0).getName(), is(ZookeeperCluster.CLIENT_TLS_PORT_NAME));
        assertThat(headful.getSpec().getPorts().get(0).getPort(), is(Integer.valueOf(ZookeeperCluster.CLIENT_TLS_PORT)));
        assertThat(headful.getSpec().getPorts().get(0).getProtocol(), is("TCP"));

        assertThat(headful.getMetadata().getAnnotations(), is(nullValue()));

        checkOwnerReference(zc.createOwnerReference(), headful);
    }

    @ParallelTest
    public void testGenerateHeadlessService() {
        Service headless = zc.generateHeadlessService();
        checkHeadlessService(headless);
        checkOwnerReference(zc.createOwnerReference(), headless);
    }

    private void checkHeadlessService(Service headless) {
        assertThat(headless.getMetadata().getName(), is(ZookeeperCluster.headlessServiceName(cluster)));
        assertThat(headless.getSpec().getType(), is("ClusterIP"));
        assertThat(headless.getSpec().getClusterIP(), is("None"));
        assertThat(headless.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(headless.getSpec().getPorts().size(), is(3));
        assertThat(headless.getSpec().getPorts().get(0).getName(), is(ZookeeperCluster.CLIENT_TLS_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(0).getPort(), is(Integer.valueOf(ZookeeperCluster.CLIENT_TLS_PORT)));
        assertThat(headless.getSpec().getPorts().get(1).getName(), is(ZookeeperCluster.CLUSTERING_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(1).getPort(), is(Integer.valueOf(ZookeeperCluster.CLUSTERING_PORT)));
        assertThat(headless.getSpec().getPorts().get(2).getName(), is(ZookeeperCluster.LEADER_ELECTION_PORT_NAME));
        assertThat(headless.getSpec().getPorts().get(2).getPort(), is(Integer.valueOf(ZookeeperCluster.LEADER_ELECTION_PORT)));
        assertThat(headless.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(headless.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(headless.getSpec().getIpFamilies(), is(emptyList()));
    }

    @ParallelTest
    public void testGenerateStatefulSet() {
        // We expect a single statefulSet ...
        StatefulSet sts = zc.generateStatefulSet(true, null, null);
        checkStatefulSet(sts);
        checkOwnerReference(zc.createOwnerReference(), sts);
    }

    @ParallelTest
    public void testGenerateStatefulSetWithPodManagementPolicy() {
        Kafka editZooAssembly = new KafkaBuilder(ka)
                        .editSpec()
                            .editZookeeper()
                                .withNewTemplate()
                                    .withNewStatefulset()
                                        .withPodManagementPolicy(PodManagementPolicy.ORDERED_READY)
                                    .endStatefulset()
                                .endTemplate()
                            .endZookeeper()
                        .endSpec().build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, editZooAssembly, VERSIONS);
        StatefulSet sts = zc.generateStatefulSet(false, null, null);
        assertThat(sts.getSpec().getPodManagementPolicy(), is(PodManagementPolicy.ORDERED_READY.toValue()));
    }

    @ParallelTest
    public void testInvalidVersion() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka ka = new KafkaBuilder(this.ka)
                    .editSpec()
                        .editKafka()
                            .withImage(null)
                            .withVersion("10000.0.0")
                        .endKafka()
                        .editZookeeper()
                            .withImage(null)
                        .endZookeeper()
                    .endSpec()
                    .build();

            ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);
        });
    }

    private void checkStatefulSet(StatefulSet sts) {
        assertThat(sts.getMetadata().getName(), is(ZookeeperCluster.zookeeperClusterName(cluster)));
        // ... in the same namespace ...
        assertThat(sts.getMetadata().getNamespace(), is(namespace));
        // ... with these labels
        assertThat(sts.getMetadata().getLabels(), is(expectedLabels()));
        assertThat(sts.getSpec().getSelector().getMatchLabels(), is(expectedSelectorLabels()));

        assertThat(sts.getSpec().getTemplate().getSpec().getSchedulerName(), is("default-scheduler"));

        List<Container> containers = sts.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.size(), is(1));

        // checks on the main Zookeeper container
        assertThat(sts.getSpec().getReplicas(), is(Integer.valueOf(replicas)));
        assertThat(sts.getSpec().getPodManagementPolicy(), is(PodManagementPolicy.PARALLEL.toValue()));
        assertThat(containers.get(0).getImage(), is(image + "-zk"));
        assertThat(containers.get(0).getLivenessProbe().getTimeoutSeconds(), is(Integer.valueOf(healthTimeout)));
        assertThat(containers.get(0).getLivenessProbe().getInitialDelaySeconds(), is(Integer.valueOf(healthDelay)));
        assertThat(containers.get(0).getLivenessProbe().getFailureThreshold(), is(Integer.valueOf(10)));
        assertThat(containers.get(0).getLivenessProbe().getSuccessThreshold(), is(Integer.valueOf(4)));
        assertThat(containers.get(0).getLivenessProbe().getPeriodSeconds(), is(Integer.valueOf(33)));
        assertThat(containers.get(0).getReadinessProbe().getTimeoutSeconds(), is(Integer.valueOf(healthTimeout)));
        assertThat(containers.get(0).getReadinessProbe().getInitialDelaySeconds(), is(Integer.valueOf(healthDelay)));
        assertThat(containers.get(0).getReadinessProbe().getFailureThreshold(), is(Integer.valueOf(10)));
        assertThat(containers.get(0).getReadinessProbe().getSuccessThreshold(), is(Integer.valueOf(4)));
        assertThat(containers.get(0).getReadinessProbe().getPeriodSeconds(), is(Integer.valueOf(33)));
        OrderedProperties expectedConfig = new OrderedProperties().addMapPairs(ZookeeperConfiguration.DEFAULTS).addPair("foo", "bar");
        OrderedProperties actual = new OrderedProperties()
                .addStringPairs(AbstractModel.containerEnvVars(containers.get(0)).get(ZookeeperCluster.ENV_VAR_ZOOKEEPER_CONFIGURATION));
        assertThat(actual, is(expectedConfig));
        assertThat(AbstractModel.containerEnvVars(containers.get(0)).get(ZookeeperCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED), is(Boolean.toString(AbstractModel.DEFAULT_JVM_GC_LOGGING_ENABLED)));
        assertThat(containers.get(0).getVolumeMounts().get(0).getName(), is(AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME));
        assertThat(containers.get(0).getVolumeMounts().get(0).getMountPath(), is(AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
        assertThat(containers.get(0).getVolumeMounts().get(3).getName(), is(ZookeeperCluster.ZOOKEEPER_NODE_CERTIFICATES_VOLUME_NAME));
        assertThat(containers.get(0).getVolumeMounts().get(3).getMountPath(), is(ZookeeperCluster.ZOOKEEPER_NODE_CERTIFICATES_VOLUME_MOUNT));
        assertThat(containers.get(0).getVolumeMounts().get(4).getName(), is(ZookeeperCluster.ZOOKEEPER_CLUSTER_CA_VOLUME_NAME));
        assertThat(containers.get(0).getVolumeMounts().get(4).getMountPath(), is(ZookeeperCluster.ZOOKEEPER_CLUSTER_CA_VOLUME_MOUNT));
    }

    // TODO test volume claim templates

    @ParallelTest
    public void testPodNames() {

        for (int i = 0; i < replicas; i++) {
            assertThat(zc.getPodName(i), is(ZookeeperCluster.zookeeperPodName(cluster, i)));
        }
    }

    @ParallelTest
    public void testPvcNames() {
        Kafka ka = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, zooConfigurationJson))
                .editSpec()
                    .editZookeeper()
                        .withNewPersistentClaimStorage().withDeleteClaim(false).withSize("100Gi").endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);

        PersistentVolumeClaim pvc = zc.getVolumeClaims().get(0);

        for (int i = 0; i < replicas; i++) {
            assertThat(pvc.getMetadata().getName() + "-" + ZookeeperCluster.zookeeperPodName(cluster, i),
                    is(zc.VOLUME_NAME + "-" + ZookeeperCluster.zookeeperPodName(cluster, i)));
        }
    }

    @ParallelTest
    public void withAffinity() throws IOException {
        ResourceTester<Kafka, ZookeeperCluster> resourceTester = new ResourceTester<>(Kafka.class, VERSIONS, (kafkaAssembly, versions) -> ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, versions), this.getClass().getSimpleName() + ".withAffinity");
        resourceTester.assertDesiredResource("-STS.yaml", zc -> zc.generateStatefulSet(true, null, null).getSpec().getTemplate().getSpec().getAffinity());
    }

    @ParallelTest
    public void withTolerations() throws IOException {
        ResourceTester<Kafka, ZookeeperCluster> resourceTester = new ResourceTester<>(Kafka.class, VERSIONS, (kafkaAssembly, versions) -> ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, versions), this.getClass().getSimpleName() + ".withTolerations");
        resourceTester.assertDesiredResource("-STS.yaml", zc -> zc.generateStatefulSet(true, null, null).getSpec().getTemplate().getSpec().getTolerations());
    }

    public void checkOwnerReference(OwnerReference ownerRef, HasMetadata resource)  {
        assertThat(resource.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(resource.getMetadata().getOwnerReferences().get(0), is(ownerRef));
    }

    private Secret generateNodeSecret() {
        ClusterCa clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(), new PasswordGenerator(10, "a", "a"), cluster, null, null);
        clusterCa.createRenewOrReplace(namespace, cluster, emptyMap(), emptyMap(), emptyMap(), null, true);

        zc.generateCertificates(ka, clusterCa, true);
        return zc.generateNodesSecret();
    }

    @ParallelTest
    public void testGenerateBrokerSecret() throws CertificateParsingException {
        Secret secret = generateNodeSecret();
        assertThat(secret.getData().keySet(), is(set(
                "foo-zookeeper-0.crt",  "foo-zookeeper-0.key", "foo-zookeeper-0.p12", "foo-zookeeper-0.password",
                "foo-zookeeper-1.crt", "foo-zookeeper-1.key", "foo-zookeeper-1.p12", "foo-zookeeper-1.password",
                "foo-zookeeper-2.crt", "foo-zookeeper-2.key", "foo-zookeeper-2.p12", "foo-zookeeper-2.password")));
        X509Certificate cert = Ca.cert(secret, "foo-zookeeper-0.crt");
        assertThat(cert.getSubjectDN().getName(), is("CN=foo-zookeeper, O=io.strimzi"));
        assertThat(new HashSet<Object>(cert.getSubjectAlternativeNames()), is(set(
                asList(2, "foo-zookeeper-0.foo-zookeeper-nodes.test.svc"),
                asList(2, "foo-zookeeper-0.foo-zookeeper-nodes.test.svc.cluster.local"),
                asList(2, "foo-zookeeper-client"),
                asList(2, "foo-zookeeper-client.test"),
                asList(2, "foo-zookeeper-client.test.svc"),
                asList(2, "foo-zookeeper-client.test.svc.cluster.local"),
                asList(2, "*.foo-zookeeper-client.test.svc"),
                asList(2, "*.foo-zookeeper-client.test.svc.cluster.local"),
                asList(2, "*.foo-zookeeper-nodes.test.svc"),
                asList(2, "*.foo-zookeeper-nodes.test.svc.cluster.local"))));

    }

    @ParallelTest
    public void testTemplate() {
        Map<String, String> ssLabels = TestUtils.map("l1", "v1", "l2", "v2",
                Labels.KUBERNETES_PART_OF_LABEL, "custom-part",
                Labels.KUBERNETES_MANAGED_BY_LABEL, "custom-managed-by");
        Map<String, String> expectedStsLabels = new HashMap<>(ssLabels);
        expectedStsLabels.remove(Labels.KUBERNETES_MANAGED_BY_LABEL);
        Map<String, String> ssAnots = TestUtils.map("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = TestUtils.map("l3", "v3", "l4", "v4");
        Map<String, String> podAnots = TestUtils.map("a3", "v3", "a4", "v4");

        Map<String, String> svcLabels = TestUtils.map("l5", "v5", "l6", "v6");
        Map<String, String> svcAnots = TestUtils.map("a5", "v5", "a6", "v6");

        Map<String, String> hSvcLabels = TestUtils.map("l7", "v7", "l8", "v8");
        Map<String, String> hSvcAnots = TestUtils.map("a7", "v7", "a8", "v8");

        Map<String, String> pdbLabels = TestUtils.map("l9", "v9", "l10", "v10");
        Map<String, String> pdbAnots = TestUtils.map("a9", "v9", "a10", "v10");

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

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, emptyMap()))
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewStatefulset()
                                .withNewMetadata()
                                    .withLabels(ssLabels)
                                    .withAnnotations(ssAnots)
                                .endMetadata()
                            .endStatefulset()
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
                            .withNewClientService()
                                .withNewMetadata()
                                    .withLabels(svcLabels)
                                    .withAnnotations(svcAnots)
                                .endMetadata()
                                .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
                                .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                            .endClientService()
                            .withNewNodesService()
                                .withNewMetadata()
                                    .withLabels(hSvcLabels)
                                    .withAnnotations(hSvcAnots)
                                .endMetadata()
                                .withIpFamilyPolicy(IpFamilyPolicy.SINGLE_STACK)
                                .withIpFamilies(IpFamily.IPV6)
                            .endNodesService()
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
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        // Check StatefulSet
        StatefulSet sts = zc.generateStatefulSet(true, null, null);
        assertThat(sts.getMetadata().getLabels().entrySet().containsAll(expectedStsLabels.entrySet()), is(true));
        assertThat(sts.getMetadata().getAnnotations().entrySet().containsAll(ssAnots.entrySet()), is(true));
        assertThat(sts.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));

        // Check Pods
        assertThat(sts.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
        assertThat(sts.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()), is(true));
        assertThat(sts.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(sts.getSpec().getTemplate().getSpec().getHostAliases(), containsInAnyOrder(hostAlias1, hostAlias2));
        assertThat(sts.getSpec().getTemplate().getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1, tsc2));
        assertThat(sts.getSpec().getTemplate().getSpec().getEnableServiceLinks(), is(false));

        // Check Service
        Service svc = zc.generateService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnots.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));

        // Check Headless Service
        svc = zc.generateHeadlessService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(hSvcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(hSvcAnots.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("SingleStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6"));

        // Check PodDisruptionBudget
        PodDisruptionBudget pdb = zc.generatePodDisruptionBudget();
        assertThat(pdb.getMetadata().getLabels().entrySet().containsAll(pdbLabels.entrySet()), is(true));
        assertThat(pdb.getMetadata().getAnnotations().entrySet().containsAll(pdbAnots.entrySet()), is(true));

        // Check Service Account
        ServiceAccount sa = zc.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnots.entrySet()), is(true));
    }

    @ParallelTest
    public void testGracePeriod() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, emptyMap()))
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewPod()
                                .withTerminationGracePeriodSeconds(123)
                            .endPod()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(true, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(Long.valueOf(123)));
    }

    @ParallelTest
    public void testDefaultGracePeriod() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, emptyMap()))
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(true, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(Long.valueOf(30)));
    }

    @ParallelTest
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, emptyMap()))
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewPod()
                                .withImagePullSecrets(secret1, secret2)
                            .endPod()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(true, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testImagePullSecretsFromCO() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        List<LocalObjectReference> secrets = new ArrayList<>(2);
        secrets.add(secret1);
        secrets.add(secret2);

        Kafka kafkaAssembly = ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, emptyMap());
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(true, null, secrets);
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testImagePullSecretsFromBoth() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, emptyMap()))
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewPod()
                                .withImagePullSecrets(secret2)
                            .endPod()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(true, null, singletonList(secret1));
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(1));
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(false));
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testDefaultImagePullSecrets() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, emptyMap()))
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(true, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getImagePullSecrets(), is(nullValue()));
    }

    @ParallelTest
    public void testSecurityContext() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, emptyMap()))
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewPod()
                                .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                            .endPod()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(true, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext(), is(notNullValue()));
        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(Long.valueOf(123)));
        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup(), is(Long.valueOf(456)));
        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser(), is(Long.valueOf(789)));
    }

    @ParallelTest
    public void testDefaultSecurityContext() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, emptyMap()))
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(true, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
    }

    @ParallelTest
    public void testPodDisruptionBudget() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, emptyMap()))
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewPodDisruptionBudget()
                                .withMaxUnavailable(2)
                            .endPodDisruptionBudget()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        PodDisruptionBudget pdb = zc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(2)));
    }

    @ParallelTest
    public void testDefaultPodDisruptionBudget() {
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, emptyMap()))
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        PodDisruptionBudget pdb = zc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(1)));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        Kafka kafkaAssembly = ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, emptyMap());
        kafkaAssembly.getSpec().getKafka().setRack(new RackBuilder().withTopologyKey("topology-key").build());
        ZookeeperCluster kc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(true, ImagePullPolicy.ALWAYS, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        sts = zc.generateStatefulSet(true, ImagePullPolicy.IFNOTPRESENT, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
    }

    @ParallelTest
    public void testNetworkPolicyNewKubernetesVersions() {
        Kafka kafkaAssembly = ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, emptyMap());
        kafkaAssembly.getSpec().getKafka().setRack(new RackBuilder().withTopologyKey("topology-key").build());
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        // Check Network Policies => Other namespace
        NetworkPolicy np = zc.generateNetworkPolicy("operator-namespace", null);

        LabelSelector podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, ZookeeperCluster.zookeeperClusterName(zc.getCluster())));
        assertThat(np.getSpec().getPodSelector(), is(podSelector));

        List<NetworkPolicyIngressRule> rules = np.getSpec().getIngress();
        assertThat(rules.size(), is(3));

        // Ports 2888 and 3888
        NetworkPolicyIngressRule zooRule = rules.get(0);
        assertThat(zooRule.getPorts().size(), is(2));
        assertThat(zooRule.getPorts().get(0).getPort(), is(new IntOrString(2888)));
        assertThat(zooRule.getPorts().get(1).getPort(), is(new IntOrString(3888)));

        assertThat(zooRule.getFrom().size(), is(1));
        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, ZookeeperCluster.zookeeperClusterName(zc.getCluster())));
        assertThat(zooRule.getFrom().get(0), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build()));

        // Port 2181
        NetworkPolicyIngressRule clientsRule = rules.get(1);
        assertThat(clientsRule.getPorts().size(), is(1));
        assertThat(clientsRule.getPorts().get(0).getPort(), is(new IntOrString(ZookeeperCluster.CLIENT_TLS_PORT)));

        assertThat(clientsRule.getFrom().size(), is(5));

        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, KafkaCluster.kafkaClusterName(zc.getCluster())));
        assertThat(clientsRule.getFrom().get(0), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build()));

        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, ZookeeperCluster.zookeeperClusterName(zc.getCluster())));
        assertThat(clientsRule.getFrom().get(1), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build()));

        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, EntityOperator.entityOperatorName(zc.getCluster())));
        assertThat(clientsRule.getFrom().get(2), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build()));

        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"));
        assertThat(clientsRule.getFrom().get(3), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).withNamespaceSelector(new LabelSelector()).build()));

        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_NAME_LABEL, CruiseControl.cruiseControlName(zc.getCluster())));
        assertThat(clientsRule.getFrom().get(4), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build()));

        // Port 9404
        NetworkPolicyIngressRule metricsRule = rules.get(2);
        assertThat(metricsRule.getPorts().size(), is(1));
        assertThat(metricsRule.getPorts().get(0).getPort(), is(new IntOrString(9404)));
        assertThat(metricsRule.getFrom().size(), is(0));

        // Check Network Policies => The same namespace
        np = zc.generateNetworkPolicy(namespace, null);
        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"));
        assertThat(np.getSpec().getIngress().get(1).getFrom().get(3), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build()));

        // Check Network Policies => The same namespace with namespace labels
        np = zc.generateNetworkPolicy(namespace, Labels.fromMap(Collections.singletonMap("nsLabelKey", "nsLabelValue")));
        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"));
        assertThat(np.getSpec().getIngress().get(1).getFrom().get(3), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).build()));

        // Check Network Policies => Other namespace with namespace labels
        np = zc.generateNetworkPolicy("operator-namespace", Labels.fromMap(Collections.singletonMap("nsLabelKey", "nsLabelValue")));
        podSelector = new LabelSelector();
        podSelector.setMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"));
        LabelSelector namespaceSelector = new LabelSelector();
        namespaceSelector.setMatchLabels(Collections.singletonMap("nsLabelKey", "nsLabelValue"));
        assertThat(np.getSpec().getIngress().get(1).getFrom().get(3), is(new NetworkPolicyPeerBuilder().withPodSelector(podSelector).withNamespaceSelector(namespaceSelector).build()));
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsPersistentWithClaimDeletion() {
        Kafka ka = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, zooConfigurationJson))
                .editSpec()
                .editZookeeper()
                .withNewPersistentClaimStorage().withStorageClass("gp2-ssd").withDeleteClaim(true).withSize("100Gi").endPersistentClaimStorage()
                .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);

        // Check Storage annotation on STS
        assertThat(zc.generateStatefulSet(true, ImagePullPolicy.NEVER, null).getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_STORAGE),
                is(ModelUtils.encodeStorageToJson(ka.getSpec().getZookeeper().getStorage())));

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = zc.generatePersistentVolumeClaims();

        assertThat(pvcs.size(), is(3));

        for (PersistentVolumeClaim pvc : pvcs) {
            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));
            assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
            assertThat(pvc.getMetadata().getName().startsWith(zc.VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(1));
            assertThat(pvc.getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM), is("true"));
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsPersistentWithoutClaimDeletion() {
        Kafka ka = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, zooConfigurationJson))
                .editSpec()
                .editZookeeper()
                .withNewPersistentClaimStorage().withStorageClass("gp2-ssd").withDeleteClaim(false).withSize("100Gi").endPersistentClaimStorage()
                .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);

        // Check Storage annotation on STS
        assertThat(zc.generateStatefulSet(true, ImagePullPolicy.NEVER, null).getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_STORAGE),
                is(ModelUtils.encodeStorageToJson(ka.getSpec().getZookeeper().getStorage())));

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = zc.generatePersistentVolumeClaims();

        assertThat(pvcs.size(), is(3));

        for (PersistentVolumeClaim pvc : pvcs) {
            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));
            assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
            assertThat(pvc.getMetadata().getName().startsWith(zc.VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(0));
            assertThat(pvc.getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM), is("false"));
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsPersistentWithOverride() {
        Kafka ka = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, zooConfigurationJson))
                .editSpec()
                .editZookeeper()
                .withNewPersistentClaimStorage()
                    .withStorageClass("gp2-ssd")
                    .withDeleteClaim(false)
                    .withSize("100Gi")
                    .withOverrides(new PersistentClaimStorageOverrideBuilder()
                            .withBroker(1)
                            .withStorageClass("gp2-ssd-az1")
                            .build())
                .endPersistentClaimStorage()
                .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);

        // Check Storage annotation on STS
        assertThat(zc.generateStatefulSet(true, ImagePullPolicy.NEVER, null).getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_STORAGE),
                is(ModelUtils.encodeStorageToJson(ka.getSpec().getZookeeper().getStorage())));

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = zc.generatePersistentVolumeClaims();

        assertThat(pvcs.size(), is(3));

        for (int i = 0; i < 3; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);

            assertThat(pvc.getSpec().getResources().getRequests().get("storage"), is(new Quantity("100Gi")));

            if (i != 1) {
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd"));
            } else {
                assertThat(pvc.getSpec().getStorageClassName(), is("gp2-ssd-az1"));
            }

            assertThat(pvc.getMetadata().getName().startsWith(zc.VOLUME_NAME), is(true));
            assertThat(pvc.getMetadata().getOwnerReferences().size(), is(0));
            assertThat(pvc.getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM), is("false"));
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsWithTemplate() {
        Kafka ka = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, zooConfigurationJson))
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewPersistentVolumeClaim()
                                .withNewMetadata()
                                    .withLabels(singletonMap("testLabel", "testValue"))
                                    .withAnnotations(singletonMap("testAnno", "testValue"))
                                .endMetadata()
                            .endPersistentVolumeClaim()
                        .endTemplate()
                        .withStorage(new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd")
                                        .withDeleteClaim(false)
                                        .withId(0)
                                        .withSize("100Gi")
                                        .withOverrides(new PersistentClaimStorageOverrideBuilder().withBroker(1).withStorageClass("gp2-ssd-az1").build())
                                        .build())
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = zc.generatePersistentVolumeClaims();

        assertThat(pvcs.size(), is(3));

        for (int i = 0; i < 3; i++) {
            PersistentVolumeClaim pvc = pvcs.get(i);
            assertThat(pvc.getMetadata().getLabels().get("testLabel"), is("testValue"));
            assertThat(pvc.getMetadata().getAnnotations().get("testAnno"), is("testValue"));
        }
    }

    @ParallelTest
    public void testGeneratePersistentVolumeClaimsEphemeral()    {
        Kafka ka = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, zooConfigurationJson))
                .editSpec()
                .editZookeeper()
                .withNewEphemeralStorage().endEphemeralStorage()
                .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);

        // Check Storage annotation on STS
        assertThat(zc.generateStatefulSet(true, ImagePullPolicy.NEVER, null).getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_STORAGE),
                is(ModelUtils.encodeStorageToJson(ka.getSpec().getZookeeper().getStorage())));

        // Check PVCs
        List<PersistentVolumeClaim> pvcs = zc.generatePersistentVolumeClaims();

        assertThat(pvcs.size(), is(0));
    }

    @ParallelTest
    public void testGenerateSTSWithPersistentVolumeEphemeral()    {
        Kafka ka = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, zooConfigurationJson))
                .editSpec()
                    .editZookeeper()
                        .withNewEphemeralStorage().endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(false, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(0).getEmptyDir().getSizeLimit(), is(nullValue()));
    }

    @ParallelTest
    public void testGenerateSTSWithPersistentVolumeEphemeralWithSizeLimit()    {
        String sizeLimit = "1Gi";
        Kafka ka = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, zooConfigurationJson))
                .editSpec()
                    .editZookeeper()
                        .withNewEphemeralStorage().withSizeLimit(sizeLimit).endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS);

        StatefulSet sts = zc.generateStatefulSet(false, null, null);
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes().get(0).getEmptyDir().getSizeLimit(), is(new Quantity("1", "Gi")));
    }

    @ParallelTest
    public void testStorageReverting() {
        SingleVolumeStorage ephemeral = new EphemeralStorageBuilder().build();
        SingleVolumeStorage persistent = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build();

        // Test Storage changes and how the are reverted

        Kafka ka = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, zooConfigurationJson))
                .editSpec()
                .editZookeeper()
                .withStorage(ephemeral)
                .endZookeeper()
                .endSpec()
                .build();
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS, persistent, replicas);
        assertThat(zc.getStorage(), is(persistent));

        ka = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, zooConfigurationJson))
                .editSpec()
                .editZookeeper()
                .withStorage(persistent)
                .endZookeeper()
                .endSpec()
                .build();
        zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ka, VERSIONS, ephemeral, replicas);

        // Storage is reverted
        assertThat(zc.getStorage(), is(ephemeral));

        // Warning status condition is set
        assertThat(zc.getWarningConditions().size(), is(1));
        assertThat(zc.getWarningConditions().get(0).getReason(), is("ZooKeeperStorage"));
    }

    @ParallelTest
    public void testStorageValidationAfterInitialDeployment() {
        assertThrows(InvalidResourceException.class, () -> {
            Storage oldStorage = new PersistentClaimStorageBuilder()
                    .withSize("100Gi")
                    .build();

            Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image,
                    healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, zooConfigurationJson))
                    .editSpec()
                    .editZookeeper()
                        .withStorage(new PersistentClaimStorageBuilder().build())
                        .endZookeeper()
                    .endSpec()
                    .build();
            ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS, oldStorage, replicas);
        });
    }

    @ParallelTest
    public void testZookeeperContainerEnvVars() {

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
        ContainerTemplate zookeeperContainer = new ContainerTemplate();
        zookeeperContainer.setEnv(testEnvs);

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, emptyMap()))
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withZookeeperContainer(zookeeperContainer)
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();

        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        List<EnvVar> zkEnvVars = zc.getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                zkEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                zkEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));

    }

    @ParallelTest
    public void testZookeeperContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = ZookeeperCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = ZookeeperCluster.ENV_VAR_ZOOKEEPER_METRICS_ENABLED;
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate zookeeperContainer = new ContainerTemplate();
        zookeeperContainer.setEnv(testEnvs);

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, emptyMap()))
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withZookeeperContainer(zookeeperContainer)
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();

        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        List<EnvVar> zkEnvVars = zc.getEnvVars();
        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                zkEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));
        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvTwoKey,
                zkEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(false));

    }

    @ParallelTest
    public void testZookeeperContainerSecurityContext() {

        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addNewDrop("ALL")
                .endCapabilities()
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, jmxMetricsConfig, configurationJson, emptyMap()))
                .editSpec()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewZookeeperContainer()
                                .withSecurityContext(securityContext)
                            .endZookeeperContainer()
                        .endTemplate()
                    .endZookeeper()
                .endSpec()
                .build();

        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);
        StatefulSet sts = zc.generateStatefulSet(false, null, null);

        assertThat(sts.getSpec().getTemplate().getSpec().getContainers(),
                hasItem(allOf(
                        hasProperty("name", equalTo(ZookeeperCluster.ZOOKEEPER_NAME)),
                        hasProperty("securityContext", equalTo(securityContext))
                )));
    }

    @ParallelTest
    public void testMetricsParsingFromConfigMap() {
        MetricsConfig metrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-metrics-configuration").withKey("config.yaml").build())
                .endValueFrom()
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout))
                .editSpec()
                    .editZookeeper()
                        .withMetricsConfig(metrics)
                    .endZookeeper()
                .endSpec()
                .build();

        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        assertThat(zc.isMetricsEnabled(), is(true));
        assertThat(zc.getMetricsConfigInCm(), is(metrics));
    }

    @ParallelTest
    public void testMetricsParsingNoMetrics() {
        ZookeeperCluster zc = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout), VERSIONS);

        assertThat(zc.isMetricsEnabled(), is(false));
        assertThat(zc.getMetricsConfigInCm(), is(nullValue()));
    }
}
