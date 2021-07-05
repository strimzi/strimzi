/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HTTPHeader;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeerBuilder;
import io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.CruiseControlSpecBuilder;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.MetricsConfig;
import io.strimzi.api.kafka.model.balancing.BrokerCapacity;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.template.IpFamily;
import io.strimzi.api.kafka.model.template.IpFamilyPolicy;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.cruisecontrol.Capacity;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import org.junit.jupiter.api.AfterAll;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.operator.cluster.model.CruiseControl.ENV_VAR_BROKER_CPU_UTILIZATION_CAPACITY;
import static io.strimzi.operator.cluster.model.CruiseControl.ENV_VAR_BROKER_DISK_MIB_CAPACITY;
import static io.strimzi.operator.cluster.model.CruiseControl.ENV_VAR_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY;
import static io.strimzi.operator.cluster.model.CruiseControl.ENV_VAR_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY;
import static io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlConfigurationParameters.CRUISE_CONTROL_ANOMALY_DETECTION_CONFIG_KEY;
import static io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlConfigurationParameters.CRUISE_CONTROL_DEFAULT_GOALS_CONFIG_KEY;
import static io.strimzi.operator.cluster.model.cruisecontrol.Capacity.DEFAULT_BROKER_CPU_UTILIZATION_CAPACITY;
import static io.strimzi.operator.cluster.model.cruisecontrol.Capacity.DEFAULT_BROKER_DISK_MIB_CAPACITY;
import static io.strimzi.operator.cluster.model.cruisecontrol.Capacity.DEFAULT_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY;
import static io.strimzi.operator.cluster.model.cruisecontrol.Capacity.DEFAULT_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY;
import static java.util.Collections.emptyList;
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
import static org.hamcrest.Matchers.hasItems;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({
        "checkstyle:ClassDataAbstractionCoupling",
        "checkstyle:ClassFanOutComplexity"
})
@ParallelSuite
public class CruiseControlTest {
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 1;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final String minInsyncReplicas = "2";
    private final Map<String, Object> metricsCm = singletonMap("animal", "wombat");
    private final String metricsCMName = "metrics-cm";
    private final JmxPrometheusExporterMetrics jmxMetricsConfig = io.strimzi.operator.cluster.TestUtils.getJmxPrometheusExporterMetrics("metrics-config.yml", metricsCMName);

    private final Map<String, Object> kafkaConfig = singletonMap(CruiseControl.MIN_INSYNC_REPLICAS, minInsyncReplicas);
    private final Map<String, Object> zooConfig = singletonMap("foo", "bar");
    private final Map<String, Object> ccConfig = new HashMap<String, Object>() {{
            putAll(CruiseControlConfiguration.getCruiseControlDefaultPropertiesMap());
            put("num.partition.metrics.windows", "2");
        }};

    private final CruiseControlConfiguration ccConfiguration = new CruiseControlConfiguration(Reconciliation.DUMMY_RECONCILIATION, ccConfig.entrySet());

    private final Storage kafkaStorage = new EphemeralStorage();
    private final SingleVolumeStorage zkStorage = new EphemeralStorage();
    private final InlineLogging kafkaLogJson = new InlineLogging();
    private final InlineLogging zooLogJson = new InlineLogging();
    private final String version = KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION;
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String kafkaHeapOpts = "-Xms" + AbstractModel.DEFAULT_JVM_XMS;
    private final String ccImage = "my-cruise-control-image";

    {
        kafkaLogJson.setLoggers(singletonMap("kafka.root.logger.level", "OFF"));
        zooLogJson.setLoggers(singletonMap("zookeeper.root.logger", "OFF"));
    }

    private final CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
            .withImage(ccImage)
            .withConfig(ccConfig)
            .build();

    private final CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, createKafka(cruiseControlSpec), VERSIONS);
    private Kafka createKafka(CruiseControlSpec cruiseControlSpec) {
        return new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .editKafka()
                        .withConfig(kafkaConfig)
                   .endKafka()
                  .withCruiseControl(cruiseControlSpec)
                .endSpec()
                .build();
    }

    private Map<String, String> expectedLabels(String name)    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                "my-user-label", "cromulent",
                Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND,
                Labels.STRIMZI_NAME_LABEL, name,
                Labels.KUBERNETES_NAME_LABEL, CruiseControl.APPLICATION_NAME,
                Labels.KUBERNETES_INSTANCE_LABEL, this.cluster,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + this.cluster,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziSelectorLabels().toMap();
    }

    private Map<String, String> expectedLabels()    {
        return expectedLabels(CruiseControlResources.deploymentName(cluster));
    }

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_CRUISE_CONTROL_METRICS_ENABLED).withValue(Boolean.toString(CruiseControl.DEFAULT_CRUISE_CONTROL_METRICS_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_STRIMZI_KAFKA_BOOTSTRAP_SERVERS).withValue(CruiseControl.defaultBootstrapServers(cluster)).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED).withValue(Boolean.toString(AbstractModel.DEFAULT_JVM_GC_LOGGING_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_MIN_INSYNC_REPLICAS).withValue(minInsyncReplicas).build());
        expected.add(new EnvVarBuilder().withName(ENV_VAR_BROKER_DISK_MIB_CAPACITY).withValue(Double.toString(DEFAULT_BROKER_DISK_MIB_CAPACITY)).build());
        expected.add(new EnvVarBuilder().withName(ENV_VAR_BROKER_CPU_UTILIZATION_CAPACITY).withValue(Integer.toString(DEFAULT_BROKER_CPU_UTILIZATION_CAPACITY)).build());
        expected.add(new EnvVarBuilder().withName(ENV_VAR_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY).withValue(Double.toString(DEFAULT_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY)).build());
        expected.add(new EnvVarBuilder().withName(ENV_VAR_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY).withValue(Double.toString(DEFAULT_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY)).build());
        expected.add(new EnvVarBuilder().withName(KafkaMirrorMakerCluster.ENV_VAR_KAFKA_HEAP_OPTS).withValue(kafkaHeapOpts).build());
        expected.add(new EnvVarBuilder().withName(CruiseControl.ENV_VAR_CRUISE_CONTROL_CONFIGURATION).withValue(ccConfiguration.getConfiguration()).build());

        return expected;
    }

    public String getCapacityConfigurationFromEnvVar(Kafka resource, String envVar) {
        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = cc.generateDeployment(true, null, null, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        // checks on the main Cruise Control container
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().get();
        List<EnvVar> ccEnvVars = ccContainer.getEnv();

        return ccEnvVars.stream().filter(var -> envVar.equals(var.getName())).map(EnvVar::getValue).findFirst().get();
    }

    @ParallelTest
    public void testBrokerCapacities() {
        // Test user defined capacities
        BrokerCapacity userDefinedBrokerCapacity = new BrokerCapacity();
        userDefinedBrokerCapacity.setDisk("20000M");
        userDefinedBrokerCapacity.setCpuUtilization(95);
        userDefinedBrokerCapacity.setInboundNetwork("50000KB/s");
        userDefinedBrokerCapacity.setOutboundNetwork("50000KB/s");

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withBrokerCapacity(userDefinedBrokerCapacity)
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        Capacity generatedCapacity = new Capacity(resource.getSpec());
        assertThat(getCapacityConfigurationFromEnvVar(resource, ENV_VAR_BROKER_DISK_MIB_CAPACITY), is(Double.toString(generatedCapacity.getDiskMiB())));
        assertThat(getCapacityConfigurationFromEnvVar(resource, ENV_VAR_BROKER_CPU_UTILIZATION_CAPACITY), is(Integer.toString(generatedCapacity.getCpuUtilization())));
        assertThat(getCapacityConfigurationFromEnvVar(resource, ENV_VAR_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY), is(Double.toString(generatedCapacity.getInboundNetworkKiBPerSecond())));
        assertThat(getCapacityConfigurationFromEnvVar(resource, ENV_VAR_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY), is(Double.toString(generatedCapacity.getOutboundNetworkKiBPerSecond())));

        // Test generated disk capacity
        JbodStorage jbodStorage = new JbodStorage();
        List<SingleVolumeStorage> volumes = new ArrayList<>();

        PersistentClaimStorage p1 = new PersistentClaimStorage();
        p1.setId(0);
        p1.setSize("50Gi");
        volumes.add(p1);

        PersistentClaimStorage p2 = new PersistentClaimStorage();
        p2.setId(1);
        p2.setSize("50G");
        volumes.add(p2);

        jbodStorage.setVolumes(volumes);

        resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
            .editSpec()
                .editKafka()
                    .withVersion(version)
                    .withStorage(jbodStorage)
                .endKafka()
                .withCruiseControl(cruiseControlSpec)
            .endSpec()
            .build();

        generatedCapacity = new Capacity(resource.getSpec());
        assertThat(getCapacityConfigurationFromEnvVar(resource, ENV_VAR_BROKER_DISK_MIB_CAPACITY), is(Double.toString(generatedCapacity.getDiskMiB())));
    }

    @ParallelTest
    public void testFromConfigMap() {
        assertThat(cc.namespace, is(namespace));
        assertThat(cc.cluster, is(cluster));
        assertThat(cc.getImage(), is(ccImage));
    }

    @ParallelTest
    public void testGenerateDeployment() {
        Deployment dep = cc.generateDeployment(true, null, null, null);
        HTTPHeader header = CruiseControl.generateAuthHttpHeader(CruiseControl.API_USER_NAME, CruiseControl.API_USER_PASSWORD);

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.size(), is(2));

        assertThat(dep.getMetadata().getName(), is(CruiseControlResources.deploymentName(cluster)));
        assertThat(dep.getMetadata().getNamespace(), is(namespace));
        assertThat(dep.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(dep.getMetadata().getOwnerReferences().get(0), is(cc.createOwnerReference()));

        // checks on the main Cruise Control container
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().get();
        assertThat(ccContainer.getImage(), is(cc.image));
        assertThat(ccContainer.getLivenessProbe().getInitialDelaySeconds(), is(Integer.valueOf(CruiseControl.DEFAULT_HEALTHCHECK_DELAY)));
        assertThat(ccContainer.getLivenessProbe().getTimeoutSeconds(), is(Integer.valueOf(CruiseControl.DEFAULT_HEALTHCHECK_TIMEOUT)));
        assertThat(ccContainer.getLivenessProbe().getHttpGet().getScheme(), is(CruiseControl.HTTPS_SCHEME));
        assertThat(ccContainer.getLivenessProbe().getHttpGet().getHttpHeaders().contains(header), is(true));
        assertThat(ccContainer.getReadinessProbe().getInitialDelaySeconds(), is(Integer.valueOf(CruiseControl.DEFAULT_HEALTHCHECK_DELAY)));
        assertThat(ccContainer.getReadinessProbe().getTimeoutSeconds(), is(Integer.valueOf(CruiseControl.DEFAULT_HEALTHCHECK_TIMEOUT)));
        assertThat(ccContainer.getReadinessProbe().getHttpGet().getScheme(), is(CruiseControl.HTTPS_SCHEME));
        assertThat(ccContainer.getReadinessProbe().getHttpGet().getHttpHeaders().contains(header), is(true));

        assertThat(ccContainer.getEnv(), is(getExpectedEnvVars()));
        assertThat(ccContainer.getPorts().size(), is(1));
        assertThat(ccContainer.getPorts().get(0).getName(), is(CruiseControl.REST_API_PORT_NAME));
        assertThat(ccContainer.getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(dep.getSpec().getStrategy().getType(), is("RollingUpdate"));

        // Test volumes
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        assertThat(volumes.size(), is(5));

        Volume volume = volumes.stream().filter(vol -> CruiseControl.TLS_SIDECAR_CC_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getSecret().getSecretName(), is(CruiseControl.secretName(cluster)));

        volume = volumes.stream().filter(vol -> CruiseControl.TLS_SIDECAR_CA_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getSecret().getSecretName(), is(AbstractModel.clusterCaCertSecretName(cluster)));

        volume = volumes.stream().filter(vol -> CruiseControl.LOG_AND_METRICS_CONFIG_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getConfigMap().getName(), is(CruiseControl.metricAndLogConfigsName(cluster)));

        volume = volumes.stream().filter(vol -> AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getEmptyDir().getMedium(), is("Memory"));

        volume = volumes.stream().filter(vol -> CruiseControl.TLS_SIDECAR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getEmptyDir().getMedium(), is("Memory"));

        // Test volume mounts
        // TLS sidecar container
        List<VolumeMount> volumesMounts = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        assertThat(volumesMounts.size(), is(4));

        VolumeMount volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.TLS_SIDECAR_CC_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.TLS_SIDECAR_CC_CERTS_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.TLS_SIDECAR_CA_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.LOG_AND_METRICS_CONFIG_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.LOG_AND_METRICS_CONFIG_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));

        // CC container
        volumesMounts = dep.getSpec().getTemplate().getSpec().getContainers().get(1).getVolumeMounts();
        assertThat(volumesMounts.size(), is(3));

        volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.TLS_SIDECAR_CC_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.TLS_SIDECAR_CC_CERTS_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.TLS_SIDECAR_CA_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(CruiseControl.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> CruiseControl.TLS_SIDECAR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME.equals(vol.getName())).findFirst().get();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));
    }

    @ParallelTest
    public void testEnvVars()   {
        assertThat(cc.getEnvVars(), is(getExpectedEnvVars()));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        Deployment dep = cc.generateDeployment(true, null, ImagePullPolicy.ALWAYS, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().get();
        assertThat(ccContainer.getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        dep = cc.generateDeployment(true, null,  ImagePullPolicy.IFNOTPRESENT, null);
        containers = dep.getSpec().getTemplate().getSpec().getContainers();
        ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().get();
        assertThat(ccContainer.getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
    }

    @ParallelTest
    public void testContainerTemplateEnvVars() {
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

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withNewTemplate()
                    .withNewCruiseControlContainer()
                       .withEnv(envVar1, envVar2)
                    .endCruiseControlContainer()
                .endTemplate()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);
        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        List<EnvVar> envVarList = cc.getEnvVars();

        assertThat(envVarList, hasItems(new EnvVar(testEnvOneKey, testEnvOneValue, null)));
        assertThat(envVarList, hasItems(new EnvVar(testEnvTwoKey, testEnvTwoValue, null)));
    }

    @ParallelTest
    public void testContainerTemplateEnvVarsWithKeyConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = "TEST_ENV_2";
        String testEnvTwoValue = "my-special-value";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withNewTemplate()
                    .withNewCruiseControlContainer()
                        .withEnv(envVar1, envVar2)
                    .endCruiseControlContainer()
                .endTemplate()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);
        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        List<EnvVar> envVarList = cc.getEnvVars();

        assertThat(envVarList, hasItems(new EnvVar(testEnvOneKey, testEnvOneValue, null)));
        assertThat(envVarList, hasItems(new EnvVar(testEnvTwoKey, testEnvTwoValue, null)));
    }

    @ParallelTest
    public void testCruiseControlNotDeployed() {
        Kafka resource = createKafka(null);
        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        try {
            assertThat(cc.generateDeployment(true, null, null, null), is(nullValue()));
            assertThat(cc.generateService(), is(nullValue()));
            assertThat(cc.generateSecret(null, true), is(nullValue()));
        } catch (Throwable expected) {
            assertEquals(NullPointerException.class, expected.getClass());
        }
    }

    @ParallelTest
    public void testGenerateService()   {
        Service svc = cc.generateService();

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getMetadata().getLabels(), is(expectedLabels(cc.getServiceName())));
        assertThat(svc.getSpec().getSelector(), is(expectedSelectorLabels()));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0).getName(), is(CruiseControl.REST_API_PORT_NAME));
        assertThat(svc.getSpec().getPorts().get(0).getPort(), is(Integer.valueOf(CruiseControl.REST_API_PORT)));
        assertThat(svc.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(svc.getSpec().getIpFamilies(), is(emptyList()));

        TestUtils.checkOwnerReference(cc.createOwnerReference(), svc);
    }

    @ParallelTest
    public void testGenerateServiceWhenDisabled()   {
        Kafka resource = createKafka(null);
        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        assertThrows(NullPointerException.class, () -> cc.generateService());
    }

    @ParallelTest
    public void testTemplate() {
        Map<String, String> depLabels = TestUtils.map("l1", "v1", "l2", "v2");
        Map<String, String> depAnots = TestUtils.map("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = TestUtils.map("l3", "v3", "l4", "v4");
        Map<String, String> podAnots = TestUtils.map("a3", "v3", "a4", "v4");

        Map<String, String> svcLabels = TestUtils.map("l5", "v5", "l6", "v6");
        Map<String, String> svcAnots = TestUtils.map("a5", "v5", "a6", "v6");

        Map<String, String> saLabels = TestUtils.map("l7", "v7", "l8", "v8");
        Map<String, String> saAnots = TestUtils.map("a7", "v7", "a8", "v8");

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

        HostAlias hostAlias1 = new HostAliasBuilder()
                .withHostnames("my-host-1", "my-host-2")
                .withIp("192.168.1.86")
                .build();
        HostAlias hostAlias2 = new HostAliasBuilder()
                .withHostnames("my-host-3")
                .withIp("192.168.1.87")
                .build();

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withNewTemplate()
                    .withNewDeployment()
                        .withNewMetadata()
                            .withLabels(depLabels)
                            .withAnnotations(depAnots)
                        .endMetadata()
                    .endDeployment()
                .withNewPod()
                    .withNewMetadata()
                        .withLabels(podLabels)
                        .withAnnotations(podAnots)
                    .endMetadata()
                    .withNewPriorityClassName("top-priority")
                    .withNewSchedulerName("my-scheduler")
                    .withHostAliases(hostAlias1, hostAlias2)
                    .withAffinity(affinity)
                    .withTolerations(tolerations)
                .endPod()
                .withNewApiService()
                    .withNewMetadata()
                        .withLabels(svcLabels)
                        .withAnnotations(svcAnots)
                    .endMetadata()
                    .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
                    .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                .endApiService()
                .withNewServiceAccount()
                    .withNewMetadata()
                        .withLabels(saLabels)
                        .withAnnotations(saAnots)
                    .endMetadata()
                .endServiceAccount()
                .endTemplate()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);
        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = cc.generateDeployment(true, depAnots, null, null);
        depLabels.putAll(expectedLabels());
        assertThat(dep.getMetadata().getLabels(), is(depLabels));
        assertThat(dep.getMetadata().getAnnotations(), is(depAnots));

        // Check Pods
        podLabels.putAll(expectedLabels());
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels(), is(podLabels));
        assertThat(dep.getSpec().getTemplate().getMetadata().getAnnotations(), is(podAnots));
        assertThat(dep.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));
        assertThat(dep.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(dep.getSpec().getTemplate().getSpec().getAffinity(), is(affinity));
        assertThat(dep.getSpec().getTemplate().getSpec().getTolerations(), is(tolerations));
        assertThat(dep.getSpec().getTemplate().getSpec().getHostAliases(), containsInAnyOrder(hostAlias1, hostAlias2));

        // Check Service
        svcLabels.putAll(expectedLabels());
        Service svc = cc.generateService();
        assertThat(svc.getMetadata().getLabels(), is(svcLabels));
        assertThat(svc.getMetadata().getAnnotations(),  is(svcAnots));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));

        // Check Service Account
        ServiceAccount sa = cc.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnots.entrySet()), is(true));
    }

    @ParallelTest
    public void testPodDisruptionBudget() {
        int maxUnavailable = 2;
        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                    .withNewTemplate()
                        .withNewPodDisruptionBudget()
                        .withMaxUnavailable(maxUnavailable)
                    .endPodDisruptionBudget()
                .endTemplate()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = cc.generateDeployment(true,  null, null, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().get();

        PodDisruptionBudget pdb = cc.generatePodDisruptionBudget();
        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(maxUnavailable)));
    }

    @ParallelTest
    public void testResources() {
        Map<String, Quantity> requests = new HashMap<>(2);
        requests.put("cpu", new Quantity("250m"));
        requests.put("memory", new Quantity("512Mi"));

        Map<String, Quantity> limits = new HashMap<>(2);
        limits.put("cpu", new Quantity("500m"));
        limits.put("memory", new Quantity("1024Mi"));

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withResources(new ResourceRequirementsBuilder().withLimits(limits).withRequests(requests).build())
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = cc.generateDeployment(true, null, null, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().get();

        assertThat(ccContainer.getResources().getLimits(), is(limits));
        assertThat(ccContainer.getResources().getRequests(), is(requests));
    }

    @ParallelTest
    public void testProbeConfiguration()   {
        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withNewLivenessProbe()
                        .withInitialDelaySeconds(healthDelay)
                        .withTimeoutSeconds(healthTimeout)
                .endLivenessProbe()
                .withNewReadinessProbe()
                       .withInitialDelaySeconds(healthDelay)
                       .withTimeoutSeconds(healthTimeout)
                .endReadinessProbe()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = cc.generateDeployment(true, null, null, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        // checks on the main Cruise Control container
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().get();
        assertThat(ccContainer.getImage(), is(cc.image));
        assertThat(ccContainer.getLivenessProbe().getInitialDelaySeconds(), is(Integer.valueOf(healthDelay)));
        assertThat(ccContainer.getLivenessProbe().getTimeoutSeconds(), is(Integer.valueOf(healthTimeout)));
        assertThat(ccContainer.getReadinessProbe().getInitialDelaySeconds(), is(Integer.valueOf(healthDelay)));
        assertThat(ccContainer.getReadinessProbe().getTimeoutSeconds(), is(Integer.valueOf(healthTimeout)));
    }

    @ParallelTest
    public void testApiSecurityEnabled() {
        Map<String, Object> config = ccConfig;
        config.put(CruiseControlConfigurationParameters.CRUISE_CONTROL_WEBSERVER_SECURITY_ENABLE.getValue(), true);
        config.put(CruiseControlConfigurationParameters.CRUISE_CONTROL_WEBSERVER_SSL_ENABLE.getValue(), true);
        testApiSecurity(config, CruiseControl.HTTPS_SCHEME, cc.getHttpHeaders());
    }

    @ParallelTest
    public void testApiSecurityDisabled() {
        Map<String, Object> config = ccConfig;
        config.put(CruiseControlConfigurationParameters.CRUISE_CONTROL_WEBSERVER_SECURITY_ENABLE.getValue(), false);
        config.put(CruiseControlConfigurationParameters.CRUISE_CONTROL_WEBSERVER_SSL_ENABLE.getValue(), false);
        testApiSecurity(config, CruiseControl.HTTP_SCHEME, null);
    }

    @ParallelTest
    public void testApiSecurityAuthenticationDisabled() {
        Map<String, Object> config = ccConfig;
        config.put(CruiseControlConfigurationParameters.CRUISE_CONTROL_WEBSERVER_SECURITY_ENABLE.getValue(), true);
        config.put(CruiseControlConfigurationParameters.CRUISE_CONTROL_WEBSERVER_SSL_ENABLE.getValue(), false);
        testApiSecurity(config, CruiseControl.HTTP_SCHEME, cc.getHttpHeaders());
    }

    @ParallelTest
    public void testApiSecurityAuthorizationDisabled() {
        Map<String, Object> config = ccConfig;
        config.put(CruiseControlConfigurationParameters.CRUISE_CONTROL_WEBSERVER_SECURITY_ENABLE.getValue(), false);
        config.put(CruiseControlConfigurationParameters.CRUISE_CONTROL_WEBSERVER_SSL_ENABLE.getValue(), true);
        testApiSecurity(config, CruiseControl.HTTPS_SCHEME, null);
    }

    public void testApiSecurity(Map<String, Object> config, String httpScheme, List<HTTPHeader> headers) {
        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig(config)
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        Deployment dep = cc.generateDeployment(true, null, null, null);
        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        // checks on the main Cruise Control container
        Container ccContainer = containers.stream().filter(container -> ccImage.equals(container.getImage())).findFirst().get();

        // Check for headers and URI scheme of probes
        Probe livenessProbe = ccContainer.getLivenessProbe();
        assertThat(livenessProbe.getHttpGet().getHttpHeaders(), is(headers));
        assertThat(livenessProbe.getHttpGet().getScheme(), is(httpScheme));

        Probe readinessProbe = ccContainer.getReadinessProbe();
        assertThat(readinessProbe.getHttpGet().getHttpHeaders(), is(headers));
        assertThat(readinessProbe.getHttpGet().getScheme(), is(httpScheme));
    }

    @ParallelTest
    public void testSecurityContext() {
        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig(ccConfig)
                .withNewTemplate()
                    .withNewPod()
                        .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                    .endPod()
                .endTemplate()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = cc.generateDeployment(true, null, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(notNullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(Long.valueOf(123)));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup(), is(Long.valueOf(456)));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser(), is(Long.valueOf(789)));
    }

    @ParallelTest
    public void testDefaultSecurityContext() {
        Deployment dep = cc.generateDeployment(true, null, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
    }

    @ParallelTest
    public void testCruiseControlContainerSecurityContext() {
        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addNewDrop("ALL")
                .endCapabilities()
                .build();

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig(ccConfig)
                .withNewTemplate()
                    .withNewCruiseControlContainer()
                        .withSecurityContext(securityContext)
                    .endCruiseControlContainer()
                .endTemplate()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = cc.generateDeployment(true, null, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getContainers(),
                hasItem(allOf(
                        hasProperty("name", equalTo(CruiseControl.CRUISE_CONTROL_CONTAINER_NAME)),
                        hasProperty("securityContext", equalTo(securityContext))
                )));
    }

    @ParallelTest
    public void testTlsSidecarContainerSecurityContext() {
        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addNewDrop("ALL")
                .endCapabilities()
                .build();

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig(ccConfig)
                .withNewTemplate()
                    .withNewTlsSidecarContainer()
                        .withSecurityContext(securityContext)
                    .endTlsSidecarContainer()
                .endTemplate()
                .build();

        Kafka resource = createKafka(cruiseControlSpec);

        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        Deployment dep = cc.generateDeployment(true, null, null, null);

        assertThat(dep.getSpec().getTemplate().getSpec().getContainers(),
                hasItem(allOf(
                        hasProperty("name", equalTo(CruiseControl.TLS_SIDECAR_NAME)),
                        hasProperty("securityContext", equalTo(securityContext))
                )));
    }

    @ParallelTest
    public void testRestApiPortNetworkPolicy() {
        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .withNewNamespaceSelector().endNamespaceSelector()
                .build();

        NetworkPolicy np = cc.generateNetworkPolicy("operator-namespace", null);

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElse(null);

        assertThat(rules.size(), is(1));
        assertThat(rules.contains(clusterOperatorPeer), is(true));
    }

    @ParallelTest
    public void testRestApiPortNetworkPolicyInTheSameNamespace() {
        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .build();

        NetworkPolicy np = cc.generateNetworkPolicy(namespace, null);

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElse(null);

        assertThat(rules.size(), is(1));
        assertThat(rules.contains(clusterOperatorPeer), is(true));
    }

    @ParallelTest
    public void testRestApiPortNetworkPolicyWithNamespaceLabels() {
        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector()
                    .withMatchLabels(Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, "cluster-operator"))
                .endPodSelector()
                .withNewNamespaceSelector()
                    .withMatchLabels(Collections.singletonMap("nsLabelKey", "nsLabelValue"))
                .endNamespaceSelector()
                .build();

        NetworkPolicy np = cc.generateNetworkPolicy(null, Labels.fromMap(Collections.singletonMap("nsLabelKey", "nsLabelValue")));

        assertThat(np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).findFirst().orElse(null), is(notNullValue()));

        List<NetworkPolicyPeer> rules = np.getSpec().getIngress().stream().filter(ing -> ing.getPorts().get(0).getPort().equals(new IntOrString(CruiseControl.REST_API_PORT))).map(NetworkPolicyIngressRule::getFrom).findFirst().orElse(null);

        assertThat(rules.size(), is(1));
        assertThat(rules.contains(clusterOperatorPeer), is(true));
    }

    @ParallelTest
    public void testGoalsCheck() {

        String customGoals = "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal," +
                "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal";

        Map<String, Object> customGoalConfig = ccConfig;
        customGoalConfig.put(CRUISE_CONTROL_DEFAULT_GOALS_CONFIG_KEY.getValue(), customGoals);

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withImage(ccImage)
                .withConfig(customGoalConfig)
                .build();

        Kafka resourceWithCustomGoals = createKafka(cruiseControlSpec);

        CruiseControl cruiseControlWithCustomGoals = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resourceWithCustomGoals, VERSIONS);

        String anomalyDetectionGoals =  cruiseControlWithCustomGoals
                .getConfiguration().asOrderedProperties().asMap()
                .get(CRUISE_CONTROL_ANOMALY_DETECTION_CONFIG_KEY.getValue());

        assertThat(anomalyDetectionGoals, is(customGoals));
    }

    @ParallelTest
    public void testMetricsParsingFromConfigMap() {
        MetricsConfig metrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder().withName("my-metrics-configuration").withKey("config.yaml").build())
                .endValueFrom()
                .build();

        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
                .withMetricsConfig(metrics)
                .build();

        Kafka kafkaAssembly = createKafka(cruiseControlSpec);
        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        assertThat(cc.isMetricsEnabled(), is(true));
        assertThat(cc.getMetricsConfigInCm(), is(metrics));
    }

    @ParallelTest
    public void testMetricsParsingNoMetrics() {
        CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder().build();
        Kafka kafkaAssembly = createKafka(cruiseControlSpec);

        CruiseControl cc = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

        assertThat(cc.isMetricsEnabled(), is(false));
        assertThat(cc.getMetricsConfigInCm(), is(nullValue()));
    }

    @AfterAll
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}