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
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaExporterSpec;
import io.strimzi.api.kafka.model.KafkaExporterSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import org.junit.jupiter.api.AfterAll;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

@ParallelSuite
public class KafkaExporterTest {
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final Map<String, Object> metricsCm = singletonMap("animal", "wombat");
    private final String metricsCmJson = "{\"animal\":\"wombat\"}";
    private final String metricsCMName = "metrics-cm";
    private final JmxPrometheusExporterMetrics jmxMetricsConfig = io.strimzi.operator.cluster.TestUtils.getJmxPrometheusExporterMetrics(AbstractModel.ANCILLARY_CM_KEY_METRICS, metricsCMName);
    private final Map<String, Object> kafkaConfig = singletonMap("foo", "bar");
    private final Map<String, Object> zooConfig = singletonMap("foo", "bar");
    private final Storage kafkaStorage = new EphemeralStorage();
    private final SingleVolumeStorage zkStorage = new EphemeralStorage();
    private final InlineLogging kafkaLogJson = new InlineLogging();
    private final InlineLogging zooLogJson = new InlineLogging();
    private final String exporterOperatorLogging = "debug";
    private final String version = KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION;
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    {
        kafkaLogJson.setLoggers(singletonMap("kafka.root.logger.level", "OFF"));
        zooLogJson.setLoggers(singletonMap("zookeeper.root.logger", "OFF"));
    }

    private final String keImage = "my-exporter-image";
    private final String groupRegex = "my-group-.*";
    private final String topicRegex = "my-topic-.*";

    private final KafkaExporterSpec exporterOperator = new KafkaExporterSpecBuilder()
            .withLogging(exporterOperatorLogging)
            .withGroupRegex(groupRegex)
            .withTopicRegex(topicRegex)
            .withImage(keImage)
            .withEnableSaramaLogging(true)
            .build();
    private final Kafka resource =
            new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                    .editSpec()
                    .editKafka()
                    .withVersion(version)
                    .endKafka()
                    .withKafkaExporter(exporterOperator)
                    .endSpec()
                    .build();
    private final KafkaExporter ke = KafkaExporter.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

    public void checkOwnerReference(OwnerReference ownerRef, HasMetadata resource)  {
        assertThat(resource.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(resource.getMetadata().getOwnerReferences().get(0), is(ownerRef));
    }

    private Map<String, String> expectedLabels(String name)    {
        return TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                "my-user-label", "cromulent",
                Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND,
                Labels.STRIMZI_NAME_LABEL, name,
                Labels.KUBERNETES_NAME_LABEL, KafkaExporter.APPLICATION_NAME,
                Labels.KUBERNETES_INSTANCE_LABEL, this.cluster,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + this.cluster,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    private Map<String, String> expectedSelectorLabels()    {
        return Labels.fromMap(expectedLabels()).strimziSelectorLabels().toMap();
    }

    private Map<String, String> expectedLabels()    {
        return expectedLabels(KafkaExporterResources.deploymentName(cluster));
    }

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_LOGGING).withValue(exporterOperatorLogging).build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_KAFKA_VERSION).withValue(version).build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_GROUP_REGEX).withValue(groupRegex).build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_TOPIC_REGEX).withValue(topicRegex).build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_KAFKA_SERVER).withValue("foo-kafka-bootstrap:" + KafkaCluster.REPLICATION_PORT).build());
        expected.add(new EnvVarBuilder().withName(KafkaExporter.ENV_VAR_KAFKA_EXPORTER_ENABLE_SARAMA).withValue("true").build());
        return expected;
    }

    @ParallelTest
    public void testFromConfigMapDefaultConfig() {
        Kafka resource = ResourceUtils.createKafka(namespace, cluster, replicas, null,
                healthDelay, healthTimeout, jmxMetricsConfig, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, kafkaLogJson, zooLogJson, new KafkaExporterSpec(), null);
        KafkaExporter ke = KafkaExporter.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);
        assertThat(ke.getImage(), is(KafkaVersionTestUtils.DEFAULT_KAFKA_IMAGE));
        assertThat(ke.logging, is("info"));
        assertThat(ke.groupRegex, is(".*"));
        assertThat(ke.topicRegex, is(".*"));
        assertThat(ke.saramaLoggingEnabled, is(false));
    }

    @ParallelTest
    public void testFromConfigMap() {
        assertThat(ke.namespace, is(namespace));
        assertThat(ke.cluster, is(cluster));
        assertThat(ke.getImage(), is(keImage));
        assertThat(ke.logging, is("debug"));
        assertThat(ke.groupRegex, is("my-group-.*"));
        assertThat(ke.topicRegex, is("my-topic-.*"));
        assertThat(ke.saramaLoggingEnabled, is(true));
    }

    @ParallelTest
    public void testGenerateDeployment() {
        Deployment dep = ke.generateDeployment(true, null, null);

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.size(), is(1));

        assertThat(dep.getMetadata().getName(), is(KafkaExporterResources.deploymentName(cluster)));
        assertThat(dep.getMetadata().getNamespace(), is(namespace));
        assertThat(dep.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(dep.getMetadata().getOwnerReferences().get(0), is(ke.createOwnerReference()));

        // checks on the main Exporter container
        assertThat(containers.get(0).getImage(), is(ke.image));
        assertThat(containers.get(0).getEnv(), is(getExpectedEnvVars()));
        assertThat(containers.get(0).getPorts().size(), is(1));
        assertThat(containers.get(0).getPorts().get(0).getName(), is(KafkaExporter.METRICS_PORT_NAME));
        assertThat(containers.get(0).getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(dep.getSpec().getStrategy().getType(), is("RollingUpdate"));

        // Test volumes
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        assertThat(volumes.size(), is(3));

        Volume volume = volumes.stream().filter(vol -> AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getEmptyDir().getMedium(), is("Memory"));

        volume = volumes.stream().filter(vol -> KafkaExporter.CLUSTER_CA_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getSecret().getSecretName(), is(KafkaResources.clusterCaCertificateSecretName(cluster)));

        volume = volumes.stream().filter(vol -> KafkaExporter.KAFKA_EXPORTER_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getSecret().getSecretName(), is(KafkaExporterResources.secretName(cluster)));

        // Test volume mounts
        List<VolumeMount> volumesMounts = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        assertThat(volumesMounts.size(), is(3));

        VolumeMount volumeMount = volumesMounts.stream().filter(vol -> AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));

        volumeMount = volumesMounts.stream().filter(vol -> KafkaExporter.CLUSTER_CA_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(KafkaExporter.CLUSTER_CA_CERTS_VOLUME_MOUNT));

        volumeMount = volumesMounts.stream().filter(vol -> KafkaExporter.KAFKA_EXPORTER_CERTS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(KafkaExporter.KAFKA_EXPORTER_CERTS_VOLUME_MOUNT));

    }

    @ParallelTest
    public void testEnvVars()   {
        assertThat(ke.getEnvVars(), is(getExpectedEnvVars()));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        Deployment dep = ke.generateDeployment(true, ImagePullPolicy.ALWAYS, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        dep = ke.generateDeployment(true, ImagePullPolicy.IFNOTPRESENT, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
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

        KafkaExporterSpec exporterSpec = new KafkaExporterSpecBuilder()
                .withLogging(exporterOperatorLogging)
                .withGroupRegex(groupRegex)
                .withTopicRegex(topicRegex)
                .withImage(keImage)
                .withNewTemplate()
                    .withNewContainer()
                        .withEnv(envVar1, envVar2)
                    .endContainer()
                .endTemplate()
                .build();

        Kafka resource = ResourceUtils.createKafka(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, jmxMetricsConfig, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, kafkaLogJson, zooLogJson, exporterSpec, null);
        KafkaExporter ke = KafkaExporter.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

        List<EnvVar> kafkaEnvVars = ke.getEnvVars();
        assertThat(kafkaEnvVars.stream().filter(var -> testEnvOneKey.equals(var.getName())).map(EnvVar::getValue).findFirst().orElseThrow(), is(testEnvOneValue));
        assertThat(kafkaEnvVars.stream().filter(var -> testEnvTwoKey.equals(var.getName())).map(EnvVar::getValue).findFirst().orElseThrow(), is(testEnvTwoValue));
    }

    @ParallelTest
    public void testContainerTemplateEnvVarsWithKeyConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = KafkaExporter.ENV_VAR_KAFKA_EXPORTER_GROUP_REGEX;
        String testEnvTwoValue = "my-special-value";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        KafkaExporterSpec exporterSpec = new KafkaExporterSpecBuilder()
                .withLogging(exporterOperatorLogging)
                .withGroupRegex(groupRegex)
                .withTopicRegex(topicRegex)
                .withImage(keImage)
                .withNewTemplate()
                    .withNewContainer()
                        .withEnv(envVar1, envVar2)
                    .endContainer()
                .endTemplate()
                .build();

        Kafka resource = ResourceUtils.createKafka(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, jmxMetricsConfig, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, kafkaLogJson, zooLogJson, exporterSpec, null);
        KafkaExporter ke = KafkaExporter.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

        List<EnvVar> kafkaEnvVars = ke.getEnvVars();
        assertThat(kafkaEnvVars.stream().filter(var -> testEnvOneKey.equals(var.getName())).map(EnvVar::getValue).findFirst().orElseThrow(), is(testEnvOneValue));
        assertThat(kafkaEnvVars.stream().filter(var -> testEnvTwoKey.equals(var.getName())).map(EnvVar::getValue).findFirst().orElseThrow(), is(groupRegex));
    }

    @ParallelTest
    public void testExporterNotDeployed() {
        Kafka resource = ResourceUtils.createKafka(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, jmxMetricsConfig, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, kafkaLogJson, zooLogJson, null, null);
        KafkaExporter ke = KafkaExporter.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

        assertThat(ke.generateDeployment(true, null, null), is(nullValue()));
        assertThat(ke.generateSecret(null, true), is(nullValue()));
    }

    @ParallelTest
    public void testGenerateDeploymentWhenDisabled()   {
        Kafka resource = ResourceUtils.createKafka(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, jmxMetricsConfig, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, kafkaLogJson, zooLogJson, null, null);
        KafkaExporter ke = KafkaExporter.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

        assertThat(ke.generateDeployment(true, null, null), is(nullValue()));
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

        Map<String, String> saLabels = TestUtils.map("l5", "v5", "l6", "v6");
        Map<String, String> saAnots = TestUtils.map("a5", "v5", "a6", "v6");

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

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewKafkaExporter()
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
                                .withPriorityClassName("top-priority")
                                .withNewSchedulerName("my-scheduler")
                                .withAffinity(affinity)
                                .withTolerations(tolerations)
                                .withTopologySpreadConstraints(tsc1, tsc2)
                                .withEnableServiceLinks(false)
                            .endPod()
                            .withNewServiceAccount()
                                .withNewMetadata()
                                    .withLabels(saLabels)
                                    .withAnnotations(saAnots)
                                .endMetadata()
                            .endServiceAccount()
                        .endTemplate()
                    .endKafkaExporter()
                .endSpec()
                .build();
        KafkaExporter ke = KafkaExporter.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

        // Check Deployment
        Deployment dep = ke.generateDeployment(true, null, null);
        assertThat(dep.getMetadata().getLabels().entrySet().containsAll(expectedDepLabels.entrySet()), is(true));
        assertThat(dep.getMetadata().getAnnotations().entrySet().containsAll(depAnots.entrySet()), is(true));

        // Check Pods
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));
        assertThat(dep.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(dep.getSpec().getTemplate().getSpec().getAffinity(), is(affinity));
        assertThat(dep.getSpec().getTemplate().getSpec().getTolerations(), is(tolerations));
        assertThat(dep.getSpec().getTemplate().getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1, tsc2));
        assertThat(dep.getSpec().getTemplate().getSpec().getEnableServiceLinks(), is(false));

        // Check Service Account
        ServiceAccount sa = ke.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnots.entrySet()), is(true));
    }

    @AfterAll
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}
