/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.openshift.api.model.BuildConfig;
import io.strimzi.api.kafka.model.ContainerEnvVarBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.build.Artifact;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
public class KafkaConnectBuildTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    private final String cluster = "my-connect";
    private final String namespace = "my-ns";

    private final Artifact jarArtifactNoChecksum = new JarArtifactBuilder()
            .withUrl("https://mydomain.tld/my.jar")
            .build();

    private final Artifact jarArtifactWithChecksum = new JarArtifactBuilder()
            .withUrl("https://mydomain.tld/my2.jar")
            .withSha512sum("sha-512-checksum")
            .build();

    private final List<String> defaultArgs = List.of("--dockerfile=/dockerfile/Dockerfile",
            "--context=dir://workspace",
            "--image-name-with-digest-file=/dev/termination-log",
            "--destination=my-image:latest");

    @ParallelTest
    public void testFromCrd()   {
        KafkaConnect kc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(cluster)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers("my-kafka:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                            .withNewPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(new PluginBuilder().withName("my-connector").withArtifacts(jarArtifactWithChecksum).build(),
                                new PluginBuilder().withName("my-connector2").withArtifacts(jarArtifactNoChecksum).build())
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild.fromCrd(new Reconciliation("test", kc.getKind(), kc.getMetadata().getNamespace(), kc.getMetadata().getName()), kc, VERSIONS);
    }

    @ParallelTest
    public void testValidationPluginsExist()   {
        KafkaConnect kc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(cluster)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers("my-kafka:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                            .withNewPushSecret("my-docker-credentials")
                        .endDockerOutput()
                    .endBuild()
                .endSpec()
                .build();

        assertThrows(InvalidResourceException.class, () -> {
            KafkaConnectBuild.fromCrd(new Reconciliation("test", kc.getKind(), kc.getMetadata().getNamespace(), kc.getMetadata().getName()), kc, VERSIONS);
        });
    }

    @ParallelTest
    public void testValidationArtifactsExist()   {
        KafkaConnect kc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(cluster)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers("my-kafka:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                            .withNewPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(new PluginBuilder().withName("my-connector").build())
                    .endBuild()
                .endSpec()
                .build();

        assertThrows(InvalidResourceException.class, () -> {
            KafkaConnectBuild.fromCrd(new Reconciliation("test", kc.getKind(), kc.getMetadata().getNamespace(), kc.getMetadata().getName()), kc, VERSIONS);
        });
    }

    @ParallelTest
    public void testValidationUniqueNames()   {
        KafkaConnect kc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(cluster)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers("my-kafka:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                            .withNewPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(new PluginBuilder().withName("my-connector").withArtifacts(jarArtifactWithChecksum).build(),
                                new PluginBuilder().withName("my-connector").withArtifacts(jarArtifactNoChecksum).build())
                    .endBuild()
                .endSpec()
                .build();

        assertThrows(InvalidResourceException.class, () -> {
            KafkaConnectBuild.fromCrd(new Reconciliation("test", kc.getKind(), kc.getMetadata().getNamespace(), kc.getMetadata().getName()), kc, VERSIONS);
        });
    }

    @ParallelTest
    public void testDeployment()   {
        Map<String, Quantity> limit = new HashMap<>();
        limit.put("cpu", new Quantity("500m"));
        limit.put("memory", new Quantity("512Mi"));

        Map<String, Quantity> request = new HashMap<>();
        request.put("cpu", new Quantity("1000m"));
        request.put("memory", new Quantity("1Gi"));

        KafkaConnect kc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(cluster)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers("my-kafka:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                            .withNewPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(new PluginBuilder().withName("my-connector").withArtifacts(jarArtifactWithChecksum).build(),
                                new PluginBuilder().withName("my-connector2").withArtifacts(jarArtifactNoChecksum).build())
                        .withResources(new ResourceRequirementsBuilder().withLimits(limit).withRequests(request).build())
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", kc.getKind(), kc.getMetadata().getNamespace(), kc.getMetadata().getName()), kc, VERSIONS);

        Pod pod = build.generateBuilderPod(true, ImagePullPolicy.IFNOTPRESENT, null, null);
        assertThat(pod.getMetadata().getName(), is(KafkaConnectResources.buildPodName(cluster)));
        assertThat(pod.getMetadata().getNamespace(), is(namespace));

        Map<String, String> expectedDeploymentLabels = TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                Labels.STRIMZI_NAME_LABEL, KafkaConnectResources.buildPodName(cluster),
                Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND,
                Labels.KUBERNETES_NAME_LABEL, KafkaConnectBuild.APPLICATION_NAME,
                Labels.KUBERNETES_INSTANCE_LABEL, this.cluster,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + this.cluster,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
        assertThat(pod.getMetadata().getLabels(), is(expectedDeploymentLabels));
        assertThat(pod.getSpec().getServiceAccountName(), is(KafkaConnectResources.buildServiceAccountName(cluster)));
        assertThat(pod.getSpec().getContainers().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getArgs(), is(defaultArgs));
        assertThat(pod.getSpec().getContainers().get(0).getName(), is(KafkaConnectResources.buildPodName(this.cluster)));
        assertThat(pod.getSpec().getContainers().get(0).getImage(), is(build.image));
        assertThat(pod.getSpec().getContainers().get(0).getPorts().size(), is(0));
        assertThat(pod.getSpec().getContainers().get(0).getResources().getLimits(), is(limit));
        assertThat(pod.getSpec().getContainers().get(0).getResources().getRequests(), is(request));
        assertThat(pod.getSpec().getVolumes().size(), is(3));
        assertThat(pod.getSpec().getVolumes().get(0).getName(), is("workspace"));
        assertThat(pod.getSpec().getVolumes().get(0).getEmptyDir(), is(notNullValue()));
        assertThat(pod.getSpec().getVolumes().get(1).getName(), is("dockerfile"));
        assertThat(pod.getSpec().getVolumes().get(1).getConfigMap().getName(), is(KafkaConnectResources.dockerFileConfigMapName(cluster)));
        assertThat(pod.getSpec().getVolumes().get(2).getName(), is("docker-credentials"));
        assertThat(pod.getSpec().getVolumes().get(2).getSecret().getSecretName(), is("my-docker-credentials"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(3));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("workspace"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/workspace"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is("dockerfile"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/dockerfile"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getName(), is("docker-credentials"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(2).getMountPath(), is("/kaniko/.docker"));
        assertThat(pod.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(pod.getMetadata().getOwnerReferences().get(0), is(build.createOwnerReference()));
    }

    @ParallelTest
    public void testDeploymentWithoutPushSecret()   {
        KafkaConnect kc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(cluster)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers("my-kafka:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                        .endDockerOutput()
                        .withPlugins(new PluginBuilder().withName("my-connector").withArtifacts(jarArtifactWithChecksum).build(),
                                new PluginBuilder().withName("my-connector2").withArtifacts(jarArtifactNoChecksum).build())
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", kc.getKind(), kc.getMetadata().getNamespace(), kc.getMetadata().getName()), kc, VERSIONS);

        Pod pod = build.generateBuilderPod(true, ImagePullPolicy.IFNOTPRESENT, null, null);
        assertThat(pod.getSpec().getVolumes().size(), is(2));
        assertThat(pod.getSpec().getContainers().get(0).getArgs(), is(defaultArgs));
        assertThat(pod.getSpec().getVolumes().get(0).getName(), is("workspace"));
        assertThat(pod.getSpec().getVolumes().get(0).getEmptyDir(), is(notNullValue()));
        assertThat(pod.getSpec().getVolumes().get(1).getName(), is("dockerfile"));
        assertThat(pod.getSpec().getVolumes().get(1).getConfigMap().getName(), is(KafkaConnectResources.dockerFileConfigMapName(cluster)));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().size(), is(2));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(), is("workspace"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(), is("/workspace"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getName(), is("dockerfile"));
        assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(1).getMountPath(), is("/dockerfile"));
    }

    @ParallelTest
    public void testConfigMap()   {
        KafkaConnect kc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(cluster)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers("my-kafka:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                            .withNewPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(new PluginBuilder().withName("my-connector").withArtifacts(jarArtifactWithChecksum).build(),
                                new PluginBuilder().withName("my-connector2").withArtifacts(jarArtifactNoChecksum).build())
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", kc.getKind(), kc.getMetadata().getNamespace(), kc.getMetadata().getName()), kc, VERSIONS);

        KafkaConnectDockerfile dockerfile = new KafkaConnectDockerfile("my-image:latest", kc.getSpec().getBuild());
        ConfigMap cm = build.generateDockerfileConfigMap(dockerfile);

        assertThat(cm.getMetadata().getName(), is(KafkaConnectResources.dockerFileConfigMapName(cluster)));
        assertThat(cm.getMetadata().getNamespace(), is(namespace));
        assertThat(cm.getData().get("Dockerfile"), is(dockerfile.getDockerfile()));
        assertThat(cm.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(cm.getMetadata().getOwnerReferences().get(0), is(build.createOwnerReference()));
    }

    @ParallelTest
    public void testBuildconfigWithDockerOutput()   {
        Map<String, Quantity> limit = new HashMap<>();
        limit.put("cpu", new Quantity("500m"));
        limit.put("memory", new Quantity("512Mi"));

        Map<String, Quantity> request = new HashMap<>();
        request.put("cpu", new Quantity("1000m"));
        request.put("memory", new Quantity("1Gi"));

        KafkaConnect kc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(cluster)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers("my-kafka:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                            .withNewPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(new PluginBuilder().withName("my-connector").withArtifacts(jarArtifactWithChecksum).build(),
                                new PluginBuilder().withName("my-connector2").withArtifacts(jarArtifactNoChecksum).build())
                        .withResources(new ResourceRequirementsBuilder().withLimits(limit).withRequests(request).build())
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", kc.getKind(), kc.getMetadata().getNamespace(), kc.getMetadata().getName()), kc, VERSIONS);

        KafkaConnectDockerfile dockerfile = new KafkaConnectDockerfile("my-image:latest", kc.getSpec().getBuild());
        BuildConfig bc = build.generateBuildConfig(dockerfile);
        assertThat(bc.getMetadata().getName(), is(KafkaConnectResources.buildConfigName(cluster)));
        assertThat(bc.getMetadata().getNamespace(), is(namespace));

        Map<String, String> expectedDeploymentLabels = TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                Labels.STRIMZI_NAME_LABEL, KafkaConnectResources.buildPodName(cluster),
                Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND,
                Labels.KUBERNETES_NAME_LABEL, KafkaConnectBuild.APPLICATION_NAME,
                Labels.KUBERNETES_INSTANCE_LABEL, this.cluster,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + this.cluster,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
        assertThat(bc.getMetadata().getLabels(), is(expectedDeploymentLabels));
        assertThat(bc.getSpec().getSource().getDockerfile(), is(dockerfile.getDockerfile()));
        assertThat(bc.getSpec().getOutput().getTo().getKind(), is("DockerImage"));
        assertThat(bc.getSpec().getOutput().getTo().getName(), is("my-image:latest"));
        assertThat(bc.getSpec().getOutput().getPushSecret().getName(), is("my-docker-credentials"));
        assertThat(bc.getSpec().getStrategy().getDockerStrategy(), is(notNullValue()));
        assertThat(bc.getSpec().getResources().getLimits(), is(limit));
        assertThat(bc.getSpec().getResources().getRequests(), is(request));
        assertThat(bc.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(bc.getMetadata().getOwnerReferences().get(0), is(build.createOwnerReference()));
    }

    @ParallelTest
    public void testBuildconfigWithImageStreamOutput()   {
        KafkaConnect kc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(cluster)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers("my-kafka:9092")
                    .withNewBuild()
                        .withNewImageStreamOutput()
                            .withImage("my-image:latest")
                        .endImageStreamOutput()
                        .withPlugins(new PluginBuilder().withName("my-connector").withArtifacts(jarArtifactWithChecksum).build(),
                                new PluginBuilder().withName("my-connector2").withArtifacts(jarArtifactNoChecksum).build())
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", kc.getKind(), kc.getMetadata().getNamespace(), kc.getMetadata().getName()), kc, VERSIONS);

        KafkaConnectDockerfile dockerfile = new KafkaConnectDockerfile("my-image:latest", kc.getSpec().getBuild());
        BuildConfig bc = build.generateBuildConfig(dockerfile);
        assertThat(bc.getMetadata().getName(), is(KafkaConnectResources.buildConfigName(cluster)));
        assertThat(bc.getMetadata().getNamespace(), is(namespace));

        Map<String, String> expectedDeploymentLabels = TestUtils.map(Labels.STRIMZI_CLUSTER_LABEL, this.cluster,
                Labels.STRIMZI_NAME_LABEL, KafkaConnectResources.buildPodName(cluster),
                Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND,
                Labels.KUBERNETES_NAME_LABEL, KafkaConnectBuild.APPLICATION_NAME,
                Labels.KUBERNETES_INSTANCE_LABEL, this.cluster,
                Labels.KUBERNETES_PART_OF_LABEL, Labels.APPLICATION_NAME + "-" + this.cluster,
                Labels.KUBERNETES_MANAGED_BY_LABEL, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
        assertThat(bc.getMetadata().getLabels(), is(expectedDeploymentLabels));
        assertThat(bc.getSpec().getSource().getDockerfile(), is(dockerfile.getDockerfile()));
        assertThat(bc.getSpec().getOutput().getTo().getKind(), is("ImageStreamTag"));
        assertThat(bc.getSpec().getOutput().getTo().getName(), is("my-image:latest"));
        assertThat(bc.getSpec().getStrategy().getDockerStrategy(), is(notNullValue()));
        assertThat(bc.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(bc.getMetadata().getOwnerReferences().get(0), is(build.createOwnerReference()));
    }

    @ParallelTest
    public void testTemplate()   {
        Map<String, String> buildPodLabels = TestUtils.map("l1", "v1", "l2", "v2");
        Map<String, String> buildPodAnnos = TestUtils.map("a1", "v1", "a2", "v2");

        Map<String, String> buildConfigLabels = TestUtils.map("l3", "v3", "l4", "v4");
        Map<String, String> buildConfigAnnos = TestUtils.map("a3", "v3", "a4", "v4");

        Map<String, String> saLabels = TestUtils.map("l5", "v5", "l6", "v6");
        Map<String, String> saAnots = TestUtils.map("a5", "v5", "a6", "v6");

        KafkaConnect kc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(cluster)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers("my-kafka:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                            .withNewPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(new PluginBuilder().withName("my-connector").withArtifacts(jarArtifactWithChecksum).build(),
                                new PluginBuilder().withName("my-connector2").withArtifacts(jarArtifactNoChecksum).build())
                    .endBuild()
                    .withNewTemplate()
                        .withNewBuildPod()
                            .withNewMetadata()
                                .withLabels(buildPodLabels)
                                .withAnnotations(buildPodAnnos)
                            .endMetadata()
                            .withPriorityClassName("top-priority")
                            .withSchedulerName("my-scheduler")
                            .withEnableServiceLinks(false)
                        .endBuildPod()
                        .withNewBuildContainer()
                            .withEnv(new ContainerEnvVarBuilder().withName("TEST_ENV_VAR").withValue("testValue").build())
                        .endBuildContainer()
                        .withNewBuildConfig()
                            .withNewMetadata()
                                .withLabels(buildConfigLabels)
                                .withAnnotations(buildConfigAnnos)
                            .endMetadata()
                        .endBuildConfig()
                        .withNewBuildServiceAccount()
                            .withNewMetadata()
                                .withLabels(saLabels)
                                .withAnnotations(saAnots)
                            .endMetadata()
                        .endBuildServiceAccount()
                    .endTemplate()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", kc.getKind(), kc.getMetadata().getNamespace(), kc.getMetadata().getName()), kc, VERSIONS);

        Pod pod = build.generateBuilderPod(true, ImagePullPolicy.IFNOTPRESENT, null, null);
        assertThat(pod.getMetadata().getLabels().entrySet().containsAll(buildPodLabels.entrySet()), is(true));
        assertThat(pod.getMetadata().getAnnotations().entrySet().containsAll(buildPodAnnos.entrySet()), is(true));
        assertThat(pod.getSpec().getPriorityClassName(), is("top-priority"));
        assertThat(pod.getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(pod.getSpec().getEnableServiceLinks(), is(false));
        assertThat(pod.getSpec().getContainers().get(0).getEnv().stream().filter(env -> "TEST_ENV_VAR".equals(env.getName())).findFirst().orElseThrow().getValue(), is("testValue"));

        KafkaConnectDockerfile dockerfile = new KafkaConnectDockerfile("my-image:latest", kc.getSpec().getBuild());
        BuildConfig bc = build.generateBuildConfig(dockerfile);
        assertThat(bc.getMetadata().getLabels().entrySet().containsAll(buildConfigLabels.entrySet()), is(true));
        assertThat(bc.getMetadata().getAnnotations().entrySet().containsAll(buildConfigAnnos.entrySet()), is(true));

        // Check Service Account
        ServiceAccount sa = build.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnots.entrySet()), is(true));
    }

    @ParallelTest
    public void testValidKanikoOptions()   {
        List<String> expectedArgs = new ArrayList<>(defaultArgs);
        expectedArgs.add("--reproducible");
        expectedArgs.add("--single-snapshot");
        expectedArgs.add("--log-format=json");

        KafkaConnect kc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(cluster)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers("my-kafka:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                            .withNewPushSecret("my-docker-credentials")
                            .withAdditionalKanikoOptions("--reproducible", "--single-snapshot", "--log-format=json")
                        .endDockerOutput()
                        .withPlugins(new PluginBuilder().withName("my-connector").withArtifacts(jarArtifactWithChecksum).build(),
                                new PluginBuilder().withName("my-connector2").withArtifacts(jarArtifactNoChecksum).build())
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(new Reconciliation("test", kc.getKind(), kc.getMetadata().getNamespace(), kc.getMetadata().getName()), kc, VERSIONS);

        Pod pod = build.generateBuilderPod(true, ImagePullPolicy.IFNOTPRESENT, null, null);
        assertThat(pod.getSpec().getContainers().get(0).getArgs(), is(expectedArgs));
    }

    @ParallelTest
    public void testInvalidKanikoOptions()   {
        KafkaConnect kc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(cluster)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers("my-kafka:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                            .withNewPushSecret("my-docker-credentials")
                            .withAdditionalKanikoOptions("--reproducible", "--reproducible-something", "--build-arg", "--single-snapshot", "--digest-file=/dev/null", "--log-format=json")
                        .endDockerOutput()
                        .withPlugins(new PluginBuilder().withName("my-connector").withArtifacts(jarArtifactWithChecksum).build(),
                                new PluginBuilder().withName("my-connector2").withArtifacts(jarArtifactNoChecksum).build())
                    .endBuild()
                .endSpec()
                .build();

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaConnectBuild.fromCrd(new Reconciliation("test", kc.getKind(), kc.getMetadata().getNamespace(), kc.getMetadata().getName()), kc, VERSIONS));

        assertThat(e.getMessage(), containsString(".spec.build.additionalKanikoOptions contains forbidden options: [--reproducible-something, --build-arg, --digest-file]"));
    }
}
