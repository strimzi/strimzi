/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.rbac.PolicyRule;
import io.fabric8.kubernetes.api.model.rbac.PolicyRuleBuilder;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.EntityUserOperatorSpecBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.TlsSidecarBuilder;
import io.strimzi.api.kafka.model.TlsSidecarLogLevel;
import io.strimzi.api.kafka.model.template.ContainerTemplate;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import org.junit.jupiter.api.AfterAll;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.test.TestUtils.map;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasProperty;

@ParallelSuite
public class EntityOperatorTest {

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    static Map<String, String> volumeMounts(List<VolumeMount> mounts) {
        return mounts.stream().collect(Collectors.toMap(vm -> vm.getName(), vm -> vm.getMountPath()));
    }

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final int tlsHealthDelay = 120;
    private final int tlsHealthTimeout = 30;

    private final EntityUserOperatorSpec entityUserOperatorSpec = new EntityUserOperatorSpecBuilder()
            .build();
    private final EntityTopicOperatorSpec entityTopicOperatorSpec = new EntityTopicOperatorSpecBuilder()
            .build();
    private final TlsSidecar tlsSidecar = new TlsSidecarBuilder()
            .withLivenessProbe(new ProbeBuilder().withInitialDelaySeconds(tlsHealthDelay).withTimeoutSeconds(tlsHealthTimeout).build())
            .withReadinessProbe(new ProbeBuilder().withInitialDelaySeconds(tlsHealthDelay).withTimeoutSeconds(tlsHealthTimeout).build())
            .build();

    private final EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
            .withTlsSidecar(tlsSidecar)
            .withTopicOperator(entityTopicOperatorSpec)
            .withUserOperator(entityUserOperatorSpec)
            .build();

    private final Kafka resource =
            new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                    .editSpec()
                    .withEntityOperator(entityOperatorSpec)
                    .endSpec()
                    .build();

    private final EntityOperator entityOperator = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

    @ParallelTest
    public void testGenerateDeployment() {

        Deployment dep = entityOperator.generateDeployment(true, Collections.EMPTY_MAP, null, null);

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(dep.getMetadata().getName(), is(entityOperator.entityOperatorName(cluster)));
        assertThat(dep.getMetadata().getNamespace(), is(namespace));
        assertThat(dep.getSpec().getReplicas(), is(Integer.valueOf(EntityOperatorSpec.DEFAULT_REPLICAS)));
        assertThat(dep.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(dep.getMetadata().getOwnerReferences().get(0), is(entityOperator.createOwnerReference()));

        assertThat(containers.size(), is(3));
        // just check names of topic and user operators (their containers are tested in the related unit test classes)
        assertThat(containers.get(0).getName(), is(EntityTopicOperator.TOPIC_OPERATOR_CONTAINER_NAME));
        assertThat(containers.get(1).getName(), is(EntityUserOperator.USER_OPERATOR_CONTAINER_NAME));
        // checks on the TLS sidecar container
        Container tlsSidecarContainer = containers.get(2);
        assertThat(tlsSidecarContainer.getImage(), is(image));
        assertThat(AbstractModel.containerEnvVars(tlsSidecarContainer).get(EntityOperator.ENV_VAR_ZOOKEEPER_CONNECT), is(EntityOperator.defaultZookeeperConnect(cluster)));
        assertThat(AbstractModel.containerEnvVars(tlsSidecarContainer).get(ModelUtils.TLS_SIDECAR_LOG_LEVEL), is(TlsSidecarLogLevel.NOTICE.toValue()));
        assertThat(EntityOperatorTest.volumeMounts(tlsSidecarContainer.getVolumeMounts()), is(map(
                        EntityOperator.TLS_SIDECAR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME, AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH,
                        EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT,
                        EntityOperator.TLS_SIDECAR_EO_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT)));
        assertThat(tlsSidecarContainer.getReadinessProbe().getInitialDelaySeconds(), is(Integer.valueOf(tlsHealthDelay)));
        assertThat(tlsSidecarContainer.getReadinessProbe().getTimeoutSeconds(), is(Integer.valueOf(tlsHealthTimeout)));
        assertThat(tlsSidecarContainer.getLivenessProbe().getInitialDelaySeconds(), is(Integer.valueOf(tlsHealthDelay)));
        assertThat(tlsSidecarContainer.getLivenessProbe().getTimeoutSeconds(), is(Integer.valueOf(tlsHealthTimeout)));
    }

    @ParallelTest
    public void testFromCrd() {
        assertThat(entityOperator.namespace, is(namespace));
        assertThat(entityOperator.cluster, is(cluster));
        assertThat(entityOperator.getZookeeperConnect(), is(EntityOperator.defaultZookeeperConnect(cluster)));
    }

    @ParallelTest
    public void testFromCrdNoTopicAndUserOperatorInEntityOperator() {
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder().build();
        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();
        EntityOperator entityOperator = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

        assertThat(entityOperator.getTopicOperator(), is(nullValue()));
        assertThat(entityOperator.getUserOperator(), is(nullValue()));
    }

    @ParallelTest
    public void withAffinityAndTolerations() throws IOException {
        ResourceTester<Kafka, EntityOperator> helper = new ResourceTester<>(Kafka.class, VERSIONS, (kAssembly, versions) -> EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), kAssembly, versions), this.getClass().getSimpleName() + ".withAffinityAndTolerations");
        helper.assertDesiredResource("-DeploymentAffinity.yaml", zc -> zc.generateDeployment(true, Collections.EMPTY_MAP, null, null).getSpec().getTemplate().getSpec().getAffinity());
        helper.assertDesiredResource("-DeploymentTolerations.yaml", zc -> zc.generateDeployment(true, Collections.EMPTY_MAP, null, null).getSpec().getTemplate().getSpec().getTolerations());
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

        Toleration toleration = new TolerationBuilder()
                .withEffect("NoSchedule")
                .withValue("")
                .build();

        Toleration assertToleration = new TolerationBuilder()
                .withEffect("NoSchedule")
                .withValue(null)
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

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                            .withNewEntityOperator()
                                .withTopicOperator(entityTopicOperatorSpec)
                                .withUserOperator(entityUserOperatorSpec)
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
                                        .withSchedulerName("my-scheduler")
                                        .withTolerations(singletonList(toleration))
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
                            .endEntityOperator()
                        .endSpec()
                        .build();
        EntityOperator entityOperator = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

        // Check Deployment
        Deployment dep = entityOperator.generateDeployment(true, Collections.EMPTY_MAP, null, null);
        assertThat(dep.getMetadata().getLabels().entrySet().containsAll(expectedDepLabels.entrySet()), is(true));
        assertThat(dep.getMetadata().getAnnotations().entrySet().containsAll(depAnots.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));

        // Check Pods
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(dep.getSpec().getTemplate().getSpec().getTopologySpreadConstraints(), containsInAnyOrder(tsc1, tsc2));
        assertThat(dep.getSpec().getTemplate().getSpec().getEnableServiceLinks(), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getTolerations(), is(singletonList(assertToleration)));

        // Check Service Account
        ServiceAccount sa = entityOperator.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnots.entrySet()), is(true));
    }

    @ParallelTest
    public void testGracePeriod() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                    .withTopicOperator(entityTopicOperatorSpec)
                    .withUserOperator(entityUserOperatorSpec)
                    .withNewTemplate()
                        .withNewPod()
                            .withTerminationGracePeriodSeconds(123)
                        .endPod()
                    .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(Long.valueOf(123)));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(2).getLifecycle(), is(notNullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(2).getLifecycle().getPreStop().getExec().getCommand().contains("/opt/stunnel/entity_operator_stunnel_pre_stop.sh"), is(true));
    }

    @ParallelTest
    public void testDefaultGracePeriod() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds(), is(Long.valueOf(30)));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(2).getLifecycle(), is(notNullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(2).getLifecycle().getPreStop().getExec().getCommand().contains("/opt/stunnel/entity_operator_stunnel_pre_stop.sh"), is(true));
    }

    @ParallelTest
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                    .withTopicOperator(entityTopicOperatorSpec)
                    .withUserOperator(entityUserOperatorSpec)
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret1, secret2)
                        .endPod()
                    .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, null, null);
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

        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, null, secrets);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(2));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testImagePullSecretsFromBoth() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                .withNewEntityOperator()
                .withTopicOperator(entityTopicOperatorSpec)
                .withUserOperator(entityUserOperatorSpec)
                .withNewTemplate()
                .withNewPod()
                .withImagePullSecrets(secret2)
                .endPod()
                .endTemplate()
                .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, null, singletonList(secret1));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size(), is(1));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2), is(true));
    }

    @ParallelTest
    public void testDefaultImagePullSecrets() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getImagePullSecrets(), is(nullValue()));
    }

    @ParallelTest
    public void testSecurityContext() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                        .withNewTemplate()
                            .withNewPod()
                                .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                            .endPod()
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(notNullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup(), is(Long.valueOf(123)));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup(), is(Long.valueOf(456)));
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser(), is(Long.valueOf(789)));
    }

    @ParallelTest
    public void testDefaultSecurityContext() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
    }

    /**
     * Verify the lookup order is:<ul>
     * <li>Kafka.spec.entityOperator.tlsSidecar.image</li>
     * <li>Kafka.spec.kafka.image</li>
     * <li>image for default version of Kafka</li></ul>
     */
    @ParallelTest
    public void testStunnelImage() {
        Kafka kafka = new KafkaBuilder(resource)
                .editSpec()
                    .editEntityOperator()
                        .editOrNewTlsSidecar()
                            .withImage("foo1")
                        .endTlsSidecar()
                    .endEntityOperator()
                    .editKafka()
                        .withImage("foo2")
                    .endKafka()
                .endSpec()
                .build();
        assertThat(EntityOperator.fromCrd(new Reconciliation("test", kafka.getKind(), kafka.getMetadata().getNamespace(), kafka.getMetadata().getName()), kafka, VERSIONS).getContainers(ImagePullPolicy.ALWAYS).get(2).getImage(), is("foo1"));

        kafka = new KafkaBuilder(resource)
                .editSpec()
                    .editEntityOperator()
                        .editOrNewTlsSidecar()
                            .withImage(null)
                        .endTlsSidecar()
                    .endEntityOperator()
                    .editKafka()
                        .withImage("foo2")
                    .endKafka()
                .endSpec()
                .build();
        assertThat(EntityOperator.fromCrd(new Reconciliation("test", kafka.getKind(), kafka.getMetadata().getNamespace(), kafka.getMetadata().getName()), kafka, VERSIONS).getContainers(ImagePullPolicy.ALWAYS).get(2).getImage(), is("foo2"));

        kafka = new KafkaBuilder(resource)
                .editSpec()
                    .editEntityOperator()
                        .editOrNewTlsSidecar()
                            .withImage(null)
                        .endTlsSidecar()
                    .endEntityOperator()
                    .editKafka()
                        .withVersion(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION)
                        .withImage(null)
                    .endKafka()
                .endSpec()
            .build();
        assertThat(EntityOperator.fromCrd(new Reconciliation("test", kafka.getKind(), kafka.getMetadata().getNamespace(), kafka.getMetadata().getName()), kafka, VERSIONS).getContainers(ImagePullPolicy.ALWAYS).get(2).getImage(), is(KafkaVersionTestUtils.DEFAULT_KAFKA_IMAGE));

        kafka = new KafkaBuilder(resource)
                .editSpec()
                    .editEntityOperator()
                        .editOrNewTlsSidecar()
                            .withImage(null)
                        .endTlsSidecar()
                    .endEntityOperator()
                    .editKafka()
                        .withVersion(KafkaVersionTestUtils.LATEST_KAFKA_VERSION)
                        .withImage(null)
                    .endKafka()
                .endSpec()
            .build();
        assertThat(EntityOperator.fromCrd(new Reconciliation("test", kafka.getKind(), kafka.getMetadata().getNamespace(), kafka.getMetadata().getName()), kafka, VERSIONS).getContainers(ImagePullPolicy.ALWAYS).get(2).getImage(), is(KafkaVersionTestUtils.DEFAULT_KAFKA_IMAGE));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, ImagePullPolicy.ALWAYS, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(1).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(2).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        dep = eo.generateDeployment(true, Collections.EMPTY_MAP, ImagePullPolicy.IFNOTPRESENT, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(1).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(2).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
    }

    @AfterAll
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    @ParallelTest
    public void testTopicOperatorContainerEnvVars() {

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

        ContainerTemplate topicOperatorContainer = new ContainerTemplate();
        topicOperatorContainer.setEnv(testEnvs);

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                        .withNewTemplate()
                        .withTopicOperatorContainer(topicOperatorContainer)
                        .endTemplate()
                        .endEntityOperator()
                        .endSpec()
                        .build();

        List<EnvVar> containerEnvVars = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS).getTopicOperator().getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                containerEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                containerEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));


    }

    @ParallelTest
    public void testTopicOperatorContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = EntityTopicOperator.ENV_VAR_RESOURCE_LABELS;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = EntityTopicOperator.ENV_VAR_KAFKA_BOOTSTRAP_SERVERS;
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate topicOperatorContainer = new ContainerTemplate();
        topicOperatorContainer.setEnv(testEnvs);

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                        .withNewTemplate()
                        .withTopicOperatorContainer(topicOperatorContainer)
                        .endTemplate()
                        .endEntityOperator()
                        .endSpec()
                        .build();

        List<EnvVar> containerEnvVars = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS).getTopicOperator().getEnvVars();

        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                containerEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));
        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvTwoKey,
                containerEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(false));

    }

    @ParallelTest
    public void testUserOperatorContainerEnvVars() {

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

        ContainerTemplate userOperatorContainer = new ContainerTemplate();
        userOperatorContainer.setEnv(testEnvs);

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                        .withNewTemplate()
                        .withUserOperatorContainer(userOperatorContainer)
                        .endTemplate()
                        .endEntityOperator()
                        .endSpec()
                        .build();

        List<EnvVar> containerEnvVars = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS).getUserOperator().getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                containerEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                containerEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));

    }

    @ParallelTest
    public void testUserOperatorContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = EntityUserOperator.ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = EntityUserOperator.ENV_VAR_KAFKA_BOOTSTRAP_SERVERS;
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate userOperatorContainer = new ContainerTemplate();
        userOperatorContainer.setEnv(testEnvs);

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                        .withNewTemplate()
                        .withUserOperatorContainer(userOperatorContainer)
                        .endTemplate()
                        .endEntityOperator()
                        .endSpec()
                        .build();

        List<EnvVar> containerEnvVars = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS).getUserOperator().getEnvVars();

        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                containerEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));
        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvTwoKey,
                containerEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(false));
    }

    @ParallelTest
    public void testTlsSideCarContainerEnvVars() {

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
        ContainerTemplate tlsContainer = new ContainerTemplate();
        tlsContainer.setEnv(testEnvs);

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                        .withNewTemplate()
                        .withTlsSidecarContainer(tlsContainer)
                        .endTemplate()
                        .endEntityOperator()
                        .endSpec()
                        .build();


        List<EnvVar> containerEnvVars = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS).getTlsSidecarEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                containerEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                containerEnvVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));

    }

    @ParallelTest
    public void testTlsSidecarContainerEnvVarsConflict() {

        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = EntityOperator.ENV_VAR_ZOOKEEPER_CONNECT;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        ContainerTemplate tlsContainer = new ContainerTemplate();
        tlsContainer.setEnv(testEnvs);

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                            .withNewEntityOperator()
                                .withTopicOperator(entityTopicOperatorSpec)
                                .withUserOperator(entityUserOperatorSpec)
                                .withNewTemplate()
                                    .withTlsSidecarContainer(tlsContainer)
                                .endTemplate()
                            .endEntityOperator()
                        .endSpec()
                        .build();


        List<EnvVar> containerEnvVars = EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS).getTlsSidecarEnvVars();

        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                containerEnvVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));
    }

    @ParallelTest
    public void testUserOperatorContainerSecurityContext() {

        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addNewDrop("ALL")
                .endCapabilities()
                .build();

        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .editOrNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                        .editOrNewTemplate()
                            .editOrNewUserOperatorContainer()
                                .withSecurityContext(securityContext)
                            .endUserOperatorContainer()
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo =  EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);
        Deployment deployment = eo.generateDeployment(false, null, null, null);

        assertThat(deployment.getSpec().getTemplate().getSpec().getContainers(),
                hasItem(allOf(
                        hasProperty("name", equalTo(EntityUserOperator.USER_OPERATOR_CONTAINER_NAME)),
                        hasProperty("securityContext", equalTo(securityContext))
                )));
    }

    @ParallelTest
    public void testTopicOperatorContainerSecurityContext() {

        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                .addNewDrop("ALL")
                .endCapabilities()
                .build();

        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .editOrNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                        .editOrNewTemplate()
                            .editOrNewTopicOperatorContainer()
                                .withSecurityContext(securityContext)
                            .endTopicOperatorContainer()
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo =  EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);
        Deployment deployment = eo.generateDeployment(false, null, null, null);

        assertThat(deployment.getSpec().getTemplate().getSpec().getContainers(),
                hasItem(allOf(
                        hasProperty("name", equalTo(EntityTopicOperator.TOPIC_OPERATOR_CONTAINER_NAME)),
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

        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .editOrNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                        .editOrNewTemplate()
                            .editOrNewTlsSidecarContainer()
                                .withSecurityContext(securityContext)
                            .endTlsSidecarContainer()
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo =  EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);
        Deployment deployment = eo.generateDeployment(false, null, null, null);

        assertThat(deployment.getSpec().getTemplate().getSpec().getContainers(),
                hasItem(allOf(
                        hasProperty("name", equalTo(EntityOperator.TLS_SIDECAR_NAME)),
                        hasProperty("securityContext", equalTo(securityContext))
                )));
    }

    @ParallelTest
    public void testRole() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .editOrNewEntityOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo =  EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);
        Role role = eo.generateRole(namespace, namespace);

        assertThat(role.getMetadata().getName(), is("foo-entity-operator"));
        assertThat(role.getMetadata().getNamespace(), is(namespace));

        List<PolicyRule> rules = new ArrayList<>();
        rules.add(new PolicyRuleBuilder()
                .addToResources("kafkatopics", "kafkatopics/status", "kafkausers", "kafkausers/status")
                .addToVerbs("get", "list", "watch", "create", "patch", "update", "delete")
                .addToApiGroups(Constants.RESOURCE_GROUP_NAME)
                .build());
        rules.add(new PolicyRuleBuilder()
                .addToResources("events")
                .addToVerbs("create")
                .addToApiGroups("")
                .build());
        rules.add(new PolicyRuleBuilder()
                .addToResources("secrets")
                .addToVerbs("get", "list", "watch", "create", "delete", "patch", "update")
                .addToApiGroups("")
                .build());
        assertThat(role.getRules(), is(rules));
    }

    @ParallelTest
    public void testRoleInDifferentNamespace() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                .editOrNewEntityOperator()
                .endEntityOperator()
                .endSpec()
                .build();

        EntityOperator eo =  EntityOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, VERSIONS);
        Role role = eo.generateRole(namespace, namespace);

        assertThat(role.getMetadata().getOwnerReferences().get(0), is(entityOperator.createOwnerReference()));

        role = eo.generateRole(namespace, "some-other-namespace");
        assertThat(role.getMetadata().getOwnerReferences().size(), is(0));
    }
}
