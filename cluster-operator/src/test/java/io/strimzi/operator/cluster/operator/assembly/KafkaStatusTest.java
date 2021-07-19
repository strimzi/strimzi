/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.api.model.NodeBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.NodeAddressType;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.status.ListenerAddress;
import io.strimzi.api.kafka.model.status.ListenerAddressBuilder;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.api.kafka.model.status.ListenerStatusBuilder;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.NodeOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:ClassDataAbstractionCoupling"})
@ExtendWith(VertxExtension.class)
public class KafkaStatusTest {
    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_18;
    private final MockCertManager certManager = new MockCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");
    private final ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String namespace = "testns";
    private final String clusterName = "testkafka";
    protected static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    public Kafka getKafkaCrd() throws ParseException {
        return new KafkaBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespace)
                    .withGeneration(2L)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build())
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .withNewStatus()
                    .withObservedGeneration(1L)
                    .withConditions(new ConditionBuilder()
                            .withNewLastTransitionTime(StatusUtils.iso8601(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2011-01-01 00:00:00")))
                            .withNewType("NotReady")
                            .withNewStatus("True")
                            .build())
                .endStatus()
                .build();
    }

    @Test
    public void testStatusAfterSuccessfulReconciliationWithPreviousFailure(VertxTestContext context) throws ParseException {
        Kafka kafka = getKafkaCrd();
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator mockKafkaOps = supplier.kafkaOperator;

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(getKafkaCrd()));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockWorkingKafkaAssemblyOperator kao = new MockWorkingKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(true));

            assertThat(kafkaCaptor.getValue(), is(notNullValue()));
            assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertThat(status.getListeners().size(), is(2));
            assertThat(status.getListeners().get(0).getType(), is("plain"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getHost(), is("my-service.my-namespace.svc"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getPort(), is(Integer.valueOf(9092)));
            assertThat(status.getListeners().get(0).getBootstrapServers(), is("my-service.my-namespace.svc:9092"));
            assertThat(status.getListeners().get(1).getType(), is("external"));
            assertThat(status.getListeners().get(1).getAddresses().get(0).getHost(), is("my-route-address.domain.tld"));
            assertThat(status.getListeners().get(1).getAddresses().get(0).getPort(), is(Integer.valueOf(443)));
            assertThat(status.getListeners().get(1).getBootstrapServers(), is("my-route-address.domain.tld:443"));

            assertThat(status.getConditions().size(), is(1));
            assertThat(status.getConditions().get(0).getType(), is("Ready"));
            assertThat(status.getConditions().get(0).getStatus(), is("True"));

            assertThat(status.getObservedGeneration(), is(2L));

            async.flag();
        });
    }

    @Test
    public void testPauseReconciliationsStatus(VertxTestContext context) throws ParseException {
        Kafka kafka = getKafkaCrd();
        kafka.getMetadata().setAnnotations(singletonMap("strimzi.io/pause-reconciliation", Boolean.toString(true)));
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator mockKafkaOps = supplier.kafkaOperator;

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(getKafkaCrd()));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockWorkingKafkaAssemblyOperator kao = new MockWorkingKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(true));

            assertThat(kafkaCaptor.getValue(), is(notNullValue()));
            assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertThat(status.getConditions().size(), is(1));
            assertThat(status.getConditions().get(0).getStatus(), is("True"));
            assertThat(status.getConditions().get(0).getType(), is("ReconciliationPaused"));
            assertThat(status.getObservedGeneration(), is(1L));
            async.flag();
        });
    }

    @Test
    public void testStatusAfterSuccessfulReconciliationWithPreviousSuccess(VertxTestContext context) throws ParseException {
        Kafka kafka = getKafkaCrd();
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator mockKafkaOps = supplier.kafkaOperator;

        Kafka readyKafka = new KafkaBuilder(kafka)
                .editStatus()
                    .withObservedGeneration(2L)
                    .editCondition(0)
                        .withType("Ready")
                    .endCondition()
                    .withListeners(new ListenerStatusBuilder()
                            .withNewType("plain")
                            .withAddresses(new ListenerAddressBuilder()
                                    .withHost("my-service.my-namespace.svc")
                                    .withPort(9092)
                                    .build())
                            .build(),
                            new ListenerStatusBuilder()
                                    .withNewType("external")
                                    .withAddresses(new ListenerAddressBuilder()
                                            .withHost("my-route-address.domain.tld")
                                            .withPort(443)
                                            .build())
                                    .build())
                .endStatus()
                .build();

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(readyKafka));

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockWorkingKafkaAssemblyOperator kao = new MockWorkingKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName), kafka).onComplete(res -> {
            assertThat(res.succeeded(), is(true));
            // The status should not change => we test that updateStatusAsync was not called
            assertThat(kafkaCaptor.getAllValues().size(), is(0));

            async.flag();
        });
    }

    @Test
    public void testStatusAfterFailedReconciliationWithPreviousFailure(VertxTestContext context) throws ParseException {
        testStatusAfterFailedReconciliationWithPreviousFailure(context, new RuntimeException("Something went wrong"));
    }

    @Test
    public void testStatusAfterFailedReconciliationWithPreviousFailure_NPE(VertxTestContext context) throws ParseException {
        testStatusAfterFailedReconciliationWithPreviousFailure(context, new NullPointerException());
    }

    public void testStatusAfterFailedReconciliationWithPreviousFailure(VertxTestContext context, Throwable exception) throws ParseException {
        Kafka kafka = getKafkaCrd();
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator mockKafkaOps = supplier.kafkaOperator;

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(getKafkaCrd()));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockFailingKafkaAssemblyOperator kao = new MockFailingKafkaAssemblyOperator(
                exception,
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(false));

            assertThat(kafkaCaptor.getValue(), is(notNullValue()));
            assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertThat(status.getListeners().size(), is(1));
            assertThat(status.getListeners().get(0).getType(), is("plain"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getHost(), is("my-service.my-namespace.svc"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getPort(), is(Integer.valueOf(9092)));
            assertThat(status.getListeners().get(0).getBootstrapServers(), is("my-service.my-namespace.svc:9092"));

            assertThat(status.getConditions().size(), is(1));
            assertThat(status.getConditions().get(0).getType(), is("NotReady"));
            assertThat(status.getConditions().get(0).getStatus(), is("True"));
            assertThat(status.getConditions().get(0).getReason(), is(exception.getClass().getSimpleName()));
            assertThat(status.getConditions().get(0).getMessage(), is(exception.getMessage()));

            assertThat(status.getObservedGeneration(), is(2L));

            async.flag();
        });
    }

    @Test
    public void testStatusAfterFailedReconciliationWithPreviousSuccess(VertxTestContext context) throws ParseException {
        Kafka kafka = getKafkaCrd();
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator mockKafkaOps = supplier.kafkaOperator;

        Kafka readyKafka = new KafkaBuilder(kafka)
                .editStatus()
                    .withObservedGeneration(1L)
                    .editCondition(0)
                        .withType("Ready")
                    .endCondition()
                    .withListeners(new ListenerStatusBuilder()
                                    .withNewType("plain")
                                    .withAddresses(new ListenerAddressBuilder()
                                            .withHost("my-service.my-namespace.svc")
                                            .withPort(9092)
                                            .build())
                                    .build(),
                            new ListenerStatusBuilder()
                                    .withNewType("external")
                                    .withAddresses(new ListenerAddressBuilder()
                                            .withHost("my-route-address.domain.tld")
                                            .withPort(443)
                                            .build())
                                    .build())
                .endStatus()
                .build();

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(readyKafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(readyKafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockFailingKafkaAssemblyOperator kao = new MockFailingKafkaAssemblyOperator(
                new RuntimeException("Something went wrong"),
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(false));

            assertThat(kafkaCaptor.getValue(), is(notNullValue()));
            assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertThat(status.getListeners().size(), is(1));
            assertThat(status.getListeners().get(0).getType(), is("plain"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getHost(), is("my-service.my-namespace.svc"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getPort(), is(Integer.valueOf(9092)));
            assertThat(status.getListeners().get(0).getBootstrapServers(), is("my-service.my-namespace.svc:9092"));

            assertThat(status.getConditions().size(), is(1));
            assertThat(status.getConditions().get(0).getType(), is("NotReady"));
            assertThat(status.getConditions().get(0).getStatus(), is("True"));
            assertThat(status.getConditions().get(0).getReason(), is("RuntimeException"));
            assertThat(status.getConditions().get(0).getMessage(), is("Something went wrong"));

            assertThat(status.getObservedGeneration(), is(2L));

            async.flag();
        });
    }

    private List<Node> getClusterNodes()    {
        Node node0 = new NodeBuilder()
                .withNewMetadata()
                    .withName("node-0")
                .endMetadata()
                .withNewStatus()
                    .withAddresses(new NodeAddress("50.35.18.119", "ExternalIP"),
                            new NodeAddress("node-0.my-kube", "InternalDNS"),
                            new NodeAddress("10.0.0.1", "InternalIP"),
                            new NodeAddress("nocde-0", "Hostname"))
                .endStatus()
                .build();

        Node node1 = new NodeBuilder()
                .withNewMetadata()
                    .withName("node-1")
                .endMetadata()
                .withNewStatus()
                    .withAddresses(new NodeAddress("55.36.78.115", "ExternalIP"),
                            new NodeAddress("node-1.my-kube", "InternalDNS"),
                            new NodeAddress("10.0.0.25", "InternalIP"),
                            new NodeAddress("node-1", "Hostname"))
                .endStatus()
                .build();

        Node node2 = new NodeBuilder()
                .withNewMetadata()
                    .withName("node-2")
                .endMetadata()
                .withNewStatus()
                    .withAddresses(new NodeAddress("35.15.152.9", "ExternalIP"),
                            new NodeAddress("node-2.my-kube", "InternalDNS"),
                            new NodeAddress("10.0.0.16", "InternalIP"),
                            new NodeAddress("node-2", "Hostname"))
                .endStatus()
                .build();

        Node node3 = new NodeBuilder()
                .withNewMetadata()
                    .withName("node-3")
                .endMetadata()
                .withNewStatus()
                    .withAddresses(new NodeAddress("5.124.16.8", "ExternalIP"),
                            new NodeAddress("node-3.my-kube", "InternalDNS"),
                            new NodeAddress("10.0.0.13", "InternalIP"),
                            new NodeAddress("node-3", "Hostname"))
                .endStatus()
                .build();

        List<Node> nodes = new ArrayList<>();
        nodes.add(node0);
        nodes.add(node1);
        nodes.add(node2);
        nodes.add(node3);

        return nodes;
    }

    @Test
    public void testKafkaListenerNodePortAddressInStatus(VertxTestContext context) throws ParseException {
        Kafka kafka = new KafkaBuilder(getKafkaCrd())
                .editOrNewSpec()
                    .editOrNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the KafkaSetOperator
        KafkaSetOperator mockKafkaSetOps = supplier.kafkaSetOperations;
        when(mockKafkaSetOps.getAsync(eq(namespace), eq(KafkaCluster.kafkaClusterName(clusterName)))).thenReturn(Future.succeededFuture(kafkaCluster.generateStatefulSet(false, null, null)));

        // Mock the ConfigMapOperator
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafkaCluster.generateMetricsAndLogConfigMap(new MetricsAndLogging(null, null))));

        // Mock Pods Operator
        Pod pod0 = new PodBuilder()
                .withNewMetadata()
                    .withName(clusterName + "-kafka-" + 0)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.1")
                .endStatus()
                .build();

        Pod pod1 = new PodBuilder()
                .withNewMetadata()
                    .withName(clusterName + "-kafka-" + 1)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.25")
                .endStatus()
                .build();

        Pod pod2 = new PodBuilder()
                .withNewMetadata()
                    .withName(clusterName + "-kafka-" + 2)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.13")
                .endStatus()
                .build();

        List<Pod> pods = new ArrayList<>();
        pods.add(pod0);
        pods.add(pod1);
        pods.add(pod2);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(namespace), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        // Mock Node operator
        NodeOperator mockNodeOps = supplier.nodeOperator;
        when(mockNodeOps.listAsync(any(Labels.class))).thenReturn(Future.succeededFuture(getClusterNodes()));

        MockNodePortStatusKafkaAssemblyOperator kao = new MockNodePortStatusKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(true));

            assertThat(kafkaCaptor.getValue(), is(notNullValue()));
            assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertThat(status.getListeners().size(), is(1));
            assertThat(status.getListeners().get(0).getType(), is("external"));

            List<ListenerAddress> addresses = status.getListeners().get(0).getAddresses();
            assertThat(addresses.size(), is(3));

            List<ListenerAddress> expected = new ArrayList<>();
            expected.add(new ListenerAddressBuilder().withHost("50.35.18.119").withPort(31234).build());
            expected.add(new ListenerAddressBuilder().withHost("55.36.78.115").withPort(31234).build());
            expected.add(new ListenerAddressBuilder().withHost("5.124.16.8").withPort(31234).build());

            async.flag();
        });
    }

    @Test
    public void testKafkaListenerNodePortAddressInStatusWithOverrides(VertxTestContext context) throws ParseException {
        GenericKafkaListenerConfigurationBroker broker0 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .withAdvertisedHost("my-address-0")
                .build();

        GenericKafkaListenerConfigurationBroker broker1 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(1)
                .withAdvertisedHost("my-address-1")
                .build();

        Kafka kafka = new KafkaBuilder(getKafkaCrd())
                .editOrNewSpec()
                    .editOrNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withBrokers(broker0, broker1)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the KafkaSetOperator
        KafkaSetOperator mockKafkaSetOps = supplier.kafkaSetOperations;
        when(mockKafkaSetOps.getAsync(eq(namespace), eq(KafkaCluster.kafkaClusterName(clusterName)))).thenReturn(Future.succeededFuture(kafkaCluster.generateStatefulSet(false, null, null)));

        // Mock the ConfigMapOperator
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafkaCluster.generateMetricsAndLogConfigMap(new MetricsAndLogging(null, null))));

        // Mock Pods Operator
        Pod pod0 = new PodBuilder()
                .withNewMetadata()
                    .withName(clusterName + "-kafka-" + 0)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.1")
                .endStatus()
                .build();

        Pod pod1 = new PodBuilder()
                .withNewMetadata()
                    .withName(clusterName + "-kafka-" + 1)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.25")
                .endStatus()
                .build();

        Pod pod2 = new PodBuilder()
                .withNewMetadata()
                    .withName(clusterName + "-kafka-" + 2)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.13")
                .endStatus()
                .build();

        List<Pod> pods = new ArrayList<>();
        pods.add(pod0);
        pods.add(pod1);
        pods.add(pod2);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(namespace), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        // Mock Node operator
        NodeOperator mockNodeOps = supplier.nodeOperator;
        when(mockNodeOps.listAsync(any(Labels.class))).thenReturn(Future.succeededFuture(getClusterNodes()));

        MockNodePortStatusKafkaAssemblyOperator kao = new MockNodePortStatusKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(true));

            assertThat(kafkaCaptor.getValue(), is(notNullValue()));
            assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertThat(status.getListeners().size(), is(1));
            assertThat(status.getListeners().get(0).getType(), is("external"));

            List<ListenerAddress> addresses = status.getListeners().get(0).getAddresses();
            assertThat(addresses.size(), is(3));

            List<ListenerAddress> expected = new ArrayList<>();
            expected.add(new ListenerAddressBuilder().withHost("my-address-0").withPort(31234).build());
            expected.add(new ListenerAddressBuilder().withHost("my-address-1").withPort(31234).build());
            expected.add(new ListenerAddressBuilder().withHost("5.124.16.8").withPort(31234).build());

            async.flag();
        });
    }

    @Test
    public void testKafkaListenerNodePortAddressWithPreferred(VertxTestContext context) throws ParseException {
        Kafka kafka = new KafkaBuilder(getKafkaCrd())
                .editOrNewSpec()
                    .editOrNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withPreferredNodePortAddressType(NodeAddressType.INTERNAL_DNS)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the KafkaSetOperator
        KafkaSetOperator mockKafkaSetOps = supplier.kafkaSetOperations;
        when(mockKafkaSetOps.getAsync(eq(namespace), eq(KafkaCluster.kafkaClusterName(clusterName)))).thenReturn(Future.succeededFuture(kafkaCluster.generateStatefulSet(false, null, null)));

        // Mock the ConfigMapOperator
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafkaCluster.generateMetricsAndLogConfigMap(new MetricsAndLogging(null, null))));

        // Mock Pods Operator
        Pod pod0 = new PodBuilder()
                .withNewMetadata()
                    .withName(clusterName + "-kafka-" + 0)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.1")
                .endStatus()
                .build();

        Pod pod1 = new PodBuilder()
                .withNewMetadata()
                    .withName(clusterName + "-kafka-" + 1)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.25")
                .endStatus()
                .build();

        Pod pod2 = new PodBuilder()
                .withNewMetadata()
                    .withName(clusterName + "-kafka-" + 2)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.13")
                .endStatus()
                .build();

        List<Pod> pods = new ArrayList<>();
        pods.add(pod0);
        pods.add(pod1);
        pods.add(pod2);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(namespace), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        // Mock Node operator
        NodeOperator mockNodeOps = supplier.nodeOperator;
        when(mockNodeOps.listAsync(any(Labels.class))).thenReturn(Future.succeededFuture(getClusterNodes()));

        MockNodePortStatusKafkaAssemblyOperator kao = new MockNodePortStatusKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(true));

            assertThat(kafkaCaptor.getValue(), is(notNullValue()));
            assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertThat(status.getListeners().size(), is(1));
            assertThat(status.getListeners().get(0).getType(), is("external"));

            List<ListenerAddress> addresses = status.getListeners().get(0).getAddresses();
            assertThat(addresses.size(), is(3));

            List<ListenerAddress> expected = new ArrayList<>();
            expected.add(new ListenerAddressBuilder().withHost("node-0.my-kube").withPort(31234).build());
            expected.add(new ListenerAddressBuilder().withHost("node-1.my-kube").withPort(31234).build());
            expected.add(new ListenerAddressBuilder().withHost("node-3.my-kube").withPort(31234).build());

            async.flag();
        });
    }

    @Test
    public void testKafkaListenerNodePortAddressSameNode(VertxTestContext context) throws ParseException {
        Kafka kafka = new KafkaBuilder(getKafkaCrd())
                .editOrNewSpec()
                    .editOrNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the KafkaSetOperator
        KafkaSetOperator mockKafkaSetOps = supplier.kafkaSetOperations;
        when(mockKafkaSetOps.getAsync(eq(namespace), eq(KafkaCluster.kafkaClusterName(clusterName)))).thenReturn(Future.succeededFuture(kafkaCluster.generateStatefulSet(false, null, null)));

        // Mock the ConfigMapOperator
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafkaCluster.generateMetricsAndLogConfigMap(new MetricsAndLogging(null, null))));

        // Mock Pods Operator
        Pod pod0 = new PodBuilder()
                .withNewMetadata()
                    .withName(clusterName + "-kafka-" + 0)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.1")
                .endStatus()
                .build();

        Pod pod1 = new PodBuilder()
                .withNewMetadata()
                    .withName(clusterName + "-kafka-" + 1)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.1")
                .endStatus()
                .build();

        Pod pod2 = new PodBuilder()
                .withNewMetadata()
                    .withName(clusterName + "-kafka-" + 2)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.1")
                .endStatus()
                .build();

        List<Pod> pods = new ArrayList<>();
        pods.add(pod0);
        pods.add(pod1);
        pods.add(pod2);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(namespace), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        // Mock Node operator
        NodeOperator mockNodeOps = supplier.nodeOperator;
        when(mockNodeOps.listAsync(any(Labels.class))).thenReturn(Future.succeededFuture(getClusterNodes()));

        MockNodePortStatusKafkaAssemblyOperator kao = new MockNodePortStatusKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(true));

            assertThat(kafkaCaptor.getValue(), is(notNullValue()));
            assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertThat(status.getListeners().size(), is(1));
            assertThat(status.getListeners().get(0).getType(), is("external"));

            List<ListenerAddress> addresses = status.getListeners().get(0).getAddresses();
            assertThat(addresses.size(), is(1));

            List<ListenerAddress> expected = new ArrayList<>();
            expected.add(new ListenerAddressBuilder().withHost("50.35.18.119").withPort(31234).build());

            async.flag();
        });
    }

    @Test
    public void testKafkaListenerNodePortAddressMissingNodes(VertxTestContext context) throws ParseException {
        Kafka kafka = new KafkaBuilder(getKafkaCrd())
                .editOrNewSpec()
                    .editOrNewKafka()
                        .withReplicas(1)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the KafkaSetOperator
        KafkaSetOperator mockKafkaSetOps = supplier.kafkaSetOperations;
        when(mockKafkaSetOps.getAsync(eq(namespace), eq(KafkaCluster.kafkaClusterName(clusterName)))).thenReturn(Future.succeededFuture(kafkaCluster.generateStatefulSet(false, null, null)));

        // Mock the ConfigMapOperator
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafkaCluster.generateMetricsAndLogConfigMap(new MetricsAndLogging(null, null))));

        // Mock Pods Operator
        Pod pod0 = new PodBuilder()
                .withNewMetadata()
                    .withName(clusterName + "-kafka-" + 0)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.5")
                .endStatus()
                .build();

        List<Pod> pods = new ArrayList<>();
        pods.add(pod0);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(namespace), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        // Mock Node operator
        NodeOperator mockNodeOps = supplier.nodeOperator;
        when(mockNodeOps.listAsync(any(Labels.class))).thenReturn(Future.succeededFuture(getClusterNodes()));

        MockNodePortStatusKafkaAssemblyOperator kao = new MockNodePortStatusKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(true));

            assertThat(kafkaCaptor.getValue(), is(notNullValue()));
            assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertThat(status.getListeners().size(), is(1));
            assertThat(status.getListeners().get(0).getType(), is("external"));

            assertThat(status.getListeners().get(0).getAddresses(), is(emptyList()));
            assertThat(status.getListeners().get(0).getBootstrapServers(), is(nullValue()));

            async.flag();
        });
    }

    @Test
    public void testInitialStatusOnNewResource() throws ParseException {
        Kafka kafka = getKafkaCrd();
        kafka.setStatus(null);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator mockKafkaOps = supplier.kafkaOperator;

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockInitialStatusKafkaAssemblyOperator kao = new MockInitialStatusKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(true));

            assertThat(kafkaCaptor.getAllValues().size(), is(2));
            assertThat(kafkaCaptor.getAllValues().get(0).getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getAllValues().get(0).getStatus();

            assertThat(status.getListeners(), is(nullValue()));

            assertThat(status.getConditions().size(), is(1));
            assertThat(status.getConditions().get(0).getType(), is("NotReady"));
            assertThat(status.getConditions().get(0).getStatus(), is("True"));
            assertThat(status.getConditions().get(0).getReason(), is("Creating"));
            assertThat(status.getConditions().get(0).getMessage(), is("Kafka cluster is being deployed"));
        });
    }

    @Test
    public void testInitialStatusOnOldResource() throws ParseException {
        Kafka kafka = getKafkaCrd();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator mockKafkaOps = supplier.kafkaOperator;

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockInitialStatusKafkaAssemblyOperator kao = new MockInitialStatusKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(true));

            assertThat(kafkaCaptor.getAllValues().size(), is(1));
        });
    }

    @Test
    public void testKafkaClusterIdInStatus(VertxTestContext context) throws ParseException {
        Kafka kafka = new KafkaBuilder(getKafkaCrd()).build();
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());
        
        // Mock the KafkaSecretOperator
        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(namespace), eq(KafkaResources.clusterCaCertificateSecretName(clusterName)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(namespace), eq(ClusterOperator.secretName(clusterName)))).thenReturn(Future.succeededFuture(secret));
        
        MockClusterIdStatusKafkaAssemblyOperator kao = new MockClusterIdStatusKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(true));

            assertThat(kafkaCaptor.getValue(), is(notNullValue()));
            assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertThat(status.getClusterId(), is(notNullValue()));

            async.flag();
        });
    }

    @Test
    public void testModelWarnings(VertxTestContext context) throws ParseException {
        Kafka kafka = getKafkaCrd();
        Kafka oldKafka = new KafkaBuilder(getKafkaCrd())
                .editOrNewSpec()
                    .editOrNewKafka()
                        .withNewPersistentClaimStorage()
                            .withSize("100Gi")
                        .endPersistentClaimStorage()
                    .endKafka()
                .endSpec()
                .build();
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, oldKafka, VERSIONS);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the KafkaSetOperator
        KafkaSetOperator mockKafkaSetOps = supplier.kafkaSetOperations;
        when(mockKafkaSetOps.getAsync(eq(namespace), eq(KafkaCluster.kafkaClusterName(clusterName)))).thenReturn(Future.succeededFuture(kafkaCluster.generateStatefulSet(false, null, null)));

        // Mock the ConfigMapOperator
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafkaCluster.generateMetricsAndLogConfigMap(new MetricsAndLogging(null, null))));

        // Mock the PodOperator
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(namespace), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        MockModelWarningsStatusKafkaAssemblyOperator kao = new MockModelWarningsStatusKafkaAssemblyOperator(
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(kafkaCaptor.getValue(), is(notNullValue()));
                    assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
                    KafkaStatus status = kafkaCaptor.getValue().getStatus();

                    assertThat(status.getConditions().size(), is(2));
                    assertThat(status.getConditions().get(0).getType(), is("Warning"));
                    assertThat(status.getConditions().get(0).getReason(), is("KafkaStorage"));
                    assertThat(status.getConditions().get(1).getType(), is("Ready"));

                    async.flag();
                })));
    }

    // This allows to test the status handling when reconciliation succeeds
    class MockWorkingKafkaAssemblyOperator extends KafkaAssemblyOperator  {
        public MockWorkingKafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, PasswordGenerator passwordGenerator, ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            ListenerStatus ls = new ListenerStatusBuilder()
                    .withNewType("plain")
                    .withAddresses(new ListenerAddressBuilder()
                            .withHost("my-service.my-namespace.svc")
                            .withPort(9092)
                            .build())
                    .build();

            ListenerStatus ls2 = new ListenerStatusBuilder()
                    .withNewType("external")
                    .withAddresses(new ListenerAddressBuilder()
                            .withHost("my-route-address.domain.tld")
                            .withPort(443)
                            .build())
                    .build();

            List<ListenerStatus> listeners = new ArrayList<>(2);
            listeners.add(ls);
            listeners.add(ls2);

            reconcileState.kafkaStatus.setListeners(listeners);

            return Future.succeededFuture();
        }
    }

    // This allows to test the status handling when reconciliation succeeds
    class MockFailingKafkaAssemblyOperator extends KafkaAssemblyOperator  {
        private final Throwable exception;

        public MockFailingKafkaAssemblyOperator(Throwable exception, Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, PasswordGenerator passwordGenerator, ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
            this.exception = exception;
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            ListenerStatus ls = new ListenerStatusBuilder()
                    .withNewType("plain")
                    .withAddresses(new ListenerAddressBuilder()
                            .withHost("my-service.my-namespace.svc")
                            .withPort(9092)
                            .build())
                    .build();

            reconcileState.kafkaStatus.setListeners(singletonList(ls));

            return Future.failedFuture(exception);
        }
    }

    // This allows to test the initial status handling when new resource is created
    class MockInitialStatusKafkaAssemblyOperator extends KafkaAssemblyOperator  {
        public MockInitialStatusKafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, PasswordGenerator passwordGenerator, ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            return reconcileState.initialStatus()
                    .map((Void) null);
        }
    }

    class MockNodePortStatusKafkaAssemblyOperator extends KafkaAssemblyOperator  {
        public MockNodePortStatusKafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, PasswordGenerator passwordGenerator, ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            reconcileState.kafkaBootstrapNodePorts.put("external-9094", 31234);

            return reconcileState.getKafkaClusterDescription()
                    .compose(state -> state.kafkaNodePortExternalListenerStatus())
                    .map((Void) null);
        }
    }

    class MockModelWarningsStatusKafkaAssemblyOperator extends KafkaAssemblyOperator  {
        public MockModelWarningsStatusKafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, PasswordGenerator passwordGenerator, ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            return reconcileState.getKafkaClusterDescription()
                    .compose(state -> state.kafkaModelWarnings())
                    .map((Void) null);
        }

    }

    class MockClusterIdStatusKafkaAssemblyOperator extends KafkaAssemblyOperator  {
        public MockClusterIdStatusKafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, PasswordGenerator passwordGenerator, ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            return reconcileState.kafkaGetClusterId()
                    .map((Void) null);
        }
    }

}
