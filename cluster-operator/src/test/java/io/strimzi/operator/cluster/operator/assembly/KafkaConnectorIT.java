/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.MeterRegistry;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectorBuilder;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.AbstractOperator;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaConnectorIT {
    private static EmbeddedKafkaCluster cluster;
    private static Vertx vertx;
    private ConnectCluster connectCluster;

    @BeforeAll
    public static void before() throws IOException {
        vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        .setEnabled(true)
        ));

        cluster = new EmbeddedKafkaCluster(3);
        cluster.start();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @BeforeEach
    public void beforeEach() throws IOException, InterruptedException {
        String connectClusterName = getClass().getSimpleName();
        cluster.createTopics(connectClusterName + "-offsets", connectClusterName + "-config", connectClusterName + "-status");

        // Start a 3 node connect cluster
        connectCluster = new ConnectCluster()
                .usingBrokers(cluster.bootstrapServers())
                .addConnectNodes(3);
        connectCluster.startup();
    }

    @AfterEach
    public void afterEach() {
        if (connectCluster != null) {
            connectCluster.shutdown();
        }
    }

    @Test
    public void test(VertxTestContext context) {
        KafkaConnectApiImpl connectClient = new KafkaConnectApiImpl(vertx);

        KubernetesClient client = new MockKube()
                .withCustomResourceDefinition(Crds.kafkaConnector(), KafkaConnector.class, KafkaConnectorList.class)
                .end()
                .build();
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(false, KubernetesVersion.V1_20);

        String namespace = "ns";
        String connectorName = "my-connector";

        LinkedHashMap<String, Object> config = new LinkedHashMap<>();
        config.put(TestingConnector.START_TIME_MS, 1_000);
        config.put(TestingConnector.STOP_TIME_MS, 0);
        config.put(TestingConnector.TASK_START_TIME_MS, 1_000);
        config.put(TestingConnector.TASK_STOP_TIME_MS, 0);
        config.put(TestingConnector.TASK_POLL_TIME_MS, 1_000);
        config.put(TestingConnector.TASK_POLL_RECORDS, 100);
        config.put(TestingConnector.NUM_PARTITIONS, 1);
        config.put(TestingConnector.TOPIC_NAME, "my-topic");

        KafkaConnector connector = createKafkaConnector(namespace, connectorName, config);
        Crds.kafkaConnectorOperation(client).inNamespace(namespace).create(connector);

        // Intercept status updates at CrdOperator level
        // This is to bridge limitations between MockKube and the CrdOperator, as there are currently no Fabric8 APIs for status update
        CrdOperator connectCrdOperator = mock(CrdOperator.class);
        when(connectCrdOperator.updateStatusAsync(any(), any())).thenAnswer(invocation -> {
            try {
                return Future.succeededFuture(Crds.kafkaConnectorOperation(client)
                        .inNamespace(namespace)
                        .withName(connectorName)
                        .patch((KafkaConnector) invocation.getArgument(1)));
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        });
        when(connectCrdOperator.getAsync(any(), any())).thenAnswer(invocationOnMock -> {
            try {
                return Future.succeededFuture(Crds.kafkaConnectorOperation(client)
                        .inNamespace(namespace)
                        .withName(connectorName)
                        .get());
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        });

        MetricsProvider metrics = new MicrometerMetricsProvider();

        KafkaConnectAssemblyOperator operator = new KafkaConnectAssemblyOperator(vertx, pfa,
                new ResourceOperatorSupplier(
                        null, null, null, null, null, null, null, null, null, null, null, null,
                        null, null, null, null, null, null, null, null, null,
                        null, null, connectCrdOperator, null, null, null, null, null, metrics, null),
                ClusterOperatorConfig.fromMap(Collections.emptyMap(), KafkaVersionTestUtils.getKafkaVersionLookup()),
            connect -> new KafkaConnectApiImpl(vertx),
            connectCluster.getPort() + 2
        ) { };

        Checkpoint async = context.checkpoint();
        operator.reconcileConnectorAndHandleResult(new Reconciliation("test", "KafkaConnect", namespace, "bogus"),
                "localhost", connectClient, true, connectorName,
                connector)
            .onComplete(context.succeeding(v -> assertConnectorIsRunning(context, client, namespace, connectorName)))
            .compose(v -> {
                config.remove(TestingConnector.START_TIME_MS, 1_000);
                config.put(TestingConnector.START_TIME_MS, 1_000);
                Crds.kafkaConnectorOperation(client)
                        .inNamespace(namespace)
                        .withName(connectorName)
                        .patch(createKafkaConnector(namespace, connectorName, config));
                return operator.reconcileConnectorAndHandleResult(new Reconciliation("test", "KafkaConnect", namespace, "bogus"),
                        "localhost", connectClient, true, connectorName, connector);
            })
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertConnectorIsRunning(context, client, namespace, connectorName);
                // Assert metrics from Connector Operator
                MeterRegistry registry = metrics.meterRegistry();

                assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", KafkaConnector.RESOURCE_KIND).counter().count(), CoreMatchers.is(2.0));
                assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", KafkaConnector.RESOURCE_KIND).counter().count(), CoreMatchers.is(2.0));

                assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", KafkaConnector.RESOURCE_KIND).timer().count(), CoreMatchers.is(2L));
                assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", KafkaConnector.RESOURCE_KIND).timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                async.flag();
            })));
    }

    private KafkaConnector createKafkaConnector(String namespace, String connectorName, LinkedHashMap<String, Object> config) {
        return new KafkaConnectorBuilder()
                    .withNewMetadata()
                        .withName(connectorName)
                        .withNamespace(namespace)
                    .endMetadata()
                    .withNewSpec()
                        .withClassName(TestingConnector.class.getName())
                        .withTasksMax(1)
                        .withConfig(config)
                    .endSpec()
                    .build();
    }

    private void assertConnectorIsRunning(VertxTestContext context, KubernetesClient client, String namespace, String connectorName) {
        context.verify(() -> {
            KafkaConnector kafkaConnector = Crds.kafkaConnectorOperation(client).inNamespace(namespace).withName(connectorName).get();
            assertThat(kafkaConnector, notNullValue());
            assertThat(kafkaConnector.getStatus(), notNullValue());
            assertThat(kafkaConnector.getStatus().getTasksMax(), is(1));
            assertThat(kafkaConnector.getStatus().getConnectorStatus(), notNullValue());
            assertThat(kafkaConnector.getStatus().getConnectorStatus().get("connector"), instanceOf(Map.class));
            assertThat(((Map) kafkaConnector.getStatus().getConnectorStatus().get("connector")).get("state"), is("RUNNING"));
        });
    }
}
