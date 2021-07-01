/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.strimzi.api.kafka.model.Spec;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.Status;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.operator.resource.AbstractWatchableStatusedResourceOperator;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptySet;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(VertxExtension.class)
@Group("strimzi")
@Version("v1")
public class OperatorMetricsTest {
    private static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        .setEnabled(true)
        ));
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    public void successfulReconcile(VertxTestContext context, Labels selectorLabels)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        AbstractWatchableStatusedResourceOperator resourceOperator = resourceOperatorWithExistingResourceWithSelectorLabel(selectorLabels);

        AbstractOperator operator = new AbstractOperator(vertx, "TestResource", resourceOperator, metrics, selectorLabels) {
            @Override
            protected Future createOrUpdate(Reconciliation reconciliation, CustomResource resource) {
                return Future.succeededFuture();
            }

            @Override
            public Set<Condition> validate(Reconciliation reconciliation, CustomResource resource) {
                return emptySet();
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }

            @Override
            protected Status createStatus() {
                return new Status() { };
            }
        };

        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    MeterRegistry registry = metrics.meterRegistry();
                    Tag selectorTag = Tag.of("selector", selectorLabels != null ? selectorLabels.toSelectorString() : "");

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "TestResource").counter().count(), is(1.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().count(), is(1L));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "resource.state")
                            .tag("kind", "TestResource")
                            .tag("name", "my-resource")
                            .tag("resource-namespace", "my-namespace")
                            .gauge().value(), is(1.0));

                    async.flag();
                })));
    }

    @Test
    public void testReconcileWithEmptySelectorLabels(VertxTestContext context) {
        successfulReconcile(context, Labels.fromMap(emptyMap()));
    }

    @Test
    public void testReconcileWithValuedSelectorLabel(VertxTestContext context) {
        successfulReconcile(context, Labels.fromMap(Collections.singletonMap("io/my-test-label", "my-test-value")));
    }

    @Test
    public void testReconcileWithoutSelectorLabel(VertxTestContext context) {
        successfulReconcile(context, null);
    }

    public void failingReconcile(VertxTestContext context, Labels selectorLabels)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        AbstractWatchableStatusedResourceOperator resourceOperator = resourceOperatorWithExistingResourceWithSelectorLabel(selectorLabels);

        AbstractOperator operator = new AbstractOperator(vertx, "TestResource", resourceOperator, metrics, selectorLabels) {
            @Override
            protected Future createOrUpdate(Reconciliation reconciliation, CustomResource resource) {
                return Future.failedFuture(new RuntimeException("Test error"));
            }

            @Override
            public Set<Condition> validate(Reconciliation reconciliation, CustomResource resource) {
                // Do nothing
                return emptySet();
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }

            @Override
            protected Status createStatus() {
                return new Status() { };
            }
        };

        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.failing(v -> context.verify(() -> {
                    MeterRegistry registry = metrics.meterRegistry();
                    Tag selectorTag = Tag.of("selector", selectorLabels != null ? selectorLabels.toSelectorString() : "");

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.failed").meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "TestResource").counter().count(), is(1.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().count(), is(1L));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "resource.state")
                            .tag("kind", "TestResource")
                            .tag("name", "my-resource")
                            .tag("resource-namespace", "my-namespace")
                            .tag("reason", "Test error")
                            .gauge().value(), is(0.0));

                    async.flag();
                })));
    }

    @Test
    public void testFailingReconcileWithEmptySelectorLabels(VertxTestContext context) {
        failingReconcile(context, Labels.fromMap(emptyMap()));
    }

    @Test
    public void testFailingReconcileWithValuedSelectorLabel(VertxTestContext context) {
        failingReconcile(context, Labels.fromMap(Collections.singletonMap("io/my-test-label", "my-test-value")));
    }

    @Test
    public void testFailingReconcileWithoutSelectorLabel(VertxTestContext context) {
        failingReconcile(context, null);
    }

    @Test
    public void testPauseReconcile(VertxTestContext context)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        AbstractWatchableStatusedResourceOperator resourceOperator = resourceOperatorWithExistingPausedResource();

        AbstractOperator operator = new AbstractOperator(vertx, "TestResource", resourceOperator, metrics, null) {
            @Override
            protected Future createOrUpdate(Reconciliation reconciliation, CustomResource resource) {
                return Future.succeededFuture();
            }

            @Override
            public Set<Condition> validate(Reconciliation reconciliation, CustomResource resource) {
                return new HashSet<>();
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }

            @Override
            protected Status createStatus() {
                return new Status() { };
            }
        };

        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    MeterRegistry registry = metrics.meterRegistry();
                    Tag selectorTag = Tag.of("selector", "");

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "resources.paused").meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "resources.paused").tag("kind", "TestResource").gauge().value(), is(1.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().count(), is(1L));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "resource.state")
                            .tag("kind", "TestResource")
                            .tag("name", "my-resource")
                            .tag("resource-namespace", "my-namespace")
                            .gauge().value(), is(1.0));

                    async.flag();
                })));
    }

    @Test
    public void testFailingWithLockReconcile(VertxTestContext context)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        AbstractWatchableStatusedResourceOperator resourceOperator = resourceOperatorWithExistingResourceWithoutSelectorLabel();

        AbstractOperator operator = new AbstractOperator(vertx, "TestResource", resourceOperator, metrics, null) {
            @Override
            protected Future createOrUpdate(Reconciliation reconciliation, CustomResource resource) {
                return Future.failedFuture(new UnableToAcquireLockException());
            }

            @Override
            public Set<Condition> validate(Reconciliation reconciliation, CustomResource resource) {
                // Do nothing
                return emptySet();
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }

            @Override
            protected Status createStatus() {
                return new Status() { };
            }
        };

        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.failing(v -> context.verify(() -> {
                    MeterRegistry registry = metrics.meterRegistry();
                    Tag selectorTag = Tag.of("selector", "");

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.locked").meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.locked").tag("kind", "TestResource").counter().count(), is(1.0));
                    async.flag();
                })));
    }

    @Test
    public void testDeleteCountsReconcile(VertxTestContext context)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        AbstractWatchableStatusedResourceOperator resourceOperator = new AbstractWatchableStatusedResourceOperator(vertx, null, "TestResource") {
            @Override
            protected MixedOperation operation() {
                return null;
            }

            @Override
            public HasMetadata get(String namespace, String name) {
                return null;
            }

            @Override
            public Future updateStatusAsync(Reconciliation reconciliation, HasMetadata resource) {
                return null;
            }
        };

        AbstractOperator operator = new AbstractOperator(vertx, "TestResource", resourceOperator, metrics, null) {
            @Override
            protected Future createOrUpdate(Reconciliation reconciliation, CustomResource resource) {
                return null;
            }

            @Override
            public Set<Condition> validate(Reconciliation reconciliation, CustomResource resource) {
                // Do nothing
                return emptySet();
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return Future.succeededFuture(Boolean.TRUE);
            }

            @Override
            protected Status createStatus() {
                return new Status() { };
            }
        };

        Checkpoint async = context.checkpoint();
        operator.reconcile(new Reconciliation("test", "TestResource", "my-namespace", "my-resource"))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    MeterRegistry registry = metrics.meterRegistry();
                    Tag selectorTag = Tag.of("selector", "");

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "TestResource").counter().count(), is(1.0));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "TestResource").counter().count(), is(1.0));

                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").meter().getId().getTags().get(2), is(selectorTag));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().count(), is(1L));
                    assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                    assertThrows(MeterNotFoundException.class, () -> {
                        registry.get(AbstractOperator.METRICS_PREFIX + "resource.state")
                                .tag("kind", "TestResource")
                                .tag("name", "my-resource")
                                .tag("resource-namespace", "my-namespace")
                                .gauge();
                    });

                    async.flag();
                })));
    }

    @Test
    public void testReconcileAll(VertxTestContext context)  {
        MetricsProvider metrics = createCleanMetricsProvider();

        Set<NamespaceAndName> resources = new HashSet<>(3);
        resources.add(new NamespaceAndName("my-namespace", "avfc"));
        resources.add(new NamespaceAndName("my-namespace", "vtid"));
        resources.add(new NamespaceAndName("my-namespace", "utv"));

        AbstractWatchableStatusedResourceOperator resourceOperator = resourceOperatorWithExistingResourceWithoutSelectorLabel();

        AbstractOperator operator = new AbstractOperator(vertx, "TestResource", resourceOperator, metrics, null) {
            @Override
            protected Future createOrUpdate(Reconciliation reconciliation, CustomResource resource) {
                return Future.succeededFuture();
            }

            public Future<Set<NamespaceAndName>> allResourceNames(String namespace) {
                return Future.succeededFuture(resources);
            }

            @Override
            public Set<Condition> validate(Reconciliation reconciliation, CustomResource resource) {
                // Do nothing
                return emptySet();
            }

            @Override
            Future<Void> updateStatus(Reconciliation reconciliation, Status desiredStatus) {
                return Future.succeededFuture();
            }

            @Override
            protected Future<Boolean> delete(Reconciliation reconciliation) {
                return null;
            }

            @Override
            protected Status createStatus() {
                return new Status() { };
            }
        };

        Promise<Void> reconcileAllPromise = Promise.promise();
        operator.reconcileAll("test", "my-namespace", reconcileAllPromise);

        Checkpoint async = context.checkpoint();
        reconcileAllPromise.future().onComplete(context.succeeding(v -> context.verify(() -> {
            MeterRegistry registry = metrics.meterRegistry();
            Tag selectorTag = Tag.of("selector", "");

            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.periodical").meter().getId().getTags().get(2), is(selectorTag));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.periodical").tag("kind", "TestResource").counter().count(), is(1.0));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").meter().getId().getTags().get(2), is(selectorTag));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "TestResource").counter().count(), is(3.0));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").meter().getId().getTags().get(2), is(selectorTag));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "TestResource").counter().count(), is(3.0));

            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").meter().getId().getTags().get(2), is(selectorTag));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().count(), is(3L));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "TestResource").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

            for (NamespaceAndName resource : resources) {
                assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "resource.state")
                        .tag("kind", "TestResource")
                        .tag("name", resource.getName())
                        .tag("resource-namespace", resource.getNamespace())
                        .gauge().value(), is(1.0));
            }

            async.flag();
        })));
    }

    /**
     * Created new MetricsProvider and makes sure it doesn't contain any metrics from previous tests.
     *
     * @return  Clean MetricsProvider
     */
    public MetricsProvider createCleanMetricsProvider() {
        MetricsProvider metrics = new MicrometerMetricsProvider();
        MeterRegistry registry = metrics.meterRegistry();

        registry.forEachMeter(meter -> {
            registry.remove(meter);
        });

        return metrics;
    }

    protected abstract static class MyResource extends CustomResource {
    }

    protected AbstractWatchableStatusedResourceOperator resourceOperatorWithExistingResource(Labels selectorLabels)    {
        return new AbstractWatchableStatusedResourceOperator(vertx, null, "TestResource") {
            @Override
            public Future updateStatusAsync(Reconciliation reconciliation, HasMetadata resource) {
                return null;
            }

            @Override
            protected MixedOperation operation() {
                return null;
            }

            @Override
            public CustomResource get(String namespace, String name) {
                @Group("strimzi")
                @Version("v1")
                class Foo extends MyResource {
                    @Override
                    public ObjectMeta getMetadata() {
                        ObjectMeta md = new ObjectMeta();
                        if (selectorLabels != null) {
                            md.setLabels(selectorLabels.toMap());
                        }
                        return md;
                    }

                    @Override
                    public void setMetadata(ObjectMeta objectMeta) {

                    }

                    @Override
                    public String getKind() {
                        return "TestResource";
                    }

                    @Override
                    public String getApiVersion() {
                        return "v1";
                    }

                    @Override
                    public void setApiVersion(String s) {

                    }

                    @Override
                    public Spec getSpec() {
                        return new Spec() { };
                    }

                    @Override
                    public void setSpec(Object spec) {
                    }

                    @Override
                    public Status getStatus() {
                        return null;
                    }

                    @Override
                    public void setStatus(Object status) {

                    }
                }
                return new Foo();
            }
        };
    }

    private AbstractWatchableStatusedResourceOperator resourceOperatorWithExistingResourceWithoutSelectorLabel() {
        return resourceOperatorWithExistingResource(null);
    }

    private AbstractWatchableStatusedResourceOperator resourceOperatorWithExistingResourceWithSelectorLabel(Labels selectorLabel) {
        return resourceOperatorWithExistingResource(selectorLabel);
    }

    private AbstractWatchableStatusedResourceOperator resourceOperatorWithExistingPausedResource() {
        return new AbstractWatchableStatusedResourceOperator(vertx, null, "TestResource") {
            @Override
            public Future updateStatusAsync(Reconciliation reconciliation, HasMetadata resource) {
                return Future.succeededFuture();
            }

            @Override
            protected MixedOperation operation() {
                return null;
            }

            @Override
            public CustomResource get(String namespace, String name) {
                @Group("strimzi")
                @Version("v1")
                class Foo extends MyResource {
                    @Override
                    public ObjectMeta getMetadata() {
                        ObjectMeta md = new ObjectMeta();
                        md.setAnnotations(singletonMap("strimzi.io/pause-reconciliation", "true"));
                        return md;
                    }

                    @Override
                    public void setMetadata(ObjectMeta objectMeta) {

                    }

                    @Override
                    public String getKind() {
                        return "TestResource";
                    }

                    @Override
                    public String getApiVersion() {
                        return "v1";
                    }

                    @Override
                    public void setApiVersion(String s) {

                    }

                    @Override
                    public Spec getSpec() {
                        return new Spec() { };
                    }

                    @Override
                    public void setSpec(Object spec) {
                    }

                    @Override
                    public Status getStatus() {
                        return null;
                    }

                    @Override
                    public void setStatus(Object status) {

                    }
                }
                return new Foo();
            }

            @Override
            public Future getAsync(String namespace, String name) {
                @Group("strimzi")
                @Version("v1")
                class Foo extends MyResource {
                    @Override
                    public ObjectMeta getMetadata() {
                        ObjectMeta md = new ObjectMeta();
                        md.setAnnotations(singletonMap("strimzi.io/pause-reconciliation", "true"));
                        return md;
                    }

                    @Override
                    public void setMetadata(ObjectMeta objectMeta) {

                    }

                    @Override
                    public String getKind() {
                        return "TestResource";
                    }

                    @Override
                    public String getApiVersion() {
                        return "v1";
                    }

                    @Override
                    public void setApiVersion(String s) {

                    }

                    @Override
                    public Spec getSpec() {
                        return new Spec() { };
                    }

                    @Override
                    public void setSpec(Object spec) {
                    }

                    @Override
                    public Status getStatus() {
                        return null;
                    }

                    @Override
                    public void setStatus(Object status) {

                    }
                }
                return Future.succeededFuture(new Foo());
            }
        };
    }
}
