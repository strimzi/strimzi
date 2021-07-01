/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.WatcherException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Meter;
import io.strimzi.api.kafka.model.Spec;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.api.kafka.model.status.Status;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.model.ResourceVisitor;
import io.strimzi.operator.common.model.ValidationVisitor;
import io.strimzi.operator.common.operator.resource.AbstractWatchableStatusedResourceOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.strimzi.operator.common.operator.resource.TimeoutException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.Util.async;

/**
 * A base implementation of {@link Operator}.
 *
 * <ul>
 * <li>uses the Fabric8 kubernetes API and implements
 * {@link #reconcile(Reconciliation)} by delegating to abstract {@link #createOrUpdate(Reconciliation, CustomResource)}
 * and {@link #delete(Reconciliation)} methods for subclasses to implement.
 * 
 * <li>add support for operator-side {@linkplain #validate(Reconciliation, CustomResource) validation}.
 *     This can be used to automatically log warnings about source resources which used deprecated part of the CR API.
 *ą
 * </ul>
 * @param <T> The Java representation of the Kubernetes resource, e.g. {@code Kafka} or {@code KafkaConnect}
 * @param <O> The "Resource Operator" for the source resource type. Typically this will be some instantiation of
 *           {@link io.strimzi.operator.common.operator.resource.CrdOperator}.
 */
public abstract class AbstractOperator<
        T extends CustomResource<P, S>,
        P extends Spec,
        S extends Status,
        O extends AbstractWatchableStatusedResourceOperator<?, T, ?, ?>>
            implements Operator {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(AbstractOperator.class);

    private static final long PROGRESS_WARNING = 60_000L;
    protected static final int LOCK_TIMEOUT_MS = 10000;
    public static final String METRICS_PREFIX = "strimzi.";

    protected final Vertx vertx;
    protected final O resourceOperator;
    private final String kind;

    private final Optional<LabelSelector> selector;

    protected final MetricsProvider metrics;

    private final Labels selectorLabels;
    private Map<String, AtomicInteger> resourcesStateCounter;
    private Map<String, AtomicInteger> resourceCounterMap;
    private Map<String, AtomicInteger> pausedResourceCounterMap;
    private Map<String, Counter> periodicReconciliationsCounterMap;
    private Map<String, Counter> reconciliationsCounterMap;
    private Map<String, Counter> failedReconciliationsCounterMap;
    private Map<String, Counter> successfulReconciliationsCounterMap;
    private Map<String, Counter> lockedReconciliationsCounterMap;
    private Map<String, Timer> reconciliationsTimerMap;

    public AbstractOperator(Vertx vertx, String kind, O resourceOperator, MetricsProvider metrics, Labels selectorLabels) {
        this.vertx = vertx;
        this.kind = kind;
        this.resourceOperator = resourceOperator;
        this.selector = (selectorLabels == null || selectorLabels.toMap().isEmpty()) ? Optional.empty() : Optional.of(new LabelSelector(null, selectorLabels.toMap()));
        this.metrics = metrics;
        this.selectorLabels = selectorLabels;

        resourceCounterMap = new ConcurrentHashMap<>(1);
        pausedResourceCounterMap = new ConcurrentHashMap<>(1);
        resourcesStateCounter = new ConcurrentHashMap<>(1);
        periodicReconciliationsCounterMap = new ConcurrentHashMap<>(1);
        reconciliationsCounterMap = new ConcurrentHashMap<>(1);
        failedReconciliationsCounterMap = new ConcurrentHashMap<>(1);
        successfulReconciliationsCounterMap = new ConcurrentHashMap<>(1);
        lockedReconciliationsCounterMap = new ConcurrentHashMap<>(1);
        reconciliationsTimerMap = new ConcurrentHashMap<>(1);
    }

    @Override
    public String kind() {
        return kind;
    }

    /**
     * Gets the name of the lock to be used for operating on the given {@code namespace} and
     * cluster {@code name}
     *
     * @param namespace The namespace containing the cluster
     * @param name The name of the cluster
     */
    private String getLockName(String namespace, String name) {
        return "lock::" + namespace + "::" + kind() + "::" + name;
    }

    /**
     * Asynchronously creates or updates the given {@code resource}.
     * This method can be called when the given {@code resource} has been created,
     * or updated and also at some regular interval while the resource continues to exist in Kubernetes.
     * The calling of this method does not imply that anything has actually changed.
     * @param reconciliation Uniquely identifies the reconciliation itself.
     * @param resource The resource to be created, or updated.
     * @return A Future which is completed once the reconciliation of the given resource instance is complete.
     */
    protected abstract Future<S> createOrUpdate(Reconciliation reconciliation, T resource);

    /**
     * Asynchronously deletes the resource identified by {@code reconciliation}.
     * Operators which only create other Kubernetes resources in order to honour their source resource can rely
     * on Kubernetes Garbage Collection to handle deletion.
     * Such operators should return a Future which completes with {@code false}.
     * Operators which handle deletion themselves should return a Future which completes with {@code true}.
     * @param reconciliation
     * @return
     */
    protected abstract Future<Boolean> delete(Reconciliation reconciliation);

    /**
     * Reconcile assembly resources in the given namespace having the given {@code name}.
     * Reconciliation works by getting the assembly resource (e.g. {@code KafkaUser})
     * in the given namespace with the given name and
     * comparing with the corresponding resource.
     * @param reconciliation The reconciliation.
     * @return A Future which is completed with the result of the reconciliation.
     */
    @Override
    @SuppressWarnings("unchecked")
    public final Future<Void> reconcile(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();

        getReconciliationsCounter(reconciliation.namespace()).increment();
        Timer.Sample reconciliationTimerSample = Timer.start(metrics.meterRegistry());

        Future<Void> handler = withLock(reconciliation, LOCK_TIMEOUT_MS, () -> {
            T cr = resourceOperator.get(namespace, name);

            if (cr != null) {
                if (!Util.matchesSelector(selector(), cr))  {
                    // When the labels matching the selector are removed from the custom resource, a DELETE event is
                    // triggered by the watch even through the custom resource might not match the watch labels anymore
                    // and might not be really deleted. We have to filter these situations out and ignore the
                    // reconciliation because such resource might be already operated by another instance (where the
                    // same change triggered ADDED event).
                    LOGGER.debugCr(reconciliation, "{} {} in namespace {} does not match label selector {} and will be ignored", kind(), name, namespace, selector().get().getMatchLabels());
                    return Future.succeededFuture();
                }

                Promise<Void> createOrUpdate = Promise.promise();
                if (Annotations.isReconciliationPausedWithAnnotation(cr)) {
                    S status = createStatus();
                    Set<Condition> conditions = validate(reconciliation, cr);
                    conditions.add(StatusUtils.getPausedCondition());
                    status.setConditions(new ArrayList<>(conditions));
                    status.setObservedGeneration(cr.getStatus() != null ? cr.getStatus().getObservedGeneration() : 0);

                    updateStatus(reconciliation, status).onComplete(statusResult -> {
                        if (statusResult.succeeded()) {
                            createOrUpdate.complete();
                        } else {
                            createOrUpdate.fail(statusResult.cause());
                        }
                    });
                    getPausedResourceCounter(namespace).getAndIncrement();
                    LOGGER.debugCr(reconciliation, "Reconciliation of {} {} is paused", kind, name);
                    return createOrUpdate.future();
                } else if (cr.getSpec() == null) {
                    InvalidResourceException exception = new InvalidResourceException("Spec cannot be null");

                    S status = createStatus();
                    Condition errorCondition = new ConditionBuilder()
                            .withLastTransitionTime(StatusUtils.iso8601Now())
                            .withType("NotReady")
                            .withStatus("True")
                            .withReason(exception.getClass().getSimpleName())
                            .withMessage(exception.getMessage())
                            .build();
                    status.setObservedGeneration(cr.getMetadata().getGeneration());
                    status.addCondition(errorCondition);

                    LOGGER.errorCr(reconciliation, "{} spec cannot be null", cr.getMetadata().getName());
                    updateStatus(reconciliation, status).onComplete(notUsed -> {
                        createOrUpdate.fail(exception);
                    });

                    return createOrUpdate.future();
                }

                Set<Condition> unknownAndDeprecatedConditions = validate(reconciliation, cr);

                LOGGER.infoCr(reconciliation, "{} {} will be checked for creation or modification", kind, name);

                createOrUpdate(reconciliation, cr)
                        .onComplete(res -> {
                            if (res.succeeded()) {
                                S status = res.result();

                                addWarningsToStatus(status, unknownAndDeprecatedConditions);
                                updateStatus(reconciliation, status).onComplete(statusResult -> {
                                    if (statusResult.succeeded()) {
                                        createOrUpdate.complete();
                                    } else {
                                        createOrUpdate.fail(statusResult.cause());
                                    }
                                });
                            } else {
                                if (res.cause() instanceof ReconciliationException) {
                                    ReconciliationException e = (ReconciliationException) res.cause();
                                    Status status = e.getStatus();
                                    addWarningsToStatus(status, unknownAndDeprecatedConditions);

                                    LOGGER.errorCr(reconciliation, "createOrUpdate failed", e.getCause());

                                    updateStatus(reconciliation, (S) status).onComplete(statusResult -> {
                                        createOrUpdate.fail(e.getCause());
                                    });
                                } else {
                                    LOGGER.errorCr(reconciliation, "createOrUpdate failed", res.cause());
                                    createOrUpdate.fail(res.cause());
                                }
                            }
                        });

                return createOrUpdate.future();
            } else {
                LOGGER.infoCr(reconciliation, "{} {} should be deleted", kind, name);
                return delete(reconciliation).map(deleteResult -> {
                    if (deleteResult) {
                        LOGGER.infoCr(reconciliation, "{} {} deleted", kind, name);
                    } else {
                        LOGGER.infoCr(reconciliation, "Assembly {} or some parts of it will be deleted by garbage collection", name);
                    }
                    return (Void) null;
                }).recover(deleteResult -> {
                    LOGGER.errorCr(reconciliation, "Deletion of {} {} failed", kind, name, deleteResult);
                    return Future.failedFuture(deleteResult);
                });
            }
        });

        Promise<Void> result = Promise.promise();
        handler.onComplete(reconcileResult -> {
            handleResult(reconciliation, reconcileResult, reconciliationTimerSample);
            result.handle(reconcileResult);
        });

        return result.future();
    }

    protected void addWarningsToStatus(Status status, Set<Condition> unknownAndDeprecatedConditions)   {
        if (status != null)  {
            status.addConditions(unknownAndDeprecatedConditions);
        }
    }

    /**
     * Updates the Status field of the Kafka CR. It diffs the desired status against the current status and calls
     * the update only when there is any difference in non-timestamp fields.
     *
     * @param reconciliation the reconciliation identified
     * @param desiredStatus The KafkaStatus which should be set
     *
     * @return
     */
    Future<Void> updateStatus(Reconciliation reconciliation, S desiredStatus) {
        if (desiredStatus == null)  {
            LOGGER.debugCr(reconciliation, "Desired status is null - status will not be updated");
            return Future.succeededFuture();
        }

        String namespace = reconciliation.namespace();
        String name = reconciliation.name();

        return resourceOperator.getAsync(namespace, name)
                .compose(res -> {
                    if (res != null) {
                        S currentStatus = res.getStatus();
                        StatusDiff sDiff = new StatusDiff(currentStatus, desiredStatus);

                        if (!sDiff.isEmpty()) {
                            res.setStatus(desiredStatus);

                            return resourceOperator.updateStatusAsync(reconciliation, res)
                                    .compose(notUsed -> {
                                        LOGGER.debugCr(reconciliation, "Completed status update");
                                        return Future.succeededFuture();
                                    }, error -> {
                                            LOGGER.errorCr(reconciliation, "Failed to update status", error);
                                            return Future.failedFuture(error);
                                        });
                        } else {
                            LOGGER.debugCr(reconciliation, "Status did not change");
                            return Future.succeededFuture();
                        }
                    } else {
                        LOGGER.errorCr(reconciliation, "Current {} resource not found", reconciliation.kind());
                        return Future.failedFuture("Current " + reconciliation.kind() + " resource with name " + name + " not found");
                    }
                }, error -> {
                        LOGGER.errorCr(reconciliation, "Failed to get the current {} resource and its status", reconciliation.kind(), error);
                        return Future.failedFuture(error);
                    });
    }

    protected abstract S createStatus();

    /**
     * The exception by which Futures returned by {@link #withLock(Reconciliation, long, Callable)} are failed when
     * the lock cannot be acquired within the timeout.
     */
    static class UnableToAcquireLockException extends TimeoutException { }

    /**
     * Acquire the lock for the resource implied by the {@code reconciliation}
     * and call the given {@code callable} with the lock held.
     * Once the callable returns (or if it throws) release the lock and complete the returned Future.
     * If the lock cannot be acquired the given {@code callable} is not called and the returned Future is completed with {@link UnableToAcquireLockException}.
     * @param reconciliation
     * @param callable
     * @param <T>
     * @return
     */
    protected final <T> Future<T> withLock(Reconciliation reconciliation, long lockTimeoutMs, Callable<Future<T>> callable) {
        Promise<T> handler = Promise.promise();
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        final String lockName = getLockName(namespace, name);
        LOGGER.debugCr(reconciliation, "Try to acquire lock {}", lockName);
        vertx.sharedData().getLockWithTimeout(lockName, lockTimeoutMs, res -> {
            if (res.succeeded()) {
                LOGGER.debugCr(reconciliation, "Lock {} acquired", lockName);

                Lock lock = res.result();
                long timerId = vertx.setPeriodic(PROGRESS_WARNING, timer -> {
                    LOGGER.infoCr(reconciliation, "Reconciliation is in progress");
                });

                try {
                    callable.call().onComplete(callableRes -> {
                        if (callableRes.succeeded()) {
                            handler.complete(callableRes.result());
                        } else {
                            handler.fail(callableRes.cause());
                        }

                        vertx.cancelTimer(timerId);
                        lock.release();
                        LOGGER.debugCr(reconciliation, "Lock {} released", lockName);
                    });
                } catch (Throwable ex) {
                    vertx.cancelTimer(timerId);
                    lock.release();
                    LOGGER.debugCr(reconciliation, "Lock {} released", lockName);
                    LOGGER.errorCr(reconciliation, "Reconciliation failed", ex);
                    handler.fail(ex);
                }
            } else {
                LOGGER.debugCr(reconciliation, "Failed to acquire lock {} within {}ms.", lockName, lockTimeoutMs);
                handler.fail(new UnableToAcquireLockException());
            }
        });
        return handler.future();
    }

    /**
     * Validate the Custom Resource.
     * This should log at the WARN level (rather than throwing)
     * if the resource can safely be reconciled (e.g. it merely using deprecated API).
     * @param reconciliation The reconciliation
     * @param resource The custom resource
     * @throws InvalidResourceException if the resource cannot be safely reconciled.
     * @return set of conditions
     */
    /*test*/ public Set<Condition> validate(Reconciliation reconciliation, T resource) {
        if (resource != null) {
            Set<Condition> warningConditions = new LinkedHashSet<>(0);

            ResourceVisitor.visit(reconciliation, resource, new ValidationVisitor(resource, LOGGER, warningConditions));

            return warningConditions;
        }

        return Collections.emptySet();
    }

    public Future<Set<NamespaceAndName>> allResourceNames(String namespace) {
        return resourceOperator.listAsync(namespace, selector())
                .map(resourceList ->
                        resourceList.stream()
                                .map(resource -> new NamespaceAndName(resource.getMetadata().getNamespace(), resource.getMetadata().getName()))
                                .collect(Collectors.toSet()));
    }

    /**
     * A selector to narrow the scope of the {@linkplain #createWatch(String, Consumer) watch}
     * and {@linkplain #allResourceNames(String) query}.
     * @return A selector.
     */
    public Optional<LabelSelector> selector() {
        return selector;
    }

    /**
     * Create Kubernetes watch.
     *
     * @param namespace Namespace where to watch for users.
     * @param onClose Callback called when the watch is closed.
     *
     * @return A future which completes when the watcher has been created.
     */
    public Future<Watch> createWatch(String namespace, Consumer<WatcherException> onClose) {
        return async(vertx, () -> resourceOperator.watch(namespace, selector(), new OperatorWatcher<>(this, namespace, onClose)));
    }

    public Consumer<WatcherException> recreateWatch(String namespace) {
        Consumer<WatcherException> kubernetesClientExceptionConsumer = new Consumer<WatcherException>() {
            @Override
            public void accept(WatcherException e) {
                if (e != null) {
                    LOGGER.errorOp("Watcher closed with exception in namespace {}", namespace, e);
                    createWatch(namespace, this);
                } else {
                    LOGGER.infoOp("Watcher closed in namespace {}", namespace);
                }
            }
        };
        return kubernetesClientExceptionConsumer;
    }

    /**
     * Log the reconciliation outcome.
     */
    private void handleResult(Reconciliation reconciliation, AsyncResult<Void> result, Timer.Sample reconciliationTimerSample) {
        if (result.succeeded()) {
            updateResourceState(reconciliation, true, null);
            getSuccessfulReconciliationsCounter(reconciliation.namespace()).increment();
            reconciliationTimerSample.stop(getReconciliationsTimer(reconciliation.namespace()));
            LOGGER.infoCr(reconciliation, "reconciled");
        } else {
            Throwable cause = result.cause();

            if (cause instanceof InvalidConfigParameterException) {
                updateResourceState(reconciliation, false, cause);
                getFailedReconciliationsCounter(reconciliation.namespace()).increment();
                reconciliationTimerSample.stop(getReconciliationsTimer(reconciliation.namespace()));
                LOGGER.warnCr(reconciliation, "Failed to reconcile {}", cause.getMessage());
            } else if (cause instanceof UnableToAcquireLockException) {
                getLockedReconciliationsCounter(reconciliation.namespace()).increment();
            } else  {
                updateResourceState(reconciliation, false, cause);
                getFailedReconciliationsCounter(reconciliation.namespace()).increment();
                reconciliationTimerSample.stop(getReconciliationsTimer(reconciliation.namespace()));
                LOGGER.warnCr(reconciliation, "Failed to reconcile", cause);
            }
        }
    }

    @Override
    public Counter getPeriodicReconciliationsCounter(String namespace) {
        return Operator.getCounter(namespace, kind(), metrics, selectorLabels, periodicReconciliationsCounterMap, METRICS_PREFIX + "reconciliations.periodical",
                "Number of periodical reconciliations done by the operator");
    }

    public Counter getReconciliationsCounter(String namespace) {
        return Operator.getCounter(namespace, kind(), metrics, selectorLabels, reconciliationsCounterMap, METRICS_PREFIX + "reconciliations",
                "Number of reconciliations done by the operator for individual resources");
    }

    public Counter getFailedReconciliationsCounter(String namespace) {
        return Operator.getCounter(namespace, kind(), metrics, selectorLabels, failedReconciliationsCounterMap, METRICS_PREFIX + "reconciliations.failed",
                "Number of reconciliations done by the operator for individual resources which failed");
    }

    public Counter getSuccessfulReconciliationsCounter(String namespace) {
        return Operator.getCounter(namespace, kind(), metrics, selectorLabels, successfulReconciliationsCounterMap, METRICS_PREFIX + "reconciliations.successful",
                "Number of reconciliations done by the operator for individual resources which were successful");
    }

    public Counter getLockedReconciliationsCounter(String namespace) {
        return Operator.getCounter(namespace, kind(), metrics, selectorLabels, lockedReconciliationsCounterMap, METRICS_PREFIX + "reconciliations.locked",
                "Number of reconciliations skipped because another reconciliation for the same resource was still running");
    }

    @Override
    public AtomicInteger getResourceCounter(String namespace) {
        return Operator.getGauge(namespace, kind(), metrics, selectorLabels, resourceCounterMap, METRICS_PREFIX + "resources",
                "Number of custom resources the operator sees");
    }

    @Override
    public AtomicInteger getPausedResourceCounter(String namespace) {
        return Operator.getGauge(namespace, kind(), metrics, selectorLabels, pausedResourceCounterMap, METRICS_PREFIX + "resources.paused",
                "Number of custom resources the operator sees but does not reconcile due to paused reconciliations");
    }

    public Timer getReconciliationsTimer(String namespace) {
        return Operator.getTimer(namespace, kind(), metrics, selectorLabels, reconciliationsTimerMap, METRICS_PREFIX + "reconciliations.duration",
                "The time the reconciliation takes to complete");
    }

    /**
     * Updates the resource state metric for the provided reconciliation which brings kind, name and namespace
     * of the custom resource.
     *
     * @param reconciliation reconciliation to use to update the resource state metric
     * @param ready if reconcile was successful and the resource is ready
     */
    private void updateResourceState(Reconciliation reconciliation, boolean ready, Throwable cause) {
        String key = reconciliation.namespace() + ":" + reconciliation.kind() + "/" + reconciliation.name();

        Tags metricTags = Tags.of(
                    Tag.of("kind", reconciliation.kind()),
                    Tag.of("name", reconciliation.name()),
                    Tag.of("resource-namespace", reconciliation.namespace()),
                    Tag.of("reason", cause == null ? "none" : cause.getMessage() == null ? "unknown error" : cause.getMessage()));

        T cr = resourceOperator.get(reconciliation.namespace(), reconciliation.name());

        Optional<Meter> metric = metrics.meterRegistry().getMeters()
                .stream()
                .filter(meter -> meter.getId().getName().equals(METRICS_PREFIX + "resource.state") &&
                        meter.getId().getTags().contains(Tag.of("kind", reconciliation.kind())) &&
                        meter.getId().getTags().contains(Tag.of("name", reconciliation.name())) &&
                        meter.getId().getTags().contains(Tag.of("resource-namespace", reconciliation.namespace()))
                ).findFirst();

        if (metric.isPresent()) {
            // remove metric so it can be re-added with new tags
            metrics.meterRegistry().remove(metric.get().getId());
            resourcesStateCounter.remove(key);
            LOGGER.debugCr(reconciliation, "Removed metric " + METRICS_PREFIX + "resource.state{}", key);
        }

        if (cr != null) {
            resourcesStateCounter.computeIfAbsent(key, tags ->
                    metrics.gauge(METRICS_PREFIX + "resource.state", "Current state of the resource: 1 ready, 0 fail", metricTags)
            );
            resourcesStateCounter.get(key).set(ready ? 1 : 0);
            LOGGER.debugCr(reconciliation, "Updated metric " + METRICS_PREFIX + "resource.state{} = {}", metricTags, ready ? 1 : 0);
        }
    }

    /**
     * In some cases, when the ClusterRoleBinding reconciliation fails with RBAC error and the desired object is null,
     * we want to ignore the error and return success. This is used to let Strimzi work without some Cluster-wide RBAC
     * rights when the features they are needed for are not used by the user.
     *
     * @param reconciliation    The reconciliation
     * @param reconcileFuture   The original reconciliation future
     * @param desired           The desired state of the resource.
     * @return                  A future which completes when the resource was reconciled.
     */
    public Future<ReconcileResult<ClusterRoleBinding>> withIgnoreRbacError(Reconciliation reconciliation, Future<ReconcileResult<ClusterRoleBinding>> reconcileFuture, ClusterRoleBinding desired) {
        return reconcileFuture.compose(
            rr -> Future.succeededFuture(),
            e -> {
                if (desired == null
                        && e.getMessage() != null
                        && e.getMessage().contains("Message: Forbidden!")) {
                    LOGGER.debugCr(reconciliation, "Ignoring forbidden access to ClusterRoleBindings resource which does not seem to be required.");
                    return Future.succeededFuture();
                }
                return Future.failedFuture(e.getMessage());
            }
        );
    }

}
