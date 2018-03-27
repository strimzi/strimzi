/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.strimzi.controller.cluster.operator.resource.ConfigMapOperator;
import io.strimzi.controller.cluster.model.AbstractCluster;
import io.strimzi.controller.cluster.model.Labels;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>Abstract cluster creation, update, read, delection, etc, for a generic cluster type {@code C}.</p>
 *
 * <p>This class manages a per-cluster-type and per-cluster locking strategy so only one operation per cluster
 * can proceed at once.</p>
 * @param <C> The type of cluster
 * @param <R> The type of resource from which the cluster state can be recovered
 */
public abstract class AbstractAssemblyOperator<C extends AbstractCluster,
        R extends HasMetadata> {

    private static final Logger log = LoggerFactory.getLogger(AbstractAssemblyOperator.class.getName());

    protected static final int LOCK_TIMEOUT = 60000;

    protected final Vertx vertx;
    protected final boolean isOpenShift;
    protected final String clusterType;
    protected final String clusterDescription;
    protected final ConfigMapOperator configMapOperations;

    /**
     * @param vertx The Vertx instance
     * @param isOpenShift True iff running on OpenShift
     * @param clusterDescription A description of the cluster, for logging. This is a high level description and different from
     *                           the {@code clusterType} passed to {@link #getLockName(String, String, String)}
     */
    protected AbstractAssemblyOperator(Vertx vertx, boolean isOpenShift, String clusterType,
                                       String clusterDescription,
                                       ConfigMapOperator configMapOperations) {
        this.vertx = vertx;
        this.isOpenShift = isOpenShift;
        this.clusterType = clusterType;
        this.clusterDescription = clusterDescription;
        this.configMapOperations = configMapOperations;
    }

    /**
     * Gets the name of the lock to be used for operating on the given {@code clusterType}, {@code namespace} and
     * cluster {@code name}
     * @param clusterType The type of cluster
     * @param namespace The namespace containing the cluster
     * @param name The name of the cluster
     */
    protected final String getLockName(String clusterType, String namespace, String name) {
        return "lock::" + namespace + "::" + clusterType + "::" + name;
    }

    /**
     * Subclasses implement this method to create or update the cluster. The implementation
     * should not assume that any resources are in any particular state (e.g. that the absence on
     * one resource means that all resources need to be created).
     * @param namespace The namespace containing the cluster.
     * @param name The name of the cluster.
     * @param handler Completion handler
     */
    protected abstract void createOrUpdate(String namespace, String name, Handler<AsyncResult<Void>> handler);

    /**
     * Subclasses implement this method to delete the cluster.
     * @param namespace The namespace containing the cluster.
     * @param name The name of the cluster.
     * @param handler Completion handler
     */
    protected abstract void delete(String namespace, String name, Handler<AsyncResult<Void>> handler);

    /**
     * The name of the given {@code resource}, as read from its metadata.
     * @param resource The resource
     */
    protected static String name(HasMetadata resource) {
        if (resource != null) {
            ObjectMeta metadata = resource.getMetadata();
            if (metadata != null) {
                return metadata.getName();
            }
        }
        return null;
    }

    /**
     * The name of the given {@code resource}, as read from its {@code strimzi.io/cluster} label.
     * @param resource The resource
     */
    protected String nameFromLabels(R resource) {
        return Labels.cluster(resource);
    }

    /**
     * Reconcile cluster resources in the given namespace having the given cluster name.
     * Reconciliation works by getting the cluster ConfigMap in the given namespace with the given name and
     * comparing with the corresponding {@linkplain #getResources(String, Labels) resource}.
     * <ul>
     * <li>A cluster will be {@linkplain #createOrUpdate(String, String, Handler) createed or updated} if ConfigMap is without same-named resources</li>
     * <li>A cluster will be {@linkplain #delete(String, String, Handler) deleted} if resources without same-named ConfigMap</li>
     * </ul>
     * @param namespace The namespace
     * @param name The name of the cluster
     */
    public final void reconcileCluster(String namespace, String name) {
        final String lockName = getLockName(clusterType, namespace, name);
        vertx.sharedData().getLockWithTimeout(lockName, LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                log.debug("Lock {} acquired", lockName);
                Lock lock = res.result();

                try {
                    log.info("Reconciling {} clusters ...", clusterDescription);

                    // get ConfigMap and related resources for the specific cluster
                    ConfigMap cm = configMapOperations.get(namespace, name);

                    List<R> resources = getResources(namespace, Labels.forCluster(name));

                    if (cm != null) {
                        String nameFromCm = name(cm);
                        log.info("Reconciliation: {} cluster {} should be created or updated", clusterDescription, cm.getMetadata().getName());
                        log.info("Creating/updating {} cluster {}", clusterDescription, nameFromCm);
                        createOrUpdate(namespace, nameFromCm, createResult -> {
                            if (createResult.succeeded()) {
                                log.info("{} cluster created/updated {}", clusterDescription, nameFromCm);
                            } else {
                                log.error("Failed to create/update {} cluster {}.", clusterDescription, nameFromCm);
                            }
                            lock.release();
                            log.debug("Lock {} released", lockName);
                        });
                    } else {

                        List<Future> result = new ArrayList<>(resources.size());
                        for (R resource : resources) {
                            log.info("Reconciliation: {} cluster {} should be deleted", clusterDescription, resource.getMetadata().getName());
                            String nameFromResource = nameFromLabels(resource);
                            log.info("Deleting {} cluster {} in namespace {}", clusterDescription, nameFromResource, namespace);

                            Future<Void> deleteFuture = Future.future();
                            result.add(deleteFuture);
                            delete(namespace, nameFromResource, deleteResult -> {
                                if (deleteResult.succeeded()) {
                                    log.info("{} cluster deleted {} in namespace {}", clusterDescription, nameFromResource, namespace);
                                } else {
                                    log.error("Failed to delete {} cluster {} in namespace {}", clusterDescription, nameFromResource, namespace);
                                }
                                deleteFuture.complete();
                            });
                        }

                        CompositeFuture.join(result).setHandler(res2 -> {
                            lock.release();
                            log.debug("Lock {} released", lockName);
                        });
                    }
                } catch (Throwable ex) {
                    log.error("Error while reconciling {} cluster", clusterDescription, ex);
                    lock.release();
                    log.debug("Lock {} released", lockName);
                }
            } else {
                log.warn("Failed to acquire lock for {} cluster {}.", clusterType, lockName);
            }
        });
    }

    /**
     * Reconcile cluster resources in the given namespace having the given selector.
     * Reconciliation works by getting the cluster ConfigMaps in the given namespace with the given selector and
     * comparing with the corresponding {@linkplain #getResources(String, Labels) resource}.
     * <ul>
     * <li>A cluster will be {@linkplain #createOrUpdate(String, String, Handler) created} for all ConfigMaps without same-named resources</li>
     * <li>A cluster will be {@linkplain #delete(String, String, Handler) deleted} for all resources without same-named ConfigMaps</li>
     * </ul>
     * @param namespace The namespace
     * @param selector The selector
     */
    public final void reconcileAll(String namespace, Labels selector) {
        Labels selectorWithCluster = selector.withType(clusterType);

        // get ConfigMap for the corresponding cluster type
        List<ConfigMap> cms = configMapOperations.list(namespace, selectorWithCluster);
        Set<String> cmsNames = cms.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toSet());

        // get resources for the corresponding cluster name (they are part of)
        List<R> resources = getResources(namespace, selectorWithCluster);
        Set<String> resourceNames = resources.stream().map(Labels::cluster).collect(Collectors.toSet());

        cmsNames.addAll(resourceNames);

        for (String name: cmsNames) {
            reconcileCluster(namespace, name);
        }
    }

    /**
     * Gets the resources in the given namespace and with the given labels
     * from which an AbstractCluster representing the current state of the cluster can be obtained.
     * @param namespace The namespace
     * @param selector The labels
     * @return The matching resources.
     */
    protected abstract List<R> getResources(String namespace, Labels selector);

}
