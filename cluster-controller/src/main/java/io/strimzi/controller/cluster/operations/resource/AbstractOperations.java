/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Abstract resource creation, for a generic resource type {@code R}.
 * This class applies the template method pattern, first checking whether the resource exists,
 * and creating it if it does not. It is not an error if the resource did already exist.
 * @param <C> The type of client used to interact with kubernetes.
 * @param <T> The Kubernetes resource type.
 * @param <L> The list variant of the Kubernetes resource type.
 * @param <D> The doneable variant of the Kubernetes resource type.
 * @param <R> The resource operations.
 */
public abstract class AbstractOperations<C, T extends HasMetadata, L extends KubernetesResourceList/*<T>*/, D, R extends Resource<T, D>> {

    private static final Logger log = LoggerFactory.getLogger(AbstractOperations.class);
    protected final Vertx vertx;
    protected final C client;
    private final String resourceKind;

    /**
     * Constructor.
     * @param vertx The vertx instance.
     * @param client The kubernetes client.
     * @param resourceKind The mind of Kubernetes resource (used for logging).
     */
    public AbstractOperations(Vertx vertx, C client, String resourceKind) {
        this.vertx = vertx;
        this.client = client;
        this.resourceKind = resourceKind;
    }

    protected abstract MixedOperation<T, L, D, R> operation();

    /**
     * Asynchronously create the given {@code resource} if it doesn't already exists,
     * calling the given {@code handler} with the outcome.
     * @param resource The resource to create.
     */
    public Future<Void> create(T resource) {
        Future<Void> fut = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    if (operation().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).get() == null) {
                        try {
                            log.info("Creating {} {}", resourceKind, resource);
                            operation().createOrReplace(resource);
                            future.complete();
                        } catch (Exception e) {
                            log.error("Caught exception while creating {}", resourceKind, e);
                            future.fail(e);
                        }
                    }
                    else {
                        log.warn("{} {} already exists", resourceKind, resource);
                        future.complete();
                    }
                },
                false,
                fut.completer()
        );
        return fut;
    }

    /**
     * Asynchronously delete the resource with the given {@code name} in the given {@code namespace},
     * calling the given {@code handler} with the outcome.
     * @param namespace The namespace of the resource to delete.
     * @param name The name of the resource to delete.
     * @param handler A handler for the outcome.
     */
    public void delete(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {

                    if (operation().inNamespace(namespace).withName(name).get() != null) {
                        try {
                            log.info("Deleting {} {} in namespace {}", resourceKind, name, namespace);
                            operation().inNamespace(namespace).withName(name).delete();
                            future.complete();
                        } catch (Exception e) {
                            log.error("Caught exception while deleting {}", resourceKind, e);
                            future.fail(e);
                        }
                    } else {
                        log.warn("{} {} in namespace {} doesn't exist", resourceKind, name, namespace);
                        future.complete();
                    }
                }, false,
                res -> {
                    if (res.succeeded()) {
                        log.info("{} {} in namespace {} has been deleted (or no longer exists)", resourceKind, name, namespace);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("{} deletion failed:", resourceKind, res.cause());
                        handler.handle(Future.failedFuture(res.cause()));
                    }
                }
        );
    }

    /**
     * Asynchronously patch the resource with the given {@code name} in the given {@code namespace}
     * with reflect the state given in the {@code patch}, calling the handler with the outcome.
     * @param namespace The namespace of the resource to patch.
     * @param name The name of the resource to patch.
     * @param patch The desired state of the resource.
     * @param handler The handler for the outcome.
     */
    public void patch(String namespace, String name, T patch, Handler<AsyncResult<Void>> handler) {
        patch(namespace, name, true, patch, handler);
    }

    public void patch(String namespace, String name, boolean cascading, T patch, Handler<AsyncResult<Void>> handler) {
        vertx./*createSharedWorkerExecutor("kubernetes-ops-pool").*/executeBlocking(
                future -> {
                    try {
                        log.info("Patching resource with {}", patch);
                        operation().inNamespace(namespace).withName(name).cascading(cascading).patch(patch);
                        future.complete();
                    }
                    catch (Exception e) {
                        log.error("Caught exception while patching", e);
                        future.fail(e);
                    }
                },
                true,
                res -> {
                    if (res.succeeded()) {
                        log.info("Resource has been patched", patch);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("Failed to patch resource: {}", res.cause().toString());
                        handler.handle(Future.failedFuture(res.cause()));
                    }
                }
        );
    }

    /**
     * Synchronously gets the resource with the given {@code name} in the given {@code namespace}.
     * @param namespace The namespace.
     * @param name The name.
     * @return The resource, or null if it doesn't exist.
     */
    public final T get(String namespace, String name) {
        return operation().inNamespace(namespace).withName(name).get();
    }

    /**
     * Synchronously list the resources in the given {@code namespace} with the given {@code labels}.
     * @param namespace The namespace.
     * @param labels The labels.
     * @return A list of matching resources.
     */
    public List<T> list(String namespace, Map<String, String> labels) {
        return operation().inNamespace(namespace).withLabels(labels).list().getItems();
    }

}
