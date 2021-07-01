/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ConnectTimeoutException;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.AbstractKafkaConnectSpec;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectSpec;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.KafkaConnectorSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.status.Status;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.NoSuchResourceException;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.AbstractOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Operator;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.operator.common.model.ResourceVisitor;
import io.strimzi.operator.common.model.ValidationVisitor;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_RESTART;
import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_RESTART_TASK;
import static java.util.Collections.emptyMap;

@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:CyclomaticComplexity"})
public abstract class AbstractConnectOperator<C extends KubernetesClient, T extends CustomResource<P, S>,
        L extends CustomResourceList<T>, R extends Resource<T>, P extends AbstractKafkaConnectSpec, S extends KafkaConnectStatus>
        extends AbstractOperator<T, P, S, CrdOperator<C, T, L>> {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(AbstractConnectOperator.class.getName());

    private final CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> connectorOperator;
    private final Function<Vertx, KafkaConnectApi> connectClientProvider;
    protected final ImagePullPolicy imagePullPolicy;
    protected final ConfigMapOperator configMapOperations;
    protected final ClusterRoleBindingOperator clusterRoleBindingOperations;
    protected final ServiceOperator serviceOperations;
    protected final SecretOperator secretOperations;
    protected final PodDisruptionBudgetOperator podDisruptionBudgetOperator;
    protected final List<LocalObjectReference> imagePullSecrets;
    protected final long operationTimeoutMs;
    protected final String operatorNamespace;
    protected final Labels operatorNamespaceLabels;
    protected final PlatformFeaturesAvailability pfa;
    protected final ServiceAccountOperator serviceAccountOperations;
    private final int port;

    private Map<String, Counter> connectorsReconciliationsCounterMap;
    private Map<String, Counter> connectorsFailedReconciliationsCounterMap;
    private Map<String, Counter> connectorsSuccessfulReconciliationsCounterMap;
    private Map<String, AtomicInteger> connectorsResourceCounterMap;
    private Map<String, Timer> connectorsReconciliationsTimerMap;

    public AbstractConnectOperator(Vertx vertx, PlatformFeaturesAvailability pfa, String kind,
                                   CrdOperator<C, T, L> resourceOperator,
                                   ResourceOperatorSupplier supplier, ClusterOperatorConfig config,
                                   Function<Vertx, KafkaConnectApi> connectClientProvider,
                                   int port) {
        super(vertx, kind, resourceOperator, supplier.metricsProvider, config.getCustomResourceSelector());
        this.connectorOperator = supplier.kafkaConnectorOperator;
        this.connectClientProvider = connectClientProvider;
        this.configMapOperations = supplier.configMapOperations;
        this.clusterRoleBindingOperations = supplier.clusterRoleBindingOperator;
        this.serviceOperations = supplier.serviceOperations;
        this.secretOperations = supplier.secretOperations;
        this.serviceAccountOperations = supplier.serviceAccountOperations;
        this.podDisruptionBudgetOperator = supplier.podDisruptionBudgetOperator;
        this.imagePullPolicy = config.getImagePullPolicy();
        this.imagePullSecrets = config.getImagePullSecrets();
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.operatorNamespace = config.getOperatorNamespace();
        this.operatorNamespaceLabels = config.getOperatorNamespaceLabels();
        this.pfa = pfa;
        this.port = port;

        connectorsReconciliationsCounterMap = new ConcurrentHashMap<>();
        connectorsFailedReconciliationsCounterMap = new ConcurrentHashMap<>();
        connectorsSuccessfulReconciliationsCounterMap = new ConcurrentHashMap<>();
        connectorsResourceCounterMap = new ConcurrentHashMap<>();
        connectorsReconciliationsTimerMap = new ConcurrentHashMap<>();

    }

    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        // When deleting KafkaConnect we need to update the status of all selected KafkaConnector
        return connectorOperator.listAsync(reconciliation.namespace(), Labels.forStrimziCluster(reconciliation.name())).compose(connectors -> {
            List<Future> connectorFutures = new ArrayList<>();
            for (KafkaConnector connector : connectors) {
                connectorFutures.add(maybeUpdateConnectorStatus(reconciliation, connector, null,
                        noConnectCluster(reconciliation.namespace(), reconciliation.name())));
            }
            return CompositeFuture.join(connectorFutures);
        }).map(ignored -> Boolean.FALSE);
    }

    /**
     * Create a watch on {@code KafkaConnector} in the given {@code namespace}.
     * The watcher will:
     * <ul>
     * <li>{@linkplain #reconcileConnectors(Reconciliation, CustomResource, KafkaConnectStatus, boolean, String, OrderedProperties)} on the KafkaConnect
     * identified by {@code KafkaConnector.metadata.labels[strimzi.io/cluster]}.</li>
     * <li>The {@code KafkaConnector} status is updated with the result.</li>
     * </ul>
     * @param connectOperator The operator for {@code KafkaConnect}.
     * @param watchNamespaceOrWildcard The namespace to watch.
     * @param selectorLabels Selector labels for filtering the custom resources
     *
     * @return A future which completes when the watch has been set up.
     */
    public static Future<Void> createConnectorWatch(AbstractConnectOperator<KubernetesClient, KafkaConnect, KafkaConnectList, Resource<KafkaConnect>, KafkaConnectSpec, KafkaConnectStatus> connectOperator,
                                                    String watchNamespaceOrWildcard, Labels selectorLabels) {
        Optional<LabelSelector> selector = (selectorLabels == null || selectorLabels.toMap().isEmpty()) ? Optional.empty() : Optional.of(new LabelSelector(null, selectorLabels.toMap()));

        return Util.async(connectOperator.vertx, () -> {
            connectOperator.connectorOperator.watch(watchNamespaceOrWildcard, new Watcher<KafkaConnector>() {
                @Override
                public void eventReceived(Action action, KafkaConnector kafkaConnector) {
                    String connectorName = kafkaConnector.getMetadata().getName();
                    String connectorNamespace = kafkaConnector.getMetadata().getNamespace();
                    String connectorKind = kafkaConnector.getKind();
                    String connectName = kafkaConnector.getMetadata().getLabels() == null ? null : kafkaConnector.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
                    String connectNamespace = connectorNamespace;

                    switch (action) {
                        case ADDED:
                        case DELETED:
                        case MODIFIED:
                            Future<Void> f;
                            if (connectName != null) {
                                // Check whether a KafkaConnect exists
                                connectOperator.resourceOperator.getAsync(connectNamespace, connectName)
                                        .compose(connect -> {
                                            KafkaConnectApi apiClient = connectOperator.connectClientProvider.apply(connectOperator.vertx);
                                            if (connect == null) {
                                                Reconciliation r = new Reconciliation("connector-watch", connectOperator.kind(),
                                                        kafkaConnector.getMetadata().getNamespace(), connectName);
                                                updateStatus(r, noConnectCluster(connectNamespace, connectName), kafkaConnector, connectOperator.connectorOperator);
                                                LOGGER.infoCr(r, "{} {} in namespace {} was {}, but Connect cluster {} does not exist", connectorKind, connectorName, connectorNamespace, action, connectName);
                                                return Future.succeededFuture();
                                            } else {
                                                // grab the lock and call reconcileConnectors()
                                                // (i.e. short circuit doing a whole KafkaConnect reconciliation).
                                                Reconciliation reconciliation = new Reconciliation("connector-watch", connectOperator.kind(),
                                                        kafkaConnector.getMetadata().getNamespace(), connectName);

                                                if (!Util.matchesSelector(selector, connect))   {
                                                    LOGGER.debugCr(reconciliation, "{} {} in namespace {} was {}, but Connect cluster {} does not match label selector {} and will be ignored", connectorKind, connectorName, connectorNamespace, action, connectName, selectorLabels);
                                                    return Future.succeededFuture();
                                                } else if (connect.getSpec() != null && connect.getSpec().getReplicas() == 0)  {
                                                    LOGGER.infoCr(reconciliation, "{} {} in namespace {} was {}, but Connect cluster {} has 0 replicas", connectorKind, connectorName, connectorNamespace, action, connectName);
                                                    updateStatus(reconciliation, zeroReplicas(connectNamespace, connectName), kafkaConnector, connectOperator.connectorOperator);
                                                    return Future.succeededFuture();
                                                } else {
                                                    LOGGER.infoCr(reconciliation, "{} {} in namespace {} was {}", connectorKind, connectorName, connectorNamespace, action);

                                                    return connectOperator.withLock(reconciliation, LOCK_TIMEOUT_MS,
                                                        () -> connectOperator.reconcileConnectorAndHandleResult(reconciliation,
                                                                    KafkaConnectResources.qualifiedServiceName(connectName, connectNamespace), apiClient,
                                                                    isUseResources(connect),
                                                                    kafkaConnector.getMetadata().getName(), action == Action.DELETED ? null : kafkaConnector)
                                                                    .compose(reconcileResult -> {
                                                                        LOGGER.infoCr(reconciliation, "reconciled");
                                                                        return Future.succeededFuture(reconcileResult);
                                                                    }));
                                                }
                                            }
                                        });
                            } else {
                                updateStatus(new Reconciliation("connector-watch", connectOperator.kind(),
                                        kafkaConnector.getMetadata().getNamespace(), null),
                                        new InvalidResourceException("Resource lacks label '"
                                                + Labels.STRIMZI_CLUSTER_LABEL
                                                + "': No connect cluster in which to create this connector."),
                                        kafkaConnector, connectOperator.connectorOperator);
                            }

                            break;
                        case ERROR:
                            LOGGER.errorCr(new Reconciliation("connector-watch", connectorKind, connectName, connectorNamespace), "Failed {} {} in namespace {} ", connectorKind, connectorName, connectorNamespace);
                            break;
                        default:
                            LOGGER.errorCr(new Reconciliation("connector-watch", connectorKind, connectName, connectorNamespace), "Unknown action: {} {} in namespace {}", connectorKind, connectorName, connectorNamespace);
                    }
                }

                @Override
                public void onClose(WatcherException e) {
                    if (e != null) {
                        throw new KubernetesClientException(e.getMessage());
                    }
                }
            });
            return null;
        });
    }

    public static boolean isUseResources(HasMetadata connect) {
        return Annotations.booleanAnnotation(connect, Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, false);
    }

    private static NoSuchResourceException noConnectCluster(String connectNamespace, String connectName) {
        return new NoSuchResourceException(
                "KafkaConnect resource '" + connectName + "' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace " + connectNamespace + ".");
    }

    private static RuntimeException zeroReplicas(String connectNamespace, String connectName) {
        return new RuntimeException(
                "Kafka Connect cluster '" + connectName + "' in namespace " + connectNamespace + " has 0 replicas.");
    }

    /**
     * Reconcile all the connectors selected by the given connect instance, updated each connectors status with the result.
     * @param reconciliation The reconciliation
     * @param connect The connector
     * @param connectStatus Status of the KafkaConnect  resource (will be used to set the available
     *                      connector plugins)
     * @param scaledToZero  Indicated whether the related Connect cluster is currently scaled to 0 replicas
     * @return A future, failed if any of the connectors' statuses could not be updated.
     */
    protected Future<Void> reconcileConnectors(Reconciliation reconciliation, T connect, S connectStatus, boolean scaledToZero, String desiredLogging, OrderedProperties defaultLogging) {
        String connectName = connect.getMetadata().getName();
        String namespace = connect.getMetadata().getNamespace();
        String host = KafkaConnectResources.qualifiedServiceName(connectName, namespace);

        if (!isUseResources(connect))    {
            return Future.succeededFuture();
        }

        if (scaledToZero)   {
            return connectorOperator.listAsync(namespace, Optional.of(new LabelSelectorBuilder().addToMatchLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName).build()))
                    .compose(connectors -> CompositeFuture.join(
                            connectors.stream().map(connector -> maybeUpdateConnectorStatus(reconciliation, connector, null, zeroReplicas(namespace, connectName)))
                                    .collect(Collectors.toList())
                    ))
                    .map((Void) null);
        }

        KafkaConnectApi apiClient = connectClientProvider.apply(vertx);

        return CompositeFuture.join(
                apiClient.list(host, port),
                connectorOperator.listAsync(namespace, Optional.of(new LabelSelectorBuilder().addToMatchLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName).build())),
                apiClient.listConnectorPlugins(reconciliation, host, port),
                apiClient.updateConnectLoggers(reconciliation, host, port, desiredLogging, defaultLogging)
        ).compose(cf -> {
            List<String> runningConnectorNames = cf.resultAt(0);
            List<KafkaConnector> desiredConnectors = cf.resultAt(1);
            List<ConnectorPlugin> connectorPlugins = cf.resultAt(2);

            LOGGER.debugCr(reconciliation, "Setting list of connector plugins in Kafka Connect status");
            connectStatus.setConnectorPlugins(connectorPlugins);

            getConnectorsResourceCounter(namespace).set(desiredConnectors.size());

            Set<String> deleteConnectorNames = new HashSet<>(runningConnectorNames);
            deleteConnectorNames.removeAll(desiredConnectors.stream().map(c -> c.getMetadata().getName()).collect(Collectors.toSet()));
            LOGGER.debugCr(reconciliation, "{} cluster: delete connectors: {}", kind(), deleteConnectorNames);
            Stream<Future<Void>> deletionFutures = deleteConnectorNames.stream().map(connectorName ->
                    reconcileConnectorAndHandleResult(reconciliation, host, apiClient, true, connectorName, null)
            );

            LOGGER.debugCr(reconciliation, "{} cluster: required connectors: {}", kind(), desiredConnectors);
            Stream<Future<Void>> createUpdateFutures = desiredConnectors.stream()
                    .map(connector -> reconcileConnectorAndHandleResult(reconciliation, host, apiClient, true, connector.getMetadata().getName(), connector));

            return CompositeFuture.join(Stream.concat(deletionFutures, createUpdateFutures).collect(Collectors.toList())).map((Void) null);
        }).recover(error -> {
            if (error instanceof ConnectTimeoutException) {
                Promise<Void> connectorStatuses = Promise.promise();
                LOGGER.warnCr(reconciliation, "Failed to connect to the REST API => trying to update the connector status");

                connectorOperator.listAsync(namespace, Optional.of(new LabelSelectorBuilder().addToMatchLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName).build()))
                        .compose(connectors -> CompositeFuture.join(
                                connectors.stream().map(connector -> maybeUpdateConnectorStatus(reconciliation, connector, null, error))
                                        .collect(Collectors.toList())
                        ))
                        .onComplete(ignore -> connectorStatuses.fail(error));

                return connectorStatuses.future();
            } else {
                return Future.failedFuture(error);
            }
        });
    }

    protected KafkaConnectApi getKafkaConnectApi() {
        return connectClientProvider.apply(vertx);
    }

    /*test*/ Future<Void> reconcileConnectorAndHandleResult(Reconciliation reconciliation, String host, KafkaConnectApi apiClient,
                                             boolean useResources, String connectorName, KafkaConnector connector) {
        Promise<Void> reconciliationResult = Promise.promise();

        getConnectorsReconciliationsCounter(reconciliation.namespace()).increment();
        Timer.Sample connectorsReconciliationsTimerSample = Timer.start(metrics.meterRegistry());

        if (connector != null && Annotations.isReconciliationPausedWithAnnotation(connector)) {

            return maybeUpdateConnectorStatus(reconciliation, connector, null, null).compose(
                i -> {
                    connectorsReconciliationsTimerSample.stop(getConnectorsReconciliationsTimer(reconciliation.namespace()));
                    getConnectorsSuccessfulReconciliationsCounter(reconciliation.namespace()).increment();
                    return Future.succeededFuture();
                }
            );
        }

        reconcileConnector(reconciliation, host, apiClient, useResources, connectorName, connector)
                .onComplete(result -> {
                    connectorsReconciliationsTimerSample.stop(getConnectorsReconciliationsTimer(reconciliation.namespace()));

                    if (result.succeeded())    {
                        getConnectorsSuccessfulReconciliationsCounter(reconciliation.namespace()).increment();
                        reconciliationResult.complete();
                    } else {
                        getConnectorsFailedReconciliationsCounter(reconciliation.namespace()).increment();
                        reconciliationResult.fail(result.cause());
                    }
                });

        return reconciliationResult.future();
    }

    private Future<Void> reconcileConnector(Reconciliation reconciliation, String host, KafkaConnectApi apiClient,
                                             boolean useResources, String connectorName, KafkaConnector connector) {
        if (connector == null) {
            if (useResources) {
                LOGGER.infoCr(reconciliation, "deleting connector: {}", connectorName);
                return apiClient.delete(reconciliation, host, port, connectorName);
            } else {
                return Future.succeededFuture();
            }
        } else {
            LOGGER.infoCr(reconciliation, "creating/updating connector: {}", connectorName);
            if (connector.getSpec() == null) {
                return maybeUpdateConnectorStatus(reconciliation, connector, null,
                        new InvalidResourceException("spec property is required"));
            }
            if (!useResources) {
                return maybeUpdateConnectorStatus(reconciliation, connector, null,
                        new NoSuchResourceException(reconciliation.kind() + " " + reconciliation.name() + " is not configured with annotation " + Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES));
            } else {
                Promise<Void> promise = Promise.promise();
                maybeCreateOrUpdateConnector(reconciliation, host, apiClient, connectorName, connector.getSpec(), connector)
                        .onComplete(result -> {
                            if (result.succeeded()) {
                                maybeUpdateConnectorStatus(reconciliation, connector, result.result(), null)
                                    .onComplete(promise);
                            } else {
                                maybeUpdateConnectorStatus(reconciliation, connector, result.result(), result.cause())
                                    .onComplete(promise);
                            }
                        });
                return promise.future();
            }
        }
    }

    /**
     * Try to get the current connector config. If the connector does not exist, or its config differs from the 
     * {@code connectorSpec}'s, then call
     * {@link #createOrUpdateConnector(Reconciliation, String, KafkaConnectApi, String, KafkaConnectorSpec)}
     * otherwise, just return the connectors current state.
     * @param reconciliation The reconciliation.
     * @param host The REST API host.
     * @param apiClient The client instance.
     * @param connectorName The connector name.
     * @param connectorSpec The desired connector spec.
     * @param resource The resource that defines the connector.
     * @return A Future whose result, when successfully completed, is a ConnectorStatusAndConditions object containing the map of the current connector state plus any conditions that have arisen.
     */
    protected Future<ConnectorStatusAndConditions> maybeCreateOrUpdateConnector(Reconciliation reconciliation, String host, KafkaConnectApi apiClient,
                                                                                String connectorName, KafkaConnectorSpec connectorSpec, CustomResource resource) {
        return apiClient.getConnectorConfig(reconciliation, new BackOff(200L, 2, 6), host, port, connectorName).compose(
            config -> {
                if (!needsReconfiguring(reconciliation, connectorName, connectorSpec, config)) {
                    LOGGER.debugCr(reconciliation, "Connector {} exists and has desired config, {}=={}", connectorName, connectorSpec.getConfig(), config);
                    return apiClient.status(reconciliation, host, port, connectorName)
                        .compose(status -> pauseResume(reconciliation, host, apiClient, connectorName, connectorSpec, status))
                        .compose(ignored -> maybeRestartConnector(reconciliation, host, apiClient, connectorName, resource, new ArrayList<>()))
                        .compose(conditions -> maybeRestartConnectorTask(reconciliation, host, apiClient, connectorName, resource, conditions))
                        .compose(conditions ->
                            apiClient.statusWithBackOff(reconciliation, new BackOff(200L, 2, 10), host, port, connectorName)
                                .compose(createConnectorStatusAndConditions(conditions)))
                        .compose(status -> updateConnectorTopics(reconciliation, host, apiClient, connectorName, status));
                } else {
                    LOGGER.debugCr(reconciliation, "Connector {} exists but does not have desired config, {}!={}", connectorName, connectorSpec.getConfig(), config);
                    return createOrUpdateConnector(reconciliation, host, apiClient, connectorName, connectorSpec)
                        .compose(createConnectorStatusAndConditions())
                        .compose(status -> updateConnectorTopics(reconciliation, host, apiClient, connectorName, status));
                }
            },
            error -> {
                if (error instanceof ConnectRestException
                        && ((ConnectRestException) error).getStatusCode() == 404) {
                    LOGGER.debugCr(reconciliation, "Connector {} does not exist", connectorName);
                    return createOrUpdateConnector(reconciliation, host, apiClient, connectorName, connectorSpec)
                        .compose(createConnectorStatusAndConditions())
                        .compose(status -> updateConnectorTopics(reconciliation, host, apiClient, connectorName, status));
                } else {
                    return Future.failedFuture(error);
                }
            });
    }

    private boolean needsReconfiguring(Reconciliation reconciliation, String connectorName,
                                       KafkaConnectorSpec connectorSpec,
                                       Map<String, String> actual) {
        Map<String, String> desired = new HashMap<>(connectorSpec.getConfig().size());
        // The actual which comes from Connect API includes tasks.max, connector.class and name,
        // which connectorSpec.getConfig() does not
        if (connectorSpec.getTasksMax() != null) {
            desired.put("tasks.max", connectorSpec.getTasksMax().toString());
        }
        desired.put("name", connectorName);
        desired.put("connector.class", connectorSpec.getClassName());
        for (Map.Entry<String, Object> entry : connectorSpec.getConfig().entrySet()) {
            desired.put(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debugCr(reconciliation, "Desired: {}", new TreeMap<>(desired));
            LOGGER.debugCr(reconciliation, "Actual:  {}", new TreeMap<>(actual));
        }
        return !desired.equals(actual);
    }

    protected Future<Map<String, Object>> createOrUpdateConnector(Reconciliation reconciliation, String host, KafkaConnectApi apiClient,
                                                                  String connectorName, KafkaConnectorSpec connectorSpec) {
        return apiClient.createOrUpdatePutRequest(reconciliation, host, port, connectorName, asJson(reconciliation, connectorSpec))
            .compose(ignored -> apiClient.statusWithBackOff(reconciliation, new BackOff(200L, 2, 10), host, port,
                    connectorName))
            .compose(status -> pauseResume(reconciliation, host, apiClient, connectorName, connectorSpec, status))
            .compose(ignored ->  apiClient.status(reconciliation, host, port, connectorName));
    }

    private Future<Void> pauseResume(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, String connectorName, KafkaConnectorSpec connectorSpec, Map<String, Object> status) {
        Object path = ((Map) status.getOrDefault("connector", emptyMap())).get("state");
        if (!(path instanceof String)) {
            return Future.failedFuture("JSON response lacked $.connector.state");
        } else {
            String state = (String) path;
            boolean shouldPause = Boolean.TRUE.equals(connectorSpec.getPause());
            if ("RUNNING".equals(state) && shouldPause) {
                LOGGER.debugCr(reconciliation, "Pausing connector {}", connectorName);
                return apiClient.pause(host, port, connectorName);
            } else if ("PAUSED".equals(state) && !shouldPause) {
                LOGGER.debugCr(reconciliation, "Resuming connector {}", connectorName);
                return apiClient.resume(host, port, connectorName);
            } else {
                return Future.succeededFuture();
            }
        }
    }

    private Future<List<Condition>> maybeRestartConnector(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, String connectorName, CustomResource resource, List<Condition> conditions) {
        if (hasRestartAnnotation(resource, connectorName)) {
            LOGGER.debugCr(reconciliation, "Restarting connector {}", connectorName);
            return apiClient.restart(host, port, connectorName)
                    .compose(ignored -> removeRestartAnnotation(reconciliation, resource)
                        .compose(v -> Future.succeededFuture(conditions)),
                        throwable -> {
                            // Ignore restart failures - add a warning and try again on the next reconcile
                            String message = "Failed to restart connector " + connectorName + ". " + throwable.getMessage();
                            LOGGER.warnCr(reconciliation, message);
                            conditions.add(StatusUtils.buildWarningCondition("RestartConnector", message));
                            return Future.succeededFuture(conditions);
                        });
        } else {
            return Future.succeededFuture(conditions);
        }
    }

    private Future<List<Condition>> maybeRestartConnectorTask(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, String connectorName, CustomResource resource, List<Condition> conditions) {
        int taskID = getRestartTaskAnnotationTaskID(resource, connectorName);
        if (taskID >= 0) {
            LOGGER.debugCr(reconciliation, "Restarting connector task {}:{}", connectorName, taskID);
            return apiClient.restartTask(host, port, connectorName, taskID)
                    .compose(ignored -> removeRestartTaskAnnotation(reconciliation, resource)
                        .compose(v -> Future.succeededFuture(conditions)),
                        throwable -> {
                            // Ignore restart failures - add a warning and try again on the next reconcile
                            String message = "Failed to restart connector task " + connectorName + ":" + taskID + ". " + throwable.getMessage();
                            LOGGER.warnCr(reconciliation, message);
                            conditions.add(StatusUtils.buildWarningCondition("RestartConnectorTask", message));
                            return Future.succeededFuture(conditions);
                        });
        } else {
            return Future.succeededFuture(conditions);
        }
    }

    private Future<ConnectorStatusAndConditions> updateConnectorTopics(Reconciliation reconciliation, String host, KafkaConnectApi apiClient, String connectorName, ConnectorStatusAndConditions status) {
        return apiClient.getConnectorTopics(reconciliation, host, port, connectorName)
            .compose(updateConnectorStatusAndConditions(status));
    }

    /**
     * Whether the provided resource instance is a KafkaConnector and has the strimzi.io/restart annotation
     *
     * @param resource resource instance to check
     * @param connectorName connectorName name of the connector to check
     * @return true if the provided resource instance has the strimzi.io/restart annotation; false otherwise
     */
    protected boolean hasRestartAnnotation(CustomResource resource, String connectorName) {
        return Annotations.booleanAnnotation(resource, ANNO_STRIMZI_IO_RESTART, false);
    }

    /**
     * Return the ID of the connector task to be restarted if the provided KafkaConnector resource instance has the strimzio.io/restart-task annotation
     *
     * @param resource resource instance to check
     * @param connectorName KafkaConnector resource instance to check
     * @return the ID of the task to be restarted if the provided KafkaConnector resource instance has the strimzio.io/restart-task annotation or -1 otherwise.
     */
    protected int getRestartTaskAnnotationTaskID(CustomResource resource, String connectorName) {
        return Annotations.intAnnotation(resource, ANNO_STRIMZI_IO_RESTART_TASK, -1);
    }

    /**
     * Patches the KafkaConnector CR to remove the strimzi.io/restart annotation, as
     * the restart action specified by the user has been completed.
     */
    protected Future<Void> removeRestartAnnotation(Reconciliation reconciliation, CustomResource resource) {
        return removeAnnotation(reconciliation, (KafkaConnector) resource, ANNO_STRIMZI_IO_RESTART);
    }

    /**
     * Patches the KafkaConnector CR to remove the strimzi.io/restart-task annotation, as
     * the restart action specified by the user has been completed.
     */
    protected Future<Void> removeRestartTaskAnnotation(Reconciliation reconciliation, CustomResource resource) {
        return removeAnnotation(reconciliation, (KafkaConnector) resource, ANNO_STRIMZI_IO_RESTART_TASK);
    }

    /**
     * Patches the KafkaConnector CR to remove the supplied annotation.
     */
    private Future<Void> removeAnnotation(Reconciliation reconciliation, KafkaConnector resource, String annotationKey) {
        LOGGER.debugCr(reconciliation, "Removing annotation {}", annotationKey);
        KafkaConnector patchedKafkaConnector = new KafkaConnectorBuilder(resource)
            .editMetadata()
            .removeFromAnnotations(annotationKey)
            .endMetadata()
            .build();
        return connectorOperator.patchAsync(reconciliation, patchedKafkaConnector)
            .compose(ignored -> Future.succeededFuture());
    }

    public static void updateStatus(Reconciliation reconciliation, Throwable error, KafkaConnector kafkaConnector2, CrdOperator<?, KafkaConnector, ?> connectorOperations) {
        KafkaConnectorStatus status = new KafkaConnectorStatus();
        StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnector2, status, error);
        StatusDiff diff = new StatusDiff(kafkaConnector2.getStatus(), status);
        if (!diff.isEmpty()) {
            KafkaConnector copy = new KafkaConnectorBuilder(kafkaConnector2).build();
            copy.setStatus(status);
            connectorOperations.updateStatusAsync(reconciliation, copy);
        }
    }

    protected class ConnectorStatusAndConditions {
        Map<String, Object> statusResult;
        List<String> topics;
        List<Condition> conditions;

        ConnectorStatusAndConditions(Map<String, Object> statusResult, List<String> topics, List<Condition> conditions) {
            this.statusResult = statusResult;
            this.topics = topics;
            this.conditions = conditions;
        }

        ConnectorStatusAndConditions(Map<String, Object> statusResult, List<Condition> conditions) {
            this(statusResult, Collections.emptyList(), conditions);
        }

        ConnectorStatusAndConditions(Map<String, Object> statusResult) {
            this(statusResult, Collections.emptyList(), Collections.emptyList());
        }
    }

    Function<Map<String, Object>, Future<ConnectorStatusAndConditions>> createConnectorStatusAndConditions() {
        return statusResult -> Future.succeededFuture(new ConnectorStatusAndConditions(statusResult));
    }

    Function<Map<String, Object>, Future<ConnectorStatusAndConditions>> createConnectorStatusAndConditions(List<Condition> conditions) {
        return statusResult -> Future.succeededFuture(new ConnectorStatusAndConditions(statusResult, conditions));
    }

    Function<List<String>, Future<ConnectorStatusAndConditions>> updateConnectorStatusAndConditions(ConnectorStatusAndConditions status) {
        return topics -> Future.succeededFuture(new ConnectorStatusAndConditions(status.statusResult, topics, status.conditions));
    }

    public Set<Condition> validate(Reconciliation reconciliation, KafkaConnector resource) {
        if (resource != null) {
            Set<Condition> warningConditions = new LinkedHashSet<>(0);

            ResourceVisitor.visit(reconciliation, resource, new ValidationVisitor(resource, LOGGER, warningConditions));

            return warningConditions;
        }

        return Collections.emptySet();
    }

    Future<Void> maybeUpdateConnectorStatus(Reconciliation reconciliation, KafkaConnector connector, ConnectorStatusAndConditions connectorStatus, Throwable error) {
        KafkaConnectorStatus status = new KafkaConnectorStatus();
        if (error != null) {
            LOGGER.warnCr(reconciliation, "Error reconciling connector {}", connector.getMetadata().getName(), error);
        }

        Map<String, Object> statusResult = null;
        List<String> topics = new ArrayList<>();
        List<Condition> conditions = new ArrayList<>();

        if (connectorStatus != null) {
            statusResult = connectorStatus.statusResult;
            topics = connectorStatus.topics.stream().sorted().collect(Collectors.toList());
            connectorStatus.conditions.forEach(condition -> conditions.add(condition));
        }

        Set<Condition> unknownAndDeprecatedConditions = validate(reconciliation, connector);
        unknownAndDeprecatedConditions.forEach(condition -> conditions.add(condition));

        if (!Annotations.isReconciliationPausedWithAnnotation(connector)) {
            StatusUtils.setStatusConditionAndObservedGeneration(connector, status, error != null ? Future.failedFuture(error) : Future.succeededFuture());
            status.setConnectorStatus(statusResult);
            status.setTasksMax(getActualTaskCount(connector, statusResult));
            status.setTopics(topics);
        } else {
            status.setObservedGeneration(connector.getStatus() != null ? connector.getStatus().getObservedGeneration() : 0);
            conditions.add(StatusUtils.getPausedCondition());
        }
        status.addConditions(conditions);

        return maybeUpdateStatusCommon(connectorOperator, connector, reconciliation, status,
            (connector1, status1) -> {
                return new KafkaConnectorBuilder(connector1).withStatus(status1).build();
            });
    }

    /**
     * The tasksMax are mirrored in the KafkaConnector.status where they are used by the scale subresource.
     * However, .spec.tasksMax is not always set and has no default value in Strimzi (only in Kafka Connect). So when
     * it is not set, we try to count the tasks from the status. And if these are missing as well, we just set it to 0.
     *
     * @param connector         The KafkaConnector instance of the reconciled connector
     * @param statusResult      The status from the Connect REST API
     * @return                  Number of tasks which should be set in the status
     */
    protected int getActualTaskCount(KafkaConnector connector, Map<String, Object> statusResult)  {
        if (connector.getSpec() != null
                && connector.getSpec().getTasksMax() != null)  {
            return connector.getSpec().getTasksMax();
        } else if (statusResult != null
                && statusResult.containsKey("tasks")
                && statusResult.get("tasks") instanceof List) {
            return ((List) statusResult.get("tasks")).size();
        } else {
            return 0;
        }
    }

    protected JsonObject asJson(Reconciliation reconciliation, KafkaConnectorSpec spec) {
        JsonObject connectorConfigJson = new JsonObject();
        if (spec.getConfig() != null) {
            for (Map.Entry<String, Object> cf : spec.getConfig().entrySet()) {
                String name = cf.getKey();
                if ("connector.class".equals(name)
                        || "tasks.max".equals(name)) {
                    // TODO include resource namespace and name in this message
                    LOGGER.warnCr(reconciliation, "Configuration parameter {} in KafkaConnector.spec.config will be ignored and the value from KafkaConnector.spec will be used instead",
                            name);
                }
                connectorConfigJson.put(name, cf.getValue());
            }
        }

        if (spec.getTasksMax() != null) {
            connectorConfigJson.put("tasks.max", spec.getTasksMax());
        }

        return connectorConfigJson.put("connector.class", spec.getClassName());
    }

    /**
     * Updates the Status field of the KafkaConnect or KafkaConnector CR. It diffs the desired status against the current status and calls
     * the update only when there is any difference in non-timestamp fields.
     *
     * @param resource The CR of KafkaConnect or KafkaConnector
     * @param reconciliation Reconciliation information
     * @param desiredStatus The KafkaConnectStatus or KafkaConnectorStatus which should be set
     *
     * @return
     */
    protected <T extends CustomResource<?, S>, S extends Status, L extends CustomResourceList<T>> Future<Void>
        maybeUpdateStatusCommon(CrdOperator<KubernetesClient, T, L> resourceOperator,
                                T resource,
                                Reconciliation reconciliation,
                                S desiredStatus,
                                BiFunction<T, S, T> copyWithStatus) {
        Promise<Void> updateStatusPromise = Promise.promise();

        resourceOperator.getAsync(resource.getMetadata().getNamespace(), resource.getMetadata().getName()).onComplete(getRes -> {
            if (getRes.succeeded()) {
                T fetchedResource = getRes.result();

                if (fetchedResource != null) {
                    if ((!(fetchedResource instanceof KafkaConnector))
                            && (!(fetchedResource instanceof KafkaMirrorMaker2))
                            && StatusUtils.isResourceV1alpha1(fetchedResource)) {
                        LOGGER.warnCr(reconciliation, "{} {} needs to be upgraded from version {} to 'v1beta1' to use the status field",
                                fetchedResource.getKind(), fetchedResource.getMetadata().getName(), fetchedResource.getApiVersion());
                        updateStatusPromise.complete();
                    } else {
                        S currentStatus = fetchedResource.getStatus();

                        StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                        if (!ksDiff.isEmpty()) {
                            T resourceWithNewStatus = copyWithStatus.apply(fetchedResource, desiredStatus);

                            resourceOperator.updateStatusAsync(reconciliation, resourceWithNewStatus).onComplete(updateRes -> {
                                if (updateRes.succeeded()) {
                                    LOGGER.debugCr(reconciliation, "Completed status update");
                                    updateStatusPromise.complete();
                                } else {
                                    LOGGER.errorCr(reconciliation, "Failed to update status", updateRes.cause());
                                    updateStatusPromise.fail(updateRes.cause());
                                }
                            });
                        } else {
                            LOGGER.debugCr(reconciliation, "Status did not change");
                            updateStatusPromise.complete();
                        }
                    }
                } else {
                    LOGGER.errorCr(reconciliation, "Current {} resource not found", resource.getKind());
                    updateStatusPromise.fail("Current " + resource.getKind() + " resource not found");
                }
            } else {
                LOGGER.errorCr(reconciliation, "Failed to get the current {} resource and its status", resource.getKind(), getRes.cause());
                updateStatusPromise.fail(getRes.cause());
            }
        });

        return updateStatusPromise.future();
    }

    Future<ReconcileResult<Secret>> kafkaConnectJmxSecret(Reconciliation reconciliation, String namespace, String name, KafkaConnectCluster connectCluster) {
        if (connectCluster.isJmxAuthenticated()) {
            Future<Secret> secretFuture = secretOperations.getAsync(namespace, KafkaConnectCluster.jmxSecretName(name));
            return secretFuture.compose(res -> {
                if (res == null) {
                    return secretOperations.reconcile(reconciliation, namespace, KafkaConnectCluster.jmxSecretName(name), connectCluster.generateJmxSecret());
                }
                return Future.succeededFuture(ReconcileResult.noop(res));
            });
        }
        return secretOperations.reconcile(reconciliation, namespace, KafkaConnectCluster.jmxSecretName(name), null);
    }

    public Counter getConnectorsReconciliationsCounter(String namespace) {
        return Operator.getCounter(namespace, KafkaConnector.RESOURCE_KIND, metrics, null, connectorsReconciliationsCounterMap,
                METRICS_PREFIX + "reconciliations",
                "Number of reconciliations done by the operator for individual resources");
    }

    public Counter getConnectorsFailedReconciliationsCounter(String namespace) {
        return Operator.getCounter(namespace, KafkaConnector.RESOURCE_KIND, metrics, null, connectorsFailedReconciliationsCounterMap,
                METRICS_PREFIX + "reconciliations.failed",
                "Number of reconciliations done by the operator for individual resources which failed");
    }

    public Counter getConnectorsSuccessfulReconciliationsCounter(String namespace) {
        return Operator.getCounter(namespace, KafkaConnector.RESOURCE_KIND, metrics, null, connectorsSuccessfulReconciliationsCounterMap,
                METRICS_PREFIX + "reconciliations.successful",
                "Number of reconciliations done by the operator for individual resources which were successful");
    }

    public AtomicInteger getConnectorsResourceCounter(String namespace) {
        return Operator.getGauge(namespace, KafkaConnector.RESOURCE_KIND, metrics, null, connectorsResourceCounterMap,
                METRICS_PREFIX + "resources",
                "Number of custom resources the operator sees");
    }

    public Timer getConnectorsReconciliationsTimer(String namespace) {
        return Operator.getTimer(namespace, KafkaConnector.RESOURCE_KIND, metrics, null, connectorsReconciliationsTimerMap,
                METRICS_PREFIX + "reconciliations.duration",
                "The time the reconciliation takes to complete");
    }

}
