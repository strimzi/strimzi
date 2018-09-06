/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaAssemblyList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.SecretCertProvider;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.EntityOperator;
import io.strimzi.operator.cluster.model.EntityTopicOperator;
import io.strimzi.operator.cluster.model.EntityUserOperator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.TopicOperator;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.ZookeeperSetOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;

import io.fabric8.openshift.api.model.Route;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static io.strimzi.operator.cluster.model.ModelUtils.findSecretWithName;

/**
 * <p>Assembly operator for a "Kafka" assembly, which manages:</p>
 * <ul>
 *     <li>A ZooKeeper cluster StatefulSet and related Services</li>
 *     <li>A Kafka cluster StatefulSet and related Services</li>
 *     <li>Optionally, a TopicOperator Deployment</li>
 * </ul>
 */
public class KafkaAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, Kafka, KafkaAssemblyList, DoneableKafka, Resource<Kafka, DoneableKafka>> {
    private static final Logger log = LogManager.getLogger(KafkaAssemblyOperator.class.getName());

    private final long operationTimeoutMs;

    private final ZookeeperSetOperator zkSetOperations;
    private final KafkaSetOperator kafkaSetOperations;
    private final ServiceOperator serviceOperations;
    private final RouteOperator routeOperations;
    private final PvcOperator pvcOperations;
    private final DeploymentOperator deploymentOperations;
    private final ConfigMapOperator configMapOperations;
    private final ServiceAccountOperator serviceAccountOperator;
    private final RoleBindingOperator roleBindingOperator;
    private final ClusterRoleBindingOperator clusterRoleBindingOperator;

    /**
     * @param vertx The Vertx instance
     * @param isOpenShift Whether we're running with OpenShift
     */
    public KafkaAssemblyOperator(Vertx vertx, boolean isOpenShift,
                                 long operationTimeoutMs,
                                 CertManager certManager,
                                 ResourceOperatorSupplier supplier) {
        super(vertx, isOpenShift, ResourceType.KAFKA, certManager, supplier.kafkaOperator, supplier.secretOperations, supplier.networkPolicyOperator);
        this.operationTimeoutMs = operationTimeoutMs;
        this.serviceOperations = supplier.serviceOperations;
        this.routeOperations = supplier.routeOperations;
        this.zkSetOperations = supplier.zkSetOperations;
        this.kafkaSetOperations = supplier.kafkaSetOperations;
        this.configMapOperations = supplier.configMapOperations;
        this.pvcOperations = supplier.pvcOperations;
        this.deploymentOperations = supplier.deploymentOperations;
        this.serviceAccountOperator = supplier.serviceAccountOperator;
        this.roleBindingOperator = supplier.roleBindingOperator;
        this.clusterRoleBindingOperator = supplier.clusterRoleBindingOperator;
    }

    @Override
    public Future<Void> createOrUpdate(Reconciliation reconciliation, Kafka kafkaAssembly) {
        Future<Void> chainFuture = Future.future();
        new ReconciliationState(reconciliation, kafkaAssembly)
                .reconcileClusterCa()
                .compose(state -> state.getZookeeperState())
                .compose(state -> state.zkScaleDown())
                .compose(state -> state.zkService())
                .compose(state -> state.zkHeadlessService())
                .compose(state -> state.zkAncillaryCm())
                .compose(state -> state.zkNodesSecret())
                .compose(state -> state.zkNetPolicy())
                .compose(state -> state.zkStatefulSet())
                .compose(state -> state.zkRollingUpdate())
                .compose(state -> state.zkScaleUp())
                .compose(state -> state.zkServiceEndpointReadiness())
                .compose(state -> state.zkHeadlessServiceEndpointReadiness())

                .compose(state -> state.getKafkaClusterDescription())
                .compose(state -> state.kafkaInitServiceAccount())
                .compose(state -> state.kafkaInitClusterRoleBinding())
                .compose(state -> state.kafkaScaleDown())
                .compose(state -> state.kafkaService())
                .compose(state -> state.kafkaHeadlessService())
                .compose(state -> state.kafkaReplicaServices())
                .compose(state -> state.kafkaBootstrapRoute())
                .compose(state -> state.kafkaReplicaRoutes())
                .compose(state -> state.kafkaBootstrapRouteReady())
                .compose(state -> state.kafkaReplicaRoutesReady())
                .compose(state -> state.kafkaAncillaryCm())
                .compose(state -> state.kafkaClientsCaSecret())
                .compose(state -> state.kafkaClientsPublicKeySecret())
                .compose(state -> state.kafkaClusterPublicKeySecret())
                .compose(state -> state.kafkaBrokersSecret())
                .compose(state -> state.kafkaNetPolicy())
                .compose(state -> state.kafkaStatefulSet())
                .compose(state -> state.kafkaRollingUpdate())
                .compose(state -> state.kafkaScaleUp())
                .compose(state -> state.kafkaServiceEndpointReady())
                .compose(state -> state.kafkaHeadlessServiceEndpointReady())

                .compose(state -> state.getTopicOperatorDescription())
                .compose(state -> state.topicOperatorServiceAccount())
                .compose(state -> state.topicOperatorRoleBinding())
                .compose(state -> state.topicOperatorAncillaryCm())
                .compose(state -> state.topicOperatorDeployment())
                .compose(state -> state.topicOperatorSecret())

                .compose(state -> state.getEntityOperatorDescription())
                .compose(state -> state.entityOperatorServiceAccount())
                .compose(state -> state.entityOperatorTopicOpRoleBinding())
                .compose(state -> state.entityOperatorUserOpRoleBinding())
                .compose(state -> state.entityOperatorTopicOpAncillaryCm())
                .compose(state -> state.entityOperatorUserOpAncillaryCm())
                .compose(state -> state.entityOperatorDeployment())
                .compose(state -> state.entityOperatorSecret())
                .compose(state -> chainFuture.complete(), chainFuture);

        return chainFuture;
    }

    /**
     * Hold the mutable state during a reconciliation
     */
    private class ReconciliationState {

        private final String namespace;
        private final String name;
        private final Kafka kafkaAssembly;
        private final Reconciliation reconciliation;

        private List<Secret> assemblySecrets;

        private ZookeeperCluster zkCluster;
        private Service zkService;
        private Service zkHeadlessService;
        private ConfigMap zkMetricsAndLogsConfigMap;
        private ReconcileResult<StatefulSet> zkDiffs;
        private boolean zkForcedRestart;

        private KafkaCluster kafkaCluster = null;
        private Service kafkaService;
        private Service kafkaHeadlessService;
        private ConfigMap kafkaMetricsAndLogsConfigMap;
        private ReconcileResult<StatefulSet> kafkaDiffs;
        private boolean kafkaForcedRestart;
        private String kafkaExternalBootstrapAddress;
        private Map<String, String> kafkaExternalAddresses = new TreeMap<>();

        private TopicOperator topicOperator;
        private Deployment toDeployment = null;
        private ConfigMap toMetricsAndLogsConfigMap = null;

        private EntityOperator entityOperator;
        private Deployment eoDeployment = null;
        private ConfigMap topicOperatorMetricsAndLogsConfigMap = null;
        private ConfigMap userOperatorMetricsAndLogsConfigMap;

        ReconciliationState(Reconciliation reconciliation, Kafka kafkaAssembly) {
            this.reconciliation = reconciliation;
            this.kafkaAssembly = kafkaAssembly;
            this.namespace = kafkaAssembly.getMetadata().getNamespace();
            this.name = kafkaAssembly.getMetadata().getName();
        }

        Future<ReconciliationState> reconcileClusterCa() {
            Labels caLabels = Labels.userLabels(kafkaAssembly.getMetadata().getLabels()).withKind(reconciliation.type().toString()).withCluster(reconciliation.name());
            Future<ReconciliationState> result = Future.future();
            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    String clusterCaName = AbstractModel.getClusterCaName(reconciliation.name());
                    List<Secret> clusterSecrets = secretOperations.list(reconciliation.namespace(), Labels.forCluster(reconciliation.name()));
                    Secret clusterCa = findSecretWithName(clusterSecrets, clusterCaName);
                    SecretCertProvider secretCertProvider = new SecretCertProvider();
                    final Secret secret;

                    if (clusterCa == null) {
                        log.debug("{}: Generating cluster CA certificate {}", reconciliation, clusterCaName);
                        File clusterCAkeyFile = null;
                        File clusterCAcertFile = null;
                        try {
                            clusterCAkeyFile = File.createTempFile("tls", "cluster-ca-key");
                            clusterCAcertFile = File.createTempFile("tls", "cluster-ca-cert");

                            Subject sbj = new Subject();
                            sbj.setOrganizationName("io.strimzi");
                            sbj.setCommonName("cluster-ca");

                            certManager.generateSelfSignedCert(clusterCAkeyFile, clusterCAcertFile, sbj, CERTS_EXPIRATION_DAYS);

                            secret = secretCertProvider.createSecret(reconciliation.namespace(), clusterCaName,
                                    "cluster-ca.key", "cluster-ca.crt",
                                    clusterCAkeyFile, clusterCAcertFile, caLabels.toMap());
                        } catch (Throwable e) {
                            future.fail(e);
                            return;
                        } finally {
                            if (clusterCAkeyFile != null)
                                clusterCAkeyFile.delete();
                            if (clusterCAcertFile != null)
                                clusterCAcertFile.delete();
                        }
                        log.debug("{}: End generating cluster CA {}", reconciliation, clusterCaName);
                    } else {
                        log.debug("{}: The cluster CA {} already exists", reconciliation, clusterCaName);
                        secret = secretCertProvider.createSecret(reconciliation.namespace(), clusterCaName,
                                clusterCa.getData(), caLabels.toMap());
                    }

                    secretOperations.reconcile(reconciliation.namespace(), clusterCaName, secret)
                            .compose(x -> {
                                clusterSecrets.add(secret);
                                this.assemblySecrets = clusterSecrets;
                                future.complete(this);
                            }, future);
                }, true,
                result.completer()
            );
            return result;
        }

        Future<ReconciliationState> getZookeeperState() {
            Future<ReconciliationState> fut = Future.future();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    try {
                        this.zkCluster = ZookeeperCluster.fromCrd(certManager, kafkaAssembly, assemblySecrets);

                        ConfigMap logAndMetricsConfigMap = zkCluster.generateMetricsAndLogConfigMap(zkCluster.getLogging() instanceof ExternalLogging ?
                                configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) zkCluster.getLogging()).getName()) :
                                null);

                        this.zkService = zkCluster.generateService();
                        this.zkHeadlessService = zkCluster.generateHeadlessService();
                        this.zkMetricsAndLogsConfigMap = zkCluster.generateMetricsAndLogConfigMap(logAndMetricsConfigMap);

                        future.complete(this);
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                }, true,
                res -> {
                    if (res.succeeded()) {
                        fut.complete((ReconciliationState) res.result());
                    } else {
                        fut.fail(res.cause());
                    }
                }
            );

            return fut;
        }

        Future<ReconciliationState> withZkDiff(Future<ReconcileResult<StatefulSet>> r) {
            return r.map(rr -> {
                this.zkDiffs = rr;
                return this;
            });
        }

        Future<ReconciliationState> withVoid(Future<?> r) {
            return r.map(this);
        }

        Future<ReconciliationState> zkScaleDown() {
            return withVoid(zkSetOperations.scaleDown(namespace, zkCluster.getName(), zkCluster.getReplicas()));
        }

        Future<ReconciliationState> zkService() {
            return withVoid(serviceOperations.reconcile(namespace, zkCluster.getServiceName(), zkService));
        }

        Future<ReconciliationState> zkHeadlessService() {
            return withVoid(serviceOperations.reconcile(namespace, zkCluster.getHeadlessServiceName(), zkHeadlessService));
        }

        Future<ReconciliationState> zkAncillaryCm() {
            return withZkAncillaryCmChanged(configMapOperations.reconcile(namespace, zkCluster.getAncillaryConfigName(), zkMetricsAndLogsConfigMap));
        }

        Future<ReconciliationState> zkNodesSecret() {
            return withVoid(secretOperations.reconcile(namespace, ZookeeperCluster.nodesSecretName(name), zkCluster.generateNodesSecret()));
        }

        Future<ReconciliationState> zkNetPolicy() {
            return withVoid(networkPolicyOperator.reconcile(namespace, ZookeeperCluster.policyName(name), zkCluster.generateNetworkPolicy()));
        }

        Future<ReconciliationState> zkStatefulSet() {
            return withZkDiff(zkSetOperations.reconcile(namespace, zkCluster.getName(), zkCluster.generateStatefulSet(isOpenShift)));
        }

        Future<ReconciliationState> zkRollingUpdate() {
            return withVoid(zkSetOperations.maybeRollingUpdate(zkDiffs.resource(), zkForcedRestart));
        }

        Future<ReconciliationState> zkScaleUp() {
            return withVoid(zkSetOperations.scaleUp(namespace, zkCluster.getName(), zkCluster.getReplicas()));
        }

        Future<ReconciliationState> zkServiceEndpointReadiness() {
            return withVoid(serviceOperations.endpointReadiness(namespace, zkService, 1_000, operationTimeoutMs));
        }

        Future<ReconciliationState> zkHeadlessServiceEndpointReadiness() {
            return withVoid(serviceOperations.endpointReadiness(namespace, zkHeadlessService, 1_000, operationTimeoutMs));
        }

        Future<ReconciliationState> withZkAncillaryCmChanged(Future<ReconcileResult<ConfigMap>> r) {
            return r.map(rr -> {
                this.zkForcedRestart = rr instanceof ReconcileResult.Patched;
                return this;
            });
        }

        private Future<ReconciliationState> getKafkaClusterDescription() {
            Future<ReconciliationState> fut = Future.future();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        this.kafkaCluster = KafkaCluster.fromCrd(kafkaAssembly);

                        ConfigMap logAndMetricsConfigMap = kafkaCluster.generateMetricsAndLogConfigMap(
                                kafkaCluster.getLogging() instanceof ExternalLogging ?
                                        configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) kafkaCluster.getLogging()).getName()) :
                                        null);
                        this.kafkaService = kafkaCluster.generateService();
                        this.kafkaHeadlessService = kafkaCluster.generateHeadlessService();
                        this.kafkaMetricsAndLogsConfigMap = logAndMetricsConfigMap;

                        future.complete(this);
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                }, true,
                res -> {
                    if (res.succeeded()) {
                        fut.complete(res.result());
                    } else {
                        fut.fail(res.cause());
                    }
                }
            );
            return fut;
        }

        Future<ReconciliationState> withKafkaDiff(Future<ReconcileResult<StatefulSet>> r) {
            return r.map(rr -> {
                this.kafkaDiffs = rr;
                return this;
            });
        }

        Future<ReconciliationState> withKafkaAncillaryCmChanged(Future<ReconcileResult<ConfigMap>> r) {
            return r.map(rr -> {
                this.kafkaForcedRestart = rr instanceof ReconcileResult.Patched;
                return this;
            });
        }

        Future<ReconciliationState> kafkaInitServiceAccount() {
            return withVoid(serviceAccountOperator.reconcile(namespace,
                    KafkaCluster.initContainerServiceAccountName(kafkaCluster.getCluster()),
                    kafkaCluster.generateInitContainerServiceAccount()));
        }

        Future<ReconciliationState> kafkaInitClusterRoleBinding() {
            return withVoid(clusterRoleBindingOperator.reconcile(
                    KafkaCluster.initContainerClusterRoleBindingName(namespace, name),
                    kafkaCluster.generateClusterRoleBinding(namespace)));
        }

        Future<ReconciliationState> kafkaScaleDown() {
            return withVoid(kafkaSetOperations.scaleDown(namespace, kafkaCluster.getName(), kafkaCluster.getReplicas()));
        }

        Future<ReconciliationState> kafkaService() {
            return withVoid(serviceOperations.reconcile(namespace, kafkaCluster.getServiceName(), kafkaService));
        }

        Future<ReconciliationState> kafkaHeadlessService() {
            return withVoid(serviceOperations.reconcile(namespace, kafkaCluster.getHeadlessServiceName(), kafkaHeadlessService));
        }

        Future<ReconciliationState> kafkaReplicaServices() {
            int replicas = kafkaCluster.getReplicas();
            List<Future> serviceFutures = new ArrayList<>(replicas);

            for (int i = 0; i < replicas; i++) {
                serviceFutures.add(serviceOperations.reconcile(namespace, KafkaCluster.externalServiceName(name, i), kafkaCluster.generateExternalService(i)));
            }

            return withVoid(CompositeFuture.join(serviceFutures));
        }

        Future<ReconciliationState> kafkaBootstrapRoute() {
            Future future = Future.succeededFuture();
            Route route = kafkaCluster.generateExternalBootstrapRoute();

            if (routeOperations != null) {
                return withVoid(routeOperations.reconcile(namespace, KafkaCluster.serviceName(name), route));
            } else if (route != null)   {
                log.warn("{}: Exposing Kafka cluster {} using OpenShift Routes is available only on OpenShift", reconciliation, name);
                return withVoid(Future.failedFuture("Exposing Kafka cluster " + name + " using OpenShift Routes is available only on OpenShift"));
            }

            return withVoid(future);
        }

        Future<ReconciliationState> kafkaReplicaRoutes() {
            int replicas = kafkaCluster.getReplicas();
            List<Future> routeFutures = new ArrayList<>(replicas);

            for (int i = 0; i < replicas; i++) {
                Route route = kafkaCluster.generateExternalRoute(i);

                if (routeOperations != null) {
                    routeFutures.add(routeOperations.reconcile(namespace, KafkaCluster.externalServiceName(name, i), route));
                } else if (route != null)   {
                    log.warn("{}: Exposing Kafka cluster {} using OpenShift Routes is available only on OpenShift", reconciliation, name);
                    return withVoid(Future.failedFuture("Exposing Kafka cluster " + name + " using OpenShift Routes is available only on OpenShift"));
                }
            }

            return withVoid(CompositeFuture.join(routeFutures));
        }


        Future<ReconciliationState> kafkaReplicaRoutesReady() {
            if (routeOperations == null || !kafkaCluster.isExposedWithRoute()) {
                return withVoid(Future.succeededFuture());
            }

            String routeName = KafkaCluster.serviceName(name);
            Future future = Future.future();
            Future<Void> address = routeOperations.hasAddress(namespace, routeName, 1_000, operationTimeoutMs);

            address.setHandler(res -> {
                if (res.succeeded())    {
                    this.kafkaExternalBootstrapAddress = routeOperations.get(namespace, routeName).getSpec().getHost();

                    if (log.isTraceEnabled()) {
                        log.trace("Found address {} for Route {}", routeOperations.get(namespace, routeName).getSpec().getHost(), routeName);
                    }

                    future.complete();
                } else {
                    log.warn("{}: No address found for Route {}", reconciliation, routeName);
                    future.fail("No address found for Route " + routeName);
                }
            });

            return withVoid(future);
        }

        Future<ReconciliationState> kafkaBootstrapRouteReady() {
            if (routeOperations == null || !kafkaCluster.isExposedWithRoute()) {
                return withVoid(Future.succeededFuture());
            }

            int replicas = kafkaCluster.getReplicas();
            List<Future> routeFutures = new ArrayList<>(replicas);

            for (int i = 0; i < replicas; i++) {
                String routeName = KafkaCluster.externalServiceName(name, i);
                Future future = Future.future();
                Future<Void> address = routeOperations.hasAddress(namespace, routeName, 1_000, operationTimeoutMs);
                int podNumber = i;

                address.setHandler(res -> {
                    if (res.succeeded()) {
                        this.kafkaExternalAddresses.put(kafkaCluster.getPodName(podNumber), routeOperations.get(namespace, routeName).getSpec().getHost());

                        if (log.isTraceEnabled()) {
                            log.trace("Found address {} for Route {}", routeOperations.get(namespace, routeName).getSpec().getHost(), routeName);
                        }

                        future.complete();
                    } else {
                        log.warn("{}: No address found for Route {}", reconciliation, routeName);
                        future.fail("No address found for Route " + routeName);
                    }
                });

                routeFutures.add(future);
            }

            return withVoid(CompositeFuture.join(routeFutures));
        }

        Future<ReconciliationState> kafkaAncillaryCm() {
            return withKafkaAncillaryCmChanged(configMapOperations.reconcile(namespace, kafkaCluster.getAncillaryConfigName(), kafkaMetricsAndLogsConfigMap));
        }

        Future<ReconciliationState> kafkaClientsCaSecret() {
            return withVoid(secretOperations.reconcile(namespace, KafkaCluster.clientsCASecretName(name), kafkaCluster.generateClientsCASecret()));
        }

        Future<ReconciliationState> kafkaClientsPublicKeySecret() {
            return withVoid(secretOperations.reconcile(namespace, KafkaCluster.clientsPublicKeyName(name), kafkaCluster.generateClientsPublicKeySecret()));
        }

        Future<ReconciliationState> kafkaClusterPublicKeySecret() {
            return withVoid(secretOperations.reconcile(namespace, KafkaCluster.clusterPublicKeyName(name), kafkaCluster.generateClusterPublicKeySecret()));
        }

        Future<ReconciliationState> kafkaBrokersSecret() {
            return withVoid(secretOperations.reconcile(namespace, KafkaCluster.brokersSecretName(name), kafkaCluster.generateBrokersSecret(certManager, assemblySecrets, kafkaExternalBootstrapAddress, kafkaExternalAddresses)));
        }

        Future<ReconciliationState> kafkaNetPolicy() {
            return withVoid(networkPolicyOperator.reconcile(namespace, KafkaCluster.policyName(name), kafkaCluster.generateNetworkPolicy()));
        }

        Future<ReconciliationState> kafkaStatefulSet() {
            return withKafkaDiff(kafkaSetOperations.reconcile(namespace, kafkaCluster.getName(), kafkaCluster.generateStatefulSet(isOpenShift, kafkaExternalAddresses)));
        }

        Future<ReconciliationState> kafkaRollingUpdate() {
            return withVoid(kafkaSetOperations.maybeRollingUpdate(kafkaDiffs.resource(), kafkaForcedRestart));
        }

        Future<ReconciliationState> kafkaScaleUp() {
            return withVoid(kafkaSetOperations.scaleUp(namespace, kafkaCluster.getName(), kafkaCluster.getReplicas()));
        }

        Future<ReconciliationState> kafkaServiceEndpointReady() {
            return withVoid(serviceOperations.endpointReadiness(namespace, kafkaService, 1_000, operationTimeoutMs));
        }

        Future<ReconciliationState> kafkaHeadlessServiceEndpointReady() {
            return withVoid(serviceOperations.endpointReadiness(namespace, kafkaHeadlessService, 1_000, operationTimeoutMs));
        }

        private final Future<ReconciliationState> getTopicOperatorDescription() {
            Future<ReconciliationState> fut = Future.future();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        this.topicOperator = TopicOperator.fromCrd(certManager, kafkaAssembly, assemblySecrets);

                        if (topicOperator != null) {
                            ConfigMap logAndMetricsConfigMap = topicOperator.generateMetricsAndLogConfigMap(
                                    topicOperator.getLogging() instanceof ExternalLogging ?
                                            configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) topicOperator.getLogging()).getName()) :
                                            null);
                            this.toDeployment = topicOperator.generateDeployment();
                            this.toMetricsAndLogsConfigMap = logAndMetricsConfigMap;
                            this.toDeployment.getSpec().getTemplate().getMetadata().getAnnotations().put("strimzi.io/logging", this.toMetricsAndLogsConfigMap.getData().get("log4j2.properties"));
                        } else {
                            this.toDeployment = null;
                            this.toMetricsAndLogsConfigMap = null;
                        }

                        future.complete(this);
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                }, true,
                res -> {
                    if (res.succeeded()) {
                        fut.complete(res.result());
                    } else {
                        fut.fail(res.cause());
                    }
                }
            );
            return fut;
        }

        Future<ReconciliationState> topicOperatorServiceAccount() {
            return withVoid(serviceAccountOperator.reconcile(namespace,
                    TopicOperator.topicOperatorServiceAccountName(name),
                    toDeployment != null ? topicOperator.generateServiceAccount() : null));
        }

        Future<ReconciliationState> topicOperatorRoleBinding() {
            String watchedNamespace = topicOperator != null ? topicOperator.getWatchedNamespace() : null;
            return withVoid(roleBindingOperator.reconcile(
                    watchedNamespace != null && !watchedNamespace.isEmpty() ?
                            watchedNamespace : namespace,
                    TopicOperator.roleBindingName(name),
                    toDeployment != null ? topicOperator.generateRoleBinding(namespace) : null));
        }

        Future<ReconciliationState> topicOperatorAncillaryCm() {
            return withVoid(configMapOperations.reconcile(namespace,
                    toDeployment != null ? topicOperator.getAncillaryConfigName() : TopicOperator.metricAndLogConfigsName(name),
                    toMetricsAndLogsConfigMap));
        }

        Future<ReconciliationState> topicOperatorDeployment() {
            return withVoid(deploymentOperations.reconcile(namespace, TopicOperator.topicOperatorName(name), toDeployment));
        }

        Future<ReconciliationState> topicOperatorSecret() {
            return withVoid(secretOperations.reconcile(namespace, TopicOperator.secretName(name), topicOperator == null ? null : topicOperator.generateSecret()));
        }

        private final Future<ReconciliationState> getEntityOperatorDescription() {
            Future<ReconciliationState> fut = Future.future();

            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        EntityOperator entityOperator = EntityOperator.fromCrd(certManager, kafkaAssembly, assemblySecrets);

                        if (entityOperator != null) {
                            EntityTopicOperator topicOperator = entityOperator.getTopicOperator();
                            EntityUserOperator userOperator = entityOperator.getUserOperator();

                            ConfigMap topicOperatorLogAndMetricsConfigMap = topicOperator != null ?
                                    topicOperator.generateMetricsAndLogConfigMap(topicOperator.getLogging() instanceof ExternalLogging ?
                                            configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) topicOperator.getLogging()).getName()) :
                                            null) : null;

                            ConfigMap userOperatorLogAndMetricsConfigMap = userOperator != null ?
                                    userOperator.generateMetricsAndLogConfigMap(userOperator.getLogging() instanceof ExternalLogging ?
                                            configMapOperations.get(kafkaAssembly.getMetadata().getNamespace(), ((ExternalLogging) userOperator.getLogging()).getName()) :
                                            null) : null;

                            this.entityOperator = entityOperator;
                            this.eoDeployment = entityOperator.generateDeployment();
                            this.topicOperatorMetricsAndLogsConfigMap = topicOperatorLogAndMetricsConfigMap;
                            this.userOperatorMetricsAndLogsConfigMap = userOperatorLogAndMetricsConfigMap;
                        }

                        future.complete(this);
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                }, true,
                res -> {
                    if (res.succeeded()) {
                        fut.complete(res.result());
                    } else {
                        fut.fail(res.cause());
                    }
                }
            );
            return fut;
        }

        Future<ReconciliationState> entityOperatorServiceAccount() {
            return withVoid(serviceAccountOperator.reconcile(namespace,
                    EntityOperator.entityOperatorServiceAccountName(name),
                    eoDeployment != null ? entityOperator.generateServiceAccount() : null));
        }

        Future<ReconciliationState> entityOperatorTopicOpRoleBinding() {
            String watchedNamespace = entityOperator != null && entityOperator.getTopicOperator() != null ?
                    entityOperator.getTopicOperator().getWatchedNamespace() : null;
            return withVoid(roleBindingOperator.reconcile(
                    watchedNamespace != null && !watchedNamespace.isEmpty() ?
                            watchedNamespace : namespace,
                    EntityTopicOperator.roleBindingName(name),
                    eoDeployment != null && entityOperator.getTopicOperator() != null ?
                            entityOperator.getTopicOperator().generateRoleBinding(namespace) : null));
        }

        Future<ReconciliationState> entityOperatorUserOpRoleBinding() {
            String watchedNamespace = entityOperator != null && entityOperator.getUserOperator() != null ?
                    entityOperator.getUserOperator().getWatchedNamespace() : null;
            return withVoid(roleBindingOperator.reconcile(
                    watchedNamespace != null && !watchedNamespace.isEmpty() ?
                            watchedNamespace : namespace,
                    EntityUserOperator.roleBindingName(name),
                    eoDeployment != null && entityOperator.getUserOperator() != null ?
                            entityOperator.getUserOperator().generateRoleBinding(namespace) : null));

        }

        Future<ReconciliationState> entityOperatorTopicOpAncillaryCm() {
            return withVoid(configMapOperations.reconcile(namespace,
                    eoDeployment != null && entityOperator.getTopicOperator() != null ?
                            entityOperator.getTopicOperator().getAncillaryConfigName() : EntityTopicOperator.metricAndLogConfigsName(name),
                    topicOperatorMetricsAndLogsConfigMap));
        }

        Future<ReconciliationState> entityOperatorUserOpAncillaryCm() {
            return withVoid(configMapOperations.reconcile(namespace,
                    eoDeployment != null && entityOperator.getUserOperator() != null ?
                            entityOperator.getUserOperator().getAncillaryConfigName() : EntityUserOperator.metricAndLogConfigsName(name),
                    userOperatorMetricsAndLogsConfigMap));
        }

        Future<ReconciliationState> entityOperatorDeployment() {
            return withVoid(deploymentOperations.reconcile(namespace, EntityOperator.entityOperatorName(name), eoDeployment));
        }

        Future<ReconciliationState> entityOperatorSecret() {
            return withVoid(secretOperations.reconcile(namespace, EntityOperator.secretName(name), entityOperator == null ? null : entityOperator.generateSecret()));
        }
    }

    private final Future<CompositeFuture> deleteKafka(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        log.debug("{}: delete kafka {}", reconciliation, name);
        String kafkaSsName = KafkaCluster.kafkaClusterName(name);
        StatefulSet ss = kafkaSetOperations.get(namespace, kafkaSsName);
        int replicas = ss != null ? ss.getSpec().getReplicas() : 0;
        boolean deleteClaims = ss == null ? false : KafkaCluster.deleteClaim(ss);
        List<Future> result = new ArrayList<>(8 + (deleteClaims ? replicas : 0) + 2 * replicas);

        result.add(configMapOperations.reconcile(namespace, KafkaCluster.metricAndLogConfigsName(name), null));
        result.add(serviceOperations.reconcile(namespace, KafkaCluster.serviceName(name), null));
        result.add(serviceOperations.reconcile(namespace, KafkaCluster.headlessServiceName(name), null));
        result.add(kafkaSetOperations.reconcile(namespace, KafkaCluster.kafkaClusterName(name), null));
        result.add(secretOperations.reconcile(namespace, KafkaCluster.clientsCASecretName(name), null));
        result.add(secretOperations.reconcile(namespace, KafkaCluster.clientsPublicKeyName(name), null));
        result.add(secretOperations.reconcile(namespace, KafkaCluster.clusterPublicKeyName(name), null));
        result.add(secretOperations.reconcile(namespace, KafkaCluster.brokersSecretName(name), null));
        result.add(networkPolicyOperator.reconcile(namespace, KafkaCluster.policyName(name), null));

        for (int i = 0; i < replicas; i++) {
            result.add(serviceOperations.reconcile(namespace, KafkaCluster.externalServiceName(name, i), null));

            if (routeOperations != null)    {
                result.add(routeOperations.reconcile(namespace, KafkaCluster.externalServiceName(name, i), null));
            }
        }

        if (routeOperations != null)    {
            result.add(routeOperations.reconcile(namespace, KafkaCluster.serviceName(name), null));
        }

        if (deleteClaims) {
            log.debug("{}: delete kafka {} PVCs", reconciliation, name);

            for (int i = 0; i < replicas; i++) {
                result.add(pvcOperations.reconcile(namespace,
                        KafkaCluster.getPersistentVolumeClaimName(kafkaSsName, i), null));
            }
        }
        result.add(clusterRoleBindingOperator.reconcile(KafkaCluster.initContainerClusterRoleBindingName(namespace, name), null));
        result.add(serviceAccountOperator.reconcile(namespace, KafkaCluster.initContainerServiceAccountName(name), null));
        return CompositeFuture.join(result);
    }

    private final Future<CompositeFuture> deleteZk(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        log.debug("{}: delete zookeeper {}", reconciliation, name);
        String zkSsName = ZookeeperCluster.zookeeperClusterName(name);
        StatefulSet ss = zkSetOperations.get(namespace, zkSsName);
        boolean deleteClaims = ss == null ? false : ZookeeperCluster.deleteClaim(ss);
        List<Future> result = new ArrayList<>(4 + (deleteClaims ? ss.getSpec().getReplicas() : 0));

        result.add(configMapOperations.reconcile(namespace, ZookeeperCluster.zookeeperMetricAndLogConfigsName(name), null));
        result.add(serviceOperations.reconcile(namespace, ZookeeperCluster.serviceName(name), null));
        result.add(serviceOperations.reconcile(namespace, ZookeeperCluster.headlessServiceName(name), null));
        result.add(zkSetOperations.reconcile(namespace, zkSsName, null));
        result.add(secretOperations.reconcile(namespace, ZookeeperCluster.nodesSecretName(name), null));
        result.add(networkPolicyOperator.reconcile(namespace, ZookeeperCluster.policyName(name), null));

        if (deleteClaims) {
            log.debug("{}: delete zookeeper {} PVCs", reconciliation, name);
            for (int i = 0; i < ss.getSpec().getReplicas(); i++) {
                result.add(pvcOperations.reconcile(namespace, ZookeeperCluster.getPersistentVolumeClaimName(zkSsName, i), null));
            }
        }

        return CompositeFuture.join(result);
    }



    private final Future<CompositeFuture> deleteTopicOperator(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        log.debug("{}: delete topic operator {}", reconciliation, name);

        List<Future> result = new ArrayList<>(3);
        result.add(configMapOperations.reconcile(namespace, TopicOperator.metricAndLogConfigsName(name), null));
        result.add(deploymentOperations.reconcile(namespace, TopicOperator.topicOperatorName(name), null));
        result.add(secretOperations.reconcile(namespace, TopicOperator.secretName(name), null));
        result.add(roleBindingOperator.reconcile(namespace, TopicOperator.roleBindingName(name), null));
        result.add(serviceAccountOperator.reconcile(namespace, TopicOperator.topicOperatorServiceAccountName(name), null));
        return CompositeFuture.join(result);
    }

    private final Future<CompositeFuture> deleteEntityOperator(Reconciliation reconciliation) {
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        log.debug("{}: delete entity operator {}", reconciliation, name);

        List<Future> result = new ArrayList<>(3);
        result.add(configMapOperations.reconcile(namespace, EntityTopicOperator.metricAndLogConfigsName(name), null));
        result.add(configMapOperations.reconcile(namespace, EntityUserOperator.metricAndLogConfigsName(name), null));
        result.add(deploymentOperations.reconcile(namespace, EntityOperator.entityOperatorName(name), null));
        result.add(secretOperations.reconcile(namespace, EntityOperator.secretName(name), null));
        result.add(roleBindingOperator.reconcile(namespace, EntityTopicOperator.roleBindingName(name), null));
        result.add(roleBindingOperator.reconcile(namespace, EntityUserOperator.roleBindingName(name), null));
        result.add(serviceAccountOperator.reconcile(namespace, EntityOperator.entityOperatorServiceAccountName(name), null));
        return CompositeFuture.join(result);
    }

    @Override
    protected Future<Void> delete(Reconciliation reconciliation) {
        return deleteEntityOperator(reconciliation)
                .compose(i -> deleteTopicOperator(reconciliation))
                .compose(i -> deleteKafka(reconciliation))
                .compose(i -> deleteZk(reconciliation))
                .compose(i -> deleteClusterCa(reconciliation));
    }

    private final Future<Void> deleteClusterCa(Reconciliation reconciliation) {
        Future<Void> result = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                String clusterCaName = AbstractModel.getClusterCaName(reconciliation.name());

                if (secretOperations.get(reconciliation.namespace(), clusterCaName) != null) {
                    log.debug("{}: Deleting cluster CA certificate {}", reconciliation, clusterCaName);
                    secretOperations.reconcile(reconciliation.namespace(), clusterCaName, null)
                            .compose(reconcileResult -> future.complete(), future);
                    log.debug("{}: Cluster CA {} deleted", reconciliation, clusterCaName);
                } else {
                    log.debug("{}: The cluster CA {} doesn't exist", reconciliation, clusterCaName);
                    future.complete();
                }
            }, true,
            result.completer()
        );
        return result;
    }

    @Override
    protected List<HasMetadata> getResources(String namespace, Labels selector) {
        List<HasMetadata> result = new ArrayList<>();
        result.addAll(kafkaSetOperations.list(namespace, selector));
        result.addAll(zkSetOperations.list(namespace, selector));
        result.addAll(deploymentOperations.list(namespace, selector));
        result.addAll(serviceOperations.list(namespace, selector));
        result.addAll(resourceOperator.list(namespace, selector));

        if (routeOperations != null) {
            result.addAll(routeOperations.list(namespace, selector));
        }

        return result;
    }
}