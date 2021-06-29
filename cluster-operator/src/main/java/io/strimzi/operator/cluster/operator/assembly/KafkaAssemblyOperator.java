/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.storage.StorageClass;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.Route;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaSpec;
import io.strimzi.api.kafka.model.listener.NodeAddressType;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.status.KafkaStatusBuilder;
import io.strimzi.api.kafka.model.status.ListenerAddress;
import io.strimzi.api.kafka.model.status.ListenerAddressBuilder;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.api.kafka.model.status.ListenerStatusBuilder;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.FeatureGates;
import io.strimzi.operator.cluster.KafkaUpgradeException;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.cluster.model.EntityOperator;
import io.strimzi.operator.cluster.model.EntityTopicOperator;
import io.strimzi.operator.cluster.model.EntityUserOperator;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.JmxTrans;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaExporter;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.ListenersUtils;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.NodeUtils;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.model.StorageDiff;
import io.strimzi.operator.cluster.model.StorageUtils;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ConcurrentDeletionException;
import io.strimzi.operator.cluster.operator.resource.KafkaRoller;
import io.strimzi.operator.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.operator.cluster.operator.resource.KafkaSpecChecker;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.ZookeeperScaler;
import io.strimzi.operator.cluster.operator.resource.ZookeeperScalerProvider;
import io.strimzi.operator.cluster.operator.resource.ZookeeperSetOperator;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationException;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.AbstractScalableResourceOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.IngressOperator;
import io.strimzi.operator.common.operator.resource.IngressV1Beta1Operator;
import io.strimzi.operator.common.operator.resource.NodeOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;
import io.strimzi.operator.common.operator.resource.RoleOperator;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.strimzi.operator.common.operator.resource.StorageClassOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaException;
import org.quartz.CronExpression;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG;
import static io.strimzi.operator.cluster.model.AbstractModel.ANNO_STRIMZI_IO_STORAGE;
import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION;
import static io.strimzi.operator.cluster.model.KafkaVersion.compareDottedVersions;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Collections.emptyMap;

/**
 * <p>Assembly operator for a "Kafka" assembly, which manages:</p>
 * <ul>
 *     <li>A ZooKeeper cluster StatefulSet and related Services</li>
 *     <li>A Kafka cluster StatefulSet and related Services</li>
 *     <li>Optionally, a TopicOperator Deployment</li>
 * </ul>
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity", "checkstyle:JavaNCSS"})
public class KafkaAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, Kafka, KafkaList, Resource<Kafka>, KafkaSpec, KafkaStatus> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaAssemblyOperator.class.getName());

    private final long operationTimeoutMs;
    private final String operatorNamespace;
    private final Labels operatorNamespaceLabels;
    private final FeatureGates featureGates;

    private final ZookeeperSetOperator zkSetOperations;
    private final KafkaSetOperator kafkaSetOperations;
    private final RouteOperator routeOperations;
    private final PvcOperator pvcOperations;
    private final DeploymentOperator deploymentOperations;
    private final RoleBindingOperator roleBindingOperations;
    private final RoleOperator roleOperations;
    private final PodOperator podOperations;
    private final IngressOperator ingressOperations;
    private final IngressV1Beta1Operator ingressV1Beta1Operations;
    private final StorageClassOperator storageClassOperator;
    private final NodeOperator nodeOperator;
    private final CrdOperator<KubernetesClient, Kafka, KafkaList> crdOperator;
    private final ZookeeperScalerProvider zkScalerProvider;
    private final AdminClientProvider adminClientProvider;

    /**
     * @param vertx The Vertx instance
     * @param pfa Platform features availability properties
     * @param certManager Certificate manager
     * @param passwordGenerator Password generator
     * @param supplier Supplies the operators for different resources
     * @param config ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     */
    public KafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                 CertManager certManager, PasswordGenerator passwordGenerator,
                                 ResourceOperatorSupplier supplier,
                                 ClusterOperatorConfig config) {
        super(vertx, pfa, Kafka.RESOURCE_KIND, certManager, passwordGenerator,
                supplier.kafkaOperator, supplier, config);
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.operatorNamespace = config.getOperatorNamespace();
        this.operatorNamespaceLabels = config.getOperatorNamespaceLabels();
        this.featureGates = config.featureGates();
        this.routeOperations = supplier.routeOperations;
        this.zkSetOperations = supplier.zkSetOperations;
        this.kafkaSetOperations = supplier.kafkaSetOperations;
        this.pvcOperations = supplier.pvcOperations;
        this.deploymentOperations = supplier.deploymentOperations;
        this.roleBindingOperations = supplier.roleBindingOperations;
        this.roleOperations = supplier.roleOperations;
        this.podOperations = supplier.podOperations;
        this.ingressOperations = supplier.ingressOperations;
        this.ingressV1Beta1Operations = supplier.ingressV1Beta1Operations;
        this.storageClassOperator = supplier.storageClassOperations;
        this.crdOperator = supplier.kafkaOperator;
        this.nodeOperator = supplier.nodeOperator;
        this.zkScalerProvider = supplier.zkScalerProvider;
        this.adminClientProvider = supplier.adminClientProvider;
    }

    @Override
    public Future<KafkaStatus> createOrUpdate(Reconciliation reconciliation, Kafka kafkaAssembly) {
        Promise<KafkaStatus> createOrUpdatePromise = Promise.promise();
        ReconciliationState reconcileState = createReconciliationState(reconciliation, kafkaAssembly);

        reconcile(reconcileState).onComplete(reconcileResult -> {
            KafkaStatus status = reconcileState.kafkaStatus;
            Condition condition;

            if (kafkaAssembly.getMetadata().getGeneration() != null)    {
                status.setObservedGeneration(kafkaAssembly.getMetadata().getGeneration());
            }

            if (reconcileResult.succeeded())    {
                condition = new ConditionBuilder()
                        .withLastTransitionTime(StatusUtils.iso8601(dateSupplier()))
                        .withType("Ready")
                        .withStatus("True")
                        .build();

                status.addCondition(condition);
                createOrUpdatePromise.complete(status);
            } else {
                condition = new ConditionBuilder()
                        .withLastTransitionTime(StatusUtils.iso8601(dateSupplier()))
                        .withType("NotReady")
                        .withStatus("True")
                        .withReason(reconcileResult.cause().getClass().getSimpleName())
                        .withMessage(reconcileResult.cause().getMessage())
                        .build();

                status.addCondition(condition);
                createOrUpdatePromise.fail(new ReconciliationException(status, reconcileResult.cause()));
            }
        });

        return createOrUpdatePromise.future();
    }

    Future<Void> reconcile(ReconciliationState reconcileState)  {
        Promise<Void> chainPromise = Promise.promise();

        reconcileState.initialStatus()
                .compose(state -> state.reconcileCas(this::dateSupplier))
                .compose(state -> state.clusterOperatorSecret(this::dateSupplier))
                .compose(state -> state.getKafkaClusterDescription())
                .compose(state -> state.prepareVersionChange())
                // Roll everything if a new CA is added to the trust store.
                .compose(state -> state.rollingUpdateForNewCaKey())
                .compose(state -> state.getZookeeperDescription())
                .compose(state -> state.zkModelWarnings())
                .compose(state -> state.zkManualPodCleaning())
                .compose(state -> state.zkNetPolicy())
                .compose(state -> state.zkManualRollingUpdate())
                .compose(state -> state.zkVersionChange())
                .compose(state -> state.zookeeperServiceAccount())
                .compose(state -> state.zkPvcs())
                .compose(state -> state.zkService())
                .compose(state -> state.zkHeadlessService())
                .compose(state -> state.zkGenerateCertificates(this::dateSupplier))
                .compose(state -> state.zkAncillaryCm())
                .compose(state -> state.zkNodesSecret())
                .compose(state -> state.zkPodDisruptionBudget())
                .compose(state -> state.zkStatefulSet())
                .compose(state -> state.zkScalingDown())
                .compose(state -> state.zkRollingUpdate())
                .compose(state -> state.zkPodsReady())
                .compose(state -> state.zkScalingUp())
                .compose(state -> state.zkScalingCheck())
                .compose(state -> state.zkServiceEndpointReadiness())
                .compose(state -> state.zkHeadlessServiceEndpointReadiness())
                .compose(state -> state.zkPersistentClaimDeletion())

                .compose(state -> state.checkKafkaSpec())
                .compose(state -> state.kafkaModelWarnings())
                .compose(state -> state.kafkaManualPodCleaning())
                .compose(state -> state.kafkaNetPolicy())
                .compose(state -> state.kafkaManualRollingUpdate())
                .compose(state -> state.kafkaPvcs())
                .compose(state -> state.kafkaInitServiceAccount())
                .compose(state -> state.kafkaInitClusterRoleBinding())
                .compose(state -> state.kafkaScaleDown())
                .compose(state -> state.kafkaServices())
                .compose(state -> state.kafkaRoutes())
                .compose(state -> state.kafkaIngresses())
                .compose(state -> state.kafkaIngressesV1Beta1())
                .compose(state -> state.kafkaInternalServicesReady())
                .compose(state -> state.kafkaLoadBalancerServicesReady())
                .compose(state -> state.kafkaNodePortServicesReady())
                .compose(state -> state.kafkaRoutesReady())
                .compose(state -> state.kafkaIngressesReady())
                .compose(state -> state.kafkaIngressesV1Beta1Ready())
                .compose(state -> state.kafkaGenerateCertificates(this::dateSupplier))
                .compose(state -> state.customListenerCertificates())
                .compose(state -> state.kafkaAncillaryCm())
                .compose(state -> state.kafkaBrokersSecret())
                .compose(state -> state.kafkaJmxSecret())
                .compose(state -> state.kafkaPodDisruptionBudget())
                .compose(state -> state.kafkaStatefulSet())
                .compose(state -> state.kafkaRollToAddOrRemoveVolumes())
                .compose(state -> state.kafkaRollingUpdate())
                .compose(state -> state.kafkaScaleUp())
                .compose(state -> state.kafkaPodsReady())
                .compose(state -> state.kafkaServiceEndpointReady())
                .compose(state -> state.kafkaHeadlessServiceEndpointReady())
                .compose(state -> state.kafkaGetClusterId())
                .compose(state -> state.kafkaPersistentClaimDeletion())
                // This has to run after all possible rolling updates which might move the pods to different nodes
                .compose(state -> state.kafkaNodePortExternalListenerStatus())
                .compose(state -> state.kafkaCustomCertificatesToStatus())

                .compose(state -> state.getEntityOperatorDescription())
                .compose(state -> state.entityOperatorRole())
                .compose(state -> state.entityTopicOperatorRole())
                .compose(state -> state.entityUserOperatorRole())
                .compose(state -> state.entityOperatorServiceAccount())
                .compose(state -> state.entityOperatorTopicOpRoleBindingForRole())
                .compose(state -> state.entityOperatorUserOpRoleBindingForRole())
                .compose(state -> state.entityOperatorTopicOpAncillaryCm())
                .compose(state -> state.entityOperatorUserOpAncillaryCm())
                .compose(state -> state.entityOperatorSecret(this::dateSupplier))
                .compose(state -> state.entityOperatorDeployment())
                .compose(state -> state.entityOperatorReady())

                .compose(state -> state.getCruiseControlDescription())
                .compose(state -> state.cruiseControlNetPolicy())
                .compose(state -> state.cruiseControlServiceAccount())
                .compose(state -> state.cruiseControlAncillaryCm())
                .compose(state -> state.cruiseControlSecret(this::dateSupplier))
                .compose(state -> state.cruiseControlDeployment())
                .compose(state -> state.cruiseControlService())
                .compose(state -> state.cruiseControlReady())

                .compose(state -> state.getKafkaExporterDescription())
                .compose(state -> state.kafkaExporterServiceAccount())
                .compose(state -> state.kafkaExporterSecret(this::dateSupplier))
                .compose(state -> state.kafkaExporterDeployment())
                .compose(state -> state.kafkaExporterReady())

                .compose(state -> state.getJmxTransDescription())
                .compose(state -> state.jmxTransServiceAccount())
                .compose(state -> state.jmxTransConfigMap())
                .compose(state -> state.jmxTransDeployment())
                .compose(state -> state.jmxTransDeploymentReady())

                .map((Void) null)
                .onComplete(chainPromise);

        return chainPromise.future();
    }

    ReconciliationState createReconciliationState(Reconciliation reconciliation, Kafka kafkaAssembly) {
        return new ReconciliationState(reconciliation, kafkaAssembly);
    }

    /**
     * Hold the mutable state during a reconciliation
     */
    class ReconciliationState {
        private final String namespace;
        private final String name;
        private final Kafka kafkaAssembly;
        private final Reconciliation reconciliation;

        private boolean kafkaStsAlreadyExists = false;
        private String currentStsVersion;
        private KafkaVersionChange versionChange;
        private String highestLogMessageFormatVersion;
        private String highestInterBrokerProtocolVersion;

        /* test */ ClusterCa clusterCa;
        /* test */ ClientsCa clientsCa;

        /* test */ ZookeeperCluster zkCluster;
        private ConfigMap zkMetricsAndLogsConfigMap;
        /* test */ ReconcileResult<StatefulSet> zkDiffs;
        private Integer zkCurrentReplicas = null;

        private KafkaCluster kafkaCluster = null;
        private Integer kafkaCurrentReplicas = null;
        /* test */ KafkaStatus kafkaStatus = new KafkaStatus();

        /* test */ ReconcileResult<StatefulSet> kafkaDiffs;
        private final Set<String> kafkaBootstrapDnsName = new HashSet<>();
        private final Set<String> kafkaAdvertisedHostnames = new TreeSet<>();
        private final Set<String> kafkaAdvertisedPorts = new TreeSet<>();
        private final Map<Integer, Set<String>> kafkaBrokerDnsNames = new HashMap<>();
        /* test */ final Map<String, Integer> kafkaBootstrapNodePorts = new HashMap<>();

        private String zkLoggingHash = "";
        private String kafkaLogging = "";
        private String kafkaLoggingAppendersHash = "";
        private String kafkaBrokerConfigurationHash = "";

        /* test */ EntityOperator entityOperator;
        /* test */ Deployment eoDeployment = null;
        private ConfigMap topicOperatorMetricsAndLogsConfigMap = null;
        private ConfigMap userOperatorMetricsAndLogsConfigMap;
        private Secret oldCoSecret;

        CruiseControl cruiseControl;
        Deployment ccDeployment = null;
        private ConfigMap cruiseControlMetricsAndLogsConfigMap;

        /* test */ KafkaExporter kafkaExporter;
        /* test */ Deployment exporterDeployment = null;

        /* test */ Set<String> fsResizingRestartRequest = new HashSet<>();

        // Certificate change indicators
        private boolean existingZookeeperCertsChanged = false;
        private boolean existingKafkaCertsChanged = false;
        private boolean existingKafkaExporterCertsChanged = false;
        private boolean existingEntityOperatorCertsChanged = false;
        private boolean existingCruiseControlCertsChanged = false;

        // Custom Listener certificates
        private final Map<String, String> customListenerCertificates = new HashMap<>();
        private final Map<String, String> customListenerCertificateThumbprints = new HashMap<>();

        private JmxTrans jmxTrans = null;
        private ConfigMap jmxTransConfigMap = null;
        private Deployment jmxTransDeployment = null;

        ReconciliationState(Reconciliation reconciliation, Kafka kafkaAssembly) {
            this.reconciliation = reconciliation;
            this.kafkaAssembly = kafkaAssembly;
            this.namespace = kafkaAssembly.getMetadata().getNamespace();
            this.name = kafkaAssembly.getMetadata().getName();
        }

        /**
         * Updates the Status field of the Kafka CR. It diffs the desired status against the current status and calls
         * the update only when there is any difference in non-timestamp fields.
         *
         * @param desiredStatus The KafkaStatus which should be set
         *
         * @return
         */
        Future<Void> updateStatus(KafkaStatus desiredStatus) {
            Promise<Void> updateStatusPromise = Promise.promise();

            crdOperator.getAsync(namespace, name).onComplete(getRes -> {
                if (getRes.succeeded())    {
                    Kafka kafka = getRes.result();

                    if (kafka != null) {
                        if ((Constants.RESOURCE_GROUP_NAME + "/" + Constants.V1ALPHA1).equals(kafka.getApiVersion()))   {
                            LOGGER.warnCr(reconciliation, "The resource needs to be upgraded from version {} to 'v1beta1' to use the status field", kafka.getApiVersion());
                            updateStatusPromise.complete();
                        } else {
                            KafkaStatus currentStatus = kafka.getStatus();

                            StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                            if (!ksDiff.isEmpty()) {
                                Kafka resourceWithNewStatus = new KafkaBuilder(kafka).withStatus(desiredStatus).build();

                                crdOperator.updateStatusAsync(reconciliation, resourceWithNewStatus).onComplete(updateRes -> {
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
                        LOGGER.errorCr(reconciliation, "Current Kafka resource not found");
                        updateStatusPromise.fail("Current Kafka resource not found");
                    }
                } else {
                    LOGGER.errorCr(reconciliation, "Failed to get the current Kafka resource and its status", getRes.cause());
                    updateStatusPromise.fail(getRes.cause());
                }
            });

            return updateStatusPromise.future();
        }

        /**
         * Sets the initial status when the Kafka resource is created and the cluster starts deploying.
         *
         * @return
         */
        Future<ReconciliationState> initialStatus() {
            Promise<ReconciliationState> initialStatusPromise = Promise.promise();

            crdOperator.getAsync(namespace, name).onComplete(getRes -> {
                if (getRes.succeeded())    {
                    Kafka kafka = getRes.result();

                    if (kafka != null && kafka.getStatus() == null) {
                        LOGGER.debugCr(reconciliation, "Setting the initial status for a new resource");

                        Condition deployingCondition = new ConditionBuilder()
                                .withLastTransitionTime(StatusUtils.iso8601(dateSupplier()))
                                .withType("NotReady")
                                .withStatus("True")
                                .withReason("Creating")
                                .withMessage("Kafka cluster is being deployed")
                                .build();

                        KafkaStatus initialStatus = new KafkaStatusBuilder()
                                .addToConditions(deployingCondition)
                                .build();

                        updateStatus(initialStatus).map(this).onComplete(initialStatusPromise);
                    } else {
                        LOGGER.debugCr(reconciliation, "Status is already set. No need to set initial status");
                        initialStatusPromise.complete(this);
                    }
                } else {
                    LOGGER.errorCr(reconciliation, "Failed to get the current Kafka resource and its status", getRes.cause());
                    initialStatusPromise.fail(getRes.cause());
                }
            });

            return initialStatusPromise.future();
        }

        /**
         * Checks the requested Kafka spec for potential issues, and adds warnings and advice for best
         * practice to the status.
         */
        Future<ReconciliationState> checkKafkaSpec() {
            KafkaSpecChecker checker = new KafkaSpecChecker(kafkaAssembly.getSpec(), versions, kafkaCluster, zkCluster);
            List<Condition> warnings = checker.run();
            kafkaStatus.addConditions(warnings);
            return Future.succeededFuture(this);
        }

        /**
         * Takes the warning conditions from the Model and adds them in the KafkaStatus
         */
        Future<ReconciliationState> kafkaModelWarnings() {
            kafkaStatus.addConditions(kafkaCluster.getWarningConditions());
            return Future.succeededFuture(this);
        }

        /**
         * Takes the warning conditions from the Model and adds them in the KafkaStatus
         */
        Future<ReconciliationState> zkModelWarnings() {
            kafkaStatus.addConditions(zkCluster.getWarningConditions());
            return Future.succeededFuture(this);
        }

        /**
         * Asynchronously reconciles the cluster and clients CA secrets.
         * The cluster CA secret has to have the name determined by {@link AbstractModel#clusterCaCertSecretName(String)}.
         * The clients CA secret has to have the name determined by {@link KafkaCluster#clientsCaCertSecretName(String)}.
         * Within both the secrets the current certificate is stored under the key {@code ca.crt}
         * and the current key is stored under the key {@code ca.key}.
         */
        Future<ReconciliationState> reconcileCas(Supplier<Date> dateSupplier) {
            Labels selectorLabels = Labels.EMPTY.withStrimziKind(reconciliation.kind()).withStrimziCluster(reconciliation.name());
            Labels caLabels = Labels.generateDefaultLabels(kafkaAssembly, Labels.APPLICATION_NAME, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
            Promise<ReconciliationState> resultPromise = Promise.promise();
            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        String clusterCaCertName = AbstractModel.clusterCaCertSecretName(name);
                        String clusterCaKeyName = AbstractModel.clusterCaKeySecretName(name);
                        String clientsCaCertName = KafkaCluster.clientsCaCertSecretName(name);
                        String clientsCaKeyName = KafkaCluster.clientsCaKeySecretName(name);
                        Secret clusterCaCertSecret = null;
                        Secret clusterCaKeySecret = null;
                        Secret clientsCaCertSecret = null;
                        Secret clientsCaKeySecret = null;
                        List<Secret> clusterSecrets = secretOperations.list(reconciliation.namespace(), selectorLabels);
                        for (Secret secret : clusterSecrets) {
                            String secretName = secret.getMetadata().getName();
                            if (secretName.equals(clusterCaCertName)) {
                                clusterCaCertSecret = secret;
                            } else if (secretName.equals(clusterCaKeyName)) {
                                clusterCaKeySecret = secret;
                            } else if (secretName.equals(clientsCaCertName)) {
                                clientsCaCertSecret = secret;
                            } else if (secretName.equals(clientsCaKeyName)) {
                                clientsCaKeySecret = secret;
                            }
                        }
                        OwnerReference ownerRef = new OwnerReferenceBuilder()
                                .withApiVersion(kafkaAssembly.getApiVersion())
                                .withKind(kafkaAssembly.getKind())
                                .withName(kafkaAssembly.getMetadata().getName())
                                .withUid(kafkaAssembly.getMetadata().getUid())
                                .withBlockOwnerDeletion(false)
                                .withController(false)
                                .build();

                        CertificateAuthority clusterCaConfig = kafkaAssembly.getSpec().getClusterCa();

                        // When we are not supposed to generate the CA but it does not exist, we should just throw an error
                        checkCustomCaSecret(clusterCaConfig, clusterCaCertSecret, clusterCaKeySecret, "Cluster CA");

                        Map<String, String> clusterCaCertLabels = emptyMap();
                        Map<String, String> clusterCaCertAnnotations = emptyMap();

                        if (kafkaAssembly.getSpec().getKafka() != null
                                && kafkaAssembly.getSpec().getKafka().getTemplate() != null
                                && kafkaAssembly.getSpec().getKafka().getTemplate().getClusterCaCert() != null
                                && kafkaAssembly.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata() != null) {
                            clusterCaCertLabels = kafkaAssembly.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata().getLabels();
                            clusterCaCertAnnotations = kafkaAssembly.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata().getAnnotations();
                        }

                        this.clusterCa = new ClusterCa(reconciliation, certManager, passwordGenerator, name, clusterCaCertSecret,
                                clusterCaKeySecret,
                                ModelUtils.getCertificateValidity(clusterCaConfig),
                                ModelUtils.getRenewalDays(clusterCaConfig),
                                clusterCaConfig == null || clusterCaConfig.isGenerateCertificateAuthority(), clusterCaConfig != null ? clusterCaConfig.getCertificateExpirationPolicy() : null);
                        clusterCa.createRenewOrReplace(
                                reconciliation.namespace(), reconciliation.name(), caLabels.toMap(),
                                clusterCaCertLabels, clusterCaCertAnnotations,
                                clusterCaConfig != null && !clusterCaConfig.isGenerateSecretOwnerReference() ? null : ownerRef,
                                isMaintenanceTimeWindowsSatisfied(dateSupplier));

                        this.clusterCa.initCaSecrets(clusterSecrets);

                        CertificateAuthority clientsCaConfig = kafkaAssembly.getSpec().getClientsCa();

                        // When we are not supposed to generate the CA but it does not exist, we should just throw an error
                        checkCustomCaSecret(clientsCaConfig, clientsCaCertSecret, clientsCaKeySecret, "Clients CA");

                        this.clientsCa = new ClientsCa(reconciliation, certManager,
                                passwordGenerator, clientsCaCertName,
                                clientsCaCertSecret, clientsCaKeyName,
                                clientsCaKeySecret,
                                ModelUtils.getCertificateValidity(clientsCaConfig),
                                ModelUtils.getRenewalDays(clientsCaConfig),
                                clientsCaConfig == null || clientsCaConfig.isGenerateCertificateAuthority(), clientsCaConfig != null ? clientsCaConfig.getCertificateExpirationPolicy() : null);
                        clientsCa.createRenewOrReplace(reconciliation.namespace(), reconciliation.name(),
                                caLabels.toMap(), emptyMap(), emptyMap(),
                                clientsCaConfig != null && !clientsCaConfig.isGenerateSecretOwnerReference() ? null : ownerRef,
                                isMaintenanceTimeWindowsSatisfied(dateSupplier));

                        List<Future> secretReconciliations = new ArrayList<>(2);

                        if (clusterCaConfig == null || clusterCaConfig.isGenerateCertificateAuthority())   {
                            Future clusterSecretReconciliation = secretOperations.reconcile(reconciliation, reconciliation.namespace(), clusterCaCertName, this.clusterCa.caCertSecret())
                                    .compose(ignored -> secretOperations.reconcile(reconciliation, reconciliation.namespace(), clusterCaKeyName, this.clusterCa.caKeySecret()));
                            secretReconciliations.add(clusterSecretReconciliation);
                        }

                        if (clientsCaConfig == null || clientsCaConfig.isGenerateCertificateAuthority())   {
                            Future clientsSecretReconciliation = secretOperations.reconcile(reconciliation, reconciliation.namespace(), clientsCaCertName, this.clientsCa.caCertSecret())
                                .compose(ignored -> secretOperations.reconcile(reconciliation, reconciliation.namespace(), clientsCaKeyName, this.clientsCa.caKeySecret()));
                            secretReconciliations.add(clientsSecretReconciliation);
                        }

                        CompositeFuture.join(secretReconciliations).onComplete(res -> {
                            if (res.succeeded())    {
                                future.complete(this);
                            } else {
                                future.fail(res.cause());
                            }
                        });
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                }, true,
                resultPromise
            );
            return resultPromise.future();
        }

        /**
         * Utility method for checking the Secret existence when custom CA is used. The custom CA is configured but the
         * secrets do not exist, it will throw InvalidConfigurationException.
         *
         * @param ca            The CA Configuration from the Custom Resource
         * @param certSecret    Secret with the certificate public key
         * @param keySecret     Secret with the certificate private key
         * @param caDescription The name of the CA for which this check is executed ("Cluster CA" or "Clients CA" - used
         *                      in the exception message)
         */
        public void checkCustomCaSecret(CertificateAuthority ca, Secret certSecret, Secret keySecret, String caDescription)   {
            if (ca != null && !ca.isGenerateCertificateAuthority() && (certSecret == null || keySecret == null))   {
                throw new InvalidConfigurationException(caDescription + " should not be generated, but the secrets were not found.");
            }
        }

        /**
         * Perform a rolling update of the cluster so that CA certificates get added to their truststores,
         * or expired CA certificates get removed from their truststores.
         * Note this is only necessary when the CA certificate has changed due to a new CA key.
         * It is not necessary when the CA certificate is replace while retaining the existing key.
         */
        Future<ReconciliationState> rollingUpdateForNewCaKey() {
            List<String> reason = new ArrayList<>(4);
            if (this.clusterCa.keyReplaced()) {
                reason.add("trust new cluster CA certificate signed by new key");
            }
            if (this.clientsCa.keyReplaced()) {
                reason.add("trust new clients CA certificate signed by new key");
            }
            if (!reason.isEmpty()) {
                Future<Void> zkRollFuture;
                Function<Pod, List<String>> rollPodAndLogReason = pod -> {
                    LOGGER.debugCr(reconciliation, "Rolling Pod {} to {}", pod.getMetadata().getName(), reason);
                    return reason;
                };
                if (this.clusterCa.keyReplaced()) {
                    zkRollFuture = zkSetOperations.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name))
                        .compose(sts -> zkSetOperations.maybeRollingUpdate(reconciliation, sts, rollPodAndLogReason,
                        clusterCa.caCertSecret(),
                        oldCoSecret));
                } else {
                    zkRollFuture = Future.succeededFuture();
                }
                return zkRollFuture
                        .compose(i -> kafkaSetOperations.getAsync(namespace, KafkaCluster.kafkaClusterName(name)))
                        .compose(sts -> new KafkaRoller(reconciliation, vertx, podOperations, 1_000, operationTimeoutMs,
                            () -> new BackOff(250, 2, 10), sts, clusterCa.caCertSecret(), oldCoSecret, adminClientProvider,
                            kafkaCluster.getBrokersConfiguration(), kafkaLogging, kafkaCluster.getKafkaVersion(), true)
                            .rollingRestart(rollPodAndLogReason))
                        .compose(i -> rollDeploymentIfExists(EntityOperator.entityOperatorName(name), reason.toString()))
                        .compose(i -> rollDeploymentIfExists(KafkaExporter.kafkaExporterName(name), reason.toString()))
                        .compose(i -> rollDeploymentIfExists(CruiseControl.cruiseControlName(name), reason.toString()))
                        .map(i -> this);
            } else {
                return Future.succeededFuture(this);
            }
        }

        /**
         * Rolls deployments when they exist. This method is used by the CA renewal to roll deployments.
         *
         * @param deploymentName    Name of the deployment which should be rolled if it exists
         * @param reasons   Reasons for which it is being rolled
         * @return  Succeeded future if it succeeded, failed otherwise.
         */
        Future<Void> rollDeploymentIfExists(String deploymentName, String reasons)  {
            return deploymentOperations.getAsync(namespace, deploymentName)
                    .compose(dep -> {
                        if (dep != null) {
                            LOGGER.debugCr(reconciliation, "Rolling Deployment {} to {}", deploymentName, reasons);
                            return deploymentOperations.rollingUpdate(reconciliation, namespace, deploymentName, operationTimeoutMs);
                        } else {
                            return Future.succeededFuture();
                        }
                    });
        }

        /**
         * Does rolling update of Kafka pods based on the annotation on Pod level
         *
         * @param sts   The Kafka StatefulSet definition needed for the rolling update
         *
         * @return  Future with the result of the rolling update
         */
        Future<Void> kafkaManualPodRollingUpdate(StatefulSet sts) {
            return podOperations.listAsync(namespace, kafkaCluster.getSelectorLabels())
                    .compose(pods -> {
                        List<String> podsToRoll = new ArrayList<>(0);

                        for (Pod pod : pods)    {
                            if (Annotations.booleanAnnotation(pod, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                                podsToRoll.add(pod.getMetadata().getName());
                            }
                        }

                        if (!podsToRoll.isEmpty())  {
                            return maybeRollKafka(sts, pod -> {
                                if (pod != null && podsToRoll.contains(pod.getMetadata().getName())) {
                                    LOGGER.debugCr(reconciliation, "Rolling Kafka pod {} due to manual rolling update annotation on a pod", pod.getMetadata().getName());
                                    return singletonList("manual rolling update annotation on a pod");
                                } else {
                                    return new ArrayList<>();
                                }
                            });
                        } else {
                            return Future.succeededFuture();
                        }
                    });
        }

        /**
         * Does manual rolling update of Kafka pods based on an annotation on the StatefulSet or on the Pods. Annotation
         * on StatefulSet level triggers rolling update of all pods. Annotation on pods trigeres rolling update only of
         * the selected pods. If the annotation is present on both StatefulSet and one or more pods, only one rolling
         * update of all pods occurs.
         *
         * @return  Future with the result of the rolling update
         */
        Future<ReconciliationState> kafkaManualRollingUpdate() {
            Future<StatefulSet> futsts = kafkaSetOperations.getAsync(namespace, KafkaCluster.kafkaClusterName(name));
            if (futsts != null) {
                return futsts.compose(sts -> {
                    if (sts != null) {
                        if (Annotations.booleanAnnotation(sts, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                            // User trigger rolling update of the whole StatefulSet
                            return maybeRollKafka(sts, pod -> {
                                if (pod == null) {
                                    throw new ConcurrentDeletionException("Unexpectedly pod no longer exists during roll of StatefulSet.");
                                }
                                LOGGER.debugCr(reconciliation, "Rolling Kafka pod {} due to manual rolling update annotation",
                                        pod.getMetadata().getName());
                                return singletonList("manual rolling update");
                            });
                        } else {
                            // The STS is not annotated to roll all pods.
                            // But maybe the individual pods are annotated to restart only some of them.
                            return kafkaManualPodRollingUpdate(sts);
                        }
                    } else {
                        // STS does not exist => nothing to roll
                        return Future.succeededFuture();
                    }
                }).map(i -> this);
            }
            return Future.succeededFuture(this);
        }

        /**
         * Does rolling update of Zoo pods based on the annotation on Pod level
         *
         * @param sts   The Zoo StatefulSet definition needed for the rolling update
         *
         * @return  Future with the result of the rolling update
         */
        Future<Void> zkManualPodRollingUpdate(StatefulSet sts) {
            return podOperations.listAsync(namespace, zkCluster.getSelectorLabels())
                    .compose(pods -> {
                        List<String> podsToRoll = new ArrayList<>(0);

                        for (Pod pod : pods)    {
                            if (Annotations.booleanAnnotation(pod, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                                podsToRoll.add(pod.getMetadata().getName());
                            }
                        }

                        if (!podsToRoll.isEmpty())  {
                            return zkSetOperations.maybeRollingUpdate(reconciliation, sts, pod -> {
                                if (pod != null && podsToRoll.contains(pod.getMetadata().getName())) {
                                    LOGGER.debugCr(reconciliation, "Rolling ZooKeeper pod {} due to manual rolling update annotation on a pod", pod.getMetadata().getName());
                                    return singletonList("manual rolling update annotation on a pod");
                                } else {
                                    return null;
                                }
                            });
                        } else {
                            return Future.succeededFuture();
                        }
                    });
        }

        /**
         * Does manual rolling update of Zoo pods based on an annotation on the StatefulSet or on the Pods. Annotation
         * on StatefulSet level triggers rolling update of all pods. Annotation on pods trigeres rolling update only of
         * the selected pods. If the annotation is present on both StatefulSet and one or more pods, only one rolling
         * update of all pods occurs.
         *
         * @return  Future with the result of the rolling update
         */
        Future<ReconciliationState> zkManualRollingUpdate() {
            Future<StatefulSet> futsts = zkSetOperations.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name));
            if (futsts != null) {
                return futsts.compose(sts -> {
                    if (sts != null) {
                        if (Annotations.booleanAnnotation(sts, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                            // User trigger rolling update of the whole StatefulSet
                            return zkSetOperations.maybeRollingUpdate(reconciliation, sts, pod -> {
                                LOGGER.debugCr(reconciliation, "Rolling Zookeeper pod {} due to manual rolling update",
                                        pod.getMetadata().getName());
                                return singletonList("manual rolling update");
                            });
                        } else {
                            // The STS is not annotated to roll all pods.
                            // But maybe the individual pods are annotated to restart only some of them.
                            return zkManualPodRollingUpdate(sts);
                        }
                    } else {
                        // STS does not exist => nothing to roll
                        return Future.succeededFuture();
                    }
                }).map(i -> this);
            }
            return Future.succeededFuture(this);
        }

        Future<ReconciliationState> zkVersionChange() {
            if (versionChange.isNoop()) {
                LOGGER.debugCr(reconciliation, "Kafka.spec.kafka.version is unchanged therefore no change to Zookeeper is required");
            } else {
                String versionChangeType;

                if (versionChange.isDowngrade()) {
                    versionChangeType = "downgrade";
                } else {
                    versionChangeType = "upgrade";
                }

                if (versionChange.requiresZookeeperChange()) {
                    LOGGER.infoCr(reconciliation, "Kafka {} from {} to {} requires Zookeeper {} from {} to {}",
                            versionChangeType,
                            versionChange.from().version(),
                            versionChange.to().version(),
                            versionChangeType,
                            versionChange.from().zookeeperVersion(),
                            versionChange.to().zookeeperVersion());
                } else {
                    LOGGER.infoCr(reconciliation, "Kafka {} from {} to {} requires no change in Zookeeper version",
                            versionChangeType,
                            versionChange.from().version(),
                            versionChange.to().version());

                }

                // Get the zookeeper image currently set in the Kafka CR or, if that is not set, the image from the target Kafka version
                String newZkImage = versions.kafkaImage(kafkaAssembly.getSpec().getZookeeper().getImage(), versionChange.to().version());
                LOGGER.debugCr(reconciliation, "Setting new Zookeeper image: " + newZkImage);
                this.zkCluster.setImage(newZkImage);
            }

            return Future.succeededFuture(this);
        }

        /**
         * If the STS exists, complete any pending rolls
         *
         * @return A Future which completes with the current state of the STS, or with null if the STS never existed.
         */
        public Future<Void> waitForQuiescence(StatefulSet sts) {
            if (sts != null) {
                return maybeRollKafka(sts,
                    pod -> {
                        boolean notUpToDate = !isPodUpToDate(sts, pod);
                        List<String> reason = emptyList();
                        if (notUpToDate) {
                            LOGGER.debugCr(reconciliation, "Rolling pod {} prior to upgrade", pod.getMetadata().getName());
                            reason = singletonList("upgrade quiescence");
                        }
                        return reason;
                    });
            } else {
                return Future.succeededFuture();
            }
        }

        /**
         * Analyses the Kafka broker versions together with log.message.format.version and inter.broker.protocol.version
         * fields and decides how to deal with possible upgrade or downgrade. When no version change is happening, it
         * just sets the default values for log.message.format.version and inter.broker.protocol.version fields. If
         * there is some version change happening, it either sets the versions for it or it throws an exception if
         * version change is not possible.
         *
         * @return
         */
        Future<ReconciliationState> prepareVersionChange() {
            if (versionChange.isNoop()) {
                LOGGER.debugCr(reconciliation, "{}: No Kafka version change", reconciliation);

                if (kafkaCluster.getInterBrokerProtocolVersion() == null) {
                    // When IBPV is not set, we set it to current Kafka version
                    kafkaCluster.setInterBrokerProtocolVersion(kafkaCluster.getKafkaVersion().protocolVersion());

                    if (highestInterBrokerProtocolVersion != null
                            && !kafkaCluster.getKafkaVersion().protocolVersion().equals(highestInterBrokerProtocolVersion)) {
                        LOGGER.infoCr(reconciliation, "Upgrading Kafka inter.broker.protocol.version from {} to {}", highestInterBrokerProtocolVersion, kafkaCluster.getKafkaVersion().protocolVersion());

                        if (kafkaCluster.getLogMessageFormatVersion() == null
                            && highestLogMessageFormatVersion != null) {
                            // IBPV and LMFV should not change in the same rolling update. When this rolling update is going
                            // to change the IPBV, we keep the old LGFV
                            kafkaCluster.setLogMessageFormatVersion(highestLogMessageFormatVersion);
                        }
                    }
                }

                if (kafkaCluster.getLogMessageFormatVersion() == null) {
                    // When LMFV is not set, we set it to current Kafka version
                    kafkaCluster.setLogMessageFormatVersion(kafkaCluster.getKafkaVersion().messageVersion());

                    if (highestLogMessageFormatVersion != null &&
                            !kafkaCluster.getKafkaVersion().messageVersion().equals(highestLogMessageFormatVersion)) {
                        LOGGER.infoCr(reconciliation, "Upgrading Kafka log.message.format.version from {} to {}", highestLogMessageFormatVersion, kafkaCluster.getKafkaVersion().messageVersion());
                    }
                }

                return Future.succeededFuture(this);
            } else {
                if (versionChange.isUpgrade()) {
                    LOGGER.infoCr(reconciliation, "Kafka is upgrading from {} to {}", versionChange.from().version(), versionChange.to().version());

                    // We make sure that the highest log.message.format.version or inter.broker.protocol.version version
                    // used by any of the brokers is not higher than the broker version we upgrade from.
                    if ((highestLogMessageFormatVersion != null && compareDottedVersions(versionChange.from().messageVersion(), highestLogMessageFormatVersion) < 0)
                            || (highestInterBrokerProtocolVersion != null && compareDottedVersions(versionChange.from().protocolVersion(), highestInterBrokerProtocolVersion) < 0)) {
                        LOGGER.warnCr(reconciliation, "log.message.format.version ({}) and inter.broker.protocol.version ({}) used by the brokers have to be lower or equal to the Kafka broker version we upgrade from ({})", highestLogMessageFormatVersion, highestInterBrokerProtocolVersion, versionChange.from().version());
                        throw new KafkaUpgradeException("log.message.format.version (" + highestLogMessageFormatVersion + ") and inter.broker.protocol.version (" + highestInterBrokerProtocolVersion + ") used by the brokers have to be lower or equal to the Kafka broker version we upgrade from (" + versionChange.from().version() + ")");
                    }

                    String desiredLogMessageFormat = kafkaCluster.getLogMessageFormatVersion();
                    String desiredInterBrokerProtocol = kafkaCluster.getInterBrokerProtocolVersion();

                    // The desired log.message.format.version will be configured in the new brokers. And therefore it
                    // cannot be higher that the Kafka version we are upgrading from. If it is, we override it with the
                    // version we are upgrading from. If it is not set, we set it to the version we are upgrading from.
                    if (desiredLogMessageFormat == null
                            || compareDottedVersions(versionChange.from().messageVersion(), desiredLogMessageFormat) < 0) {
                        kafkaCluster.setLogMessageFormatVersion(versionChange.from().messageVersion());
                    }

                    // The desired inter.broker.protocol.version will be configured in the new brokers. And therefore it
                    // cannot be higher that the Kafka version we are upgrading from. If it is, we override it with the
                    // version we are upgrading from. If it is not set, we set it to the version we are upgrading from.
                    if (desiredInterBrokerProtocol == null
                            || compareDottedVersions(versionChange.from().protocolVersion(), desiredInterBrokerProtocol) < 0) {
                        kafkaCluster.setInterBrokerProtocolVersion(versionChange.from().protocolVersion());
                    }
                } else {
                    // Has to be a downgrade
                    LOGGER.infoCr(reconciliation, "Kafka is downgrading from {} to {}", versionChange.from().version(), versionChange.to().version());

                    // The currently used log.message.format.version and inter.broker.protocol.version cannot be higher
                    // than the version we are downgrading to. If it is we fail the reconciliation. If they are not set,
                    // we assume that it will use the default value which is the from version. In such case we fail the
                    // reconciliation as well.
                    if (highestLogMessageFormatVersion == null
                            || compareDottedVersions(versionChange.to().messageVersion(), highestLogMessageFormatVersion) < 0
                            || highestInterBrokerProtocolVersion == null
                            || compareDottedVersions(versionChange.to().protocolVersion(), highestInterBrokerProtocolVersion) < 0) {
                        LOGGER.warnCr(reconciliation, "log.message.format.version ({}) and inter.broker.protocol.version ({}) used by the brokers have to be set and be lower or equal to the Kafka broker version we downgrade to ({})", highestLogMessageFormatVersion, highestInterBrokerProtocolVersion, versionChange.to().version());
                        throw new KafkaUpgradeException("log.message.format.version (" + highestLogMessageFormatVersion + ") and inter.broker.protocol.version (" + highestInterBrokerProtocolVersion + ") used by the brokers have to be set and be lower or equal to the Kafka broker version we downgrade to (" + versionChange.to().version() + ")");
                    }

                    String desiredLogMessageFormat = kafkaCluster.getLogMessageFormatVersion();
                    String desiredInterBrokerProtocol = kafkaCluster.getInterBrokerProtocolVersion();

                    // If log.message.format.version is not set, we set it to the version we are downgrading to.
                    if (desiredLogMessageFormat == null) {
                        desiredLogMessageFormat = versionChange.to().messageVersion();
                        kafkaCluster.setLogMessageFormatVersion(versionChange.to().messageVersion());
                    }

                    // If inter.broker.protocol.version is not set, we set it to the version we are downgrading to.
                    if (desiredInterBrokerProtocol == null) {
                        desiredInterBrokerProtocol = versionChange.to().protocolVersion();
                        kafkaCluster.setInterBrokerProtocolVersion(versionChange.to().protocolVersion());
                    }

                    // Either log.message.format.version or inter.broker.protocol.version are higher than the Kafka
                    // version we are downgrading to. This should normally not happen since that should not pass the CR
                    // validation. But we still double check it as safety.
                    if (compareDottedVersions(versionChange.to().messageVersion(), desiredLogMessageFormat) < 0
                            || compareDottedVersions(versionChange.to().protocolVersion(), desiredInterBrokerProtocol) < 0) {
                        LOGGER.warnCr(reconciliation, "log.message.format.version ({}) and inter.broker.protocol.version ({}) used in the Kafka CR have to be set and be lower or equal to the Kafka broker version we downgrade to ({})", highestLogMessageFormatVersion, highestInterBrokerProtocolVersion, versionChange.to().version());
                        throw new KafkaUpgradeException("log.message.format.version and inter.broker.protocol.version used in the Kafka CR have to be set and be lower or equal to the Kafka broker version we downgrade to");
                    }
                }
            }

            return Future.succeededFuture(this);
        }

        protected CompositeFuture adminClientSecrets() {
            Future<Secret> clusterCaCertSecretFuture = secretOperations.getAsync(
                namespace, KafkaResources.clusterCaCertificateSecretName(name)).compose(secret -> {
                    if (secret == null) {
                        return Future.failedFuture(Util.missingSecretException(namespace, KafkaCluster.clusterCaCertSecretName(name)));
                    } else {
                        return Future.succeededFuture(secret);
                    }
                });
            Future<Secret> coKeySecretFuture = secretOperations.getAsync(
                namespace, ClusterOperator.secretName(name)).compose(secret -> {
                    if (secret == null) {
                        return Future.failedFuture(Util.missingSecretException(namespace, ClusterOperator.secretName(name)));
                    } else {
                        return Future.succeededFuture(secret);
                    }
                });
            return CompositeFuture.join(clusterCaCertSecretFuture, coKeySecretFuture);
        }

        /**
         * Rolls Kafka pods if needed
         *
         * @param sts Kafka statefullset
         * @param podNeedsRestart this function serves as a predicate whether to roll pod or not
         *
         * @return succeeded future if kafka pod was rolled and is ready
         */
        Future<Void> maybeRollKafka(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart) {
            return maybeRollKafka(sts, podNeedsRestart, true);
        }

        /**
         * Rolls Kafka pods if needed
         *
         * @param sts Kafka statefullset
         * @param podNeedsRestart this function serves as a predicate whether to roll pod or not
         * @param allowReconfiguration defines whether the rolling update should also attempt to do dynamic reconfiguration or not
         *
         * @return succeeded future if kafka pod was rolled and is ready
         */
        Future<Void> maybeRollKafka(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, boolean allowReconfiguration) {
            return adminClientSecrets()
                .compose(compositeFuture -> new KafkaRoller(reconciliation, vertx, podOperations, 1_000, operationTimeoutMs,
                    () -> new BackOff(250, 2, 10), sts, compositeFuture.resultAt(0), compositeFuture.resultAt(1), adminClientProvider,
                        kafkaCluster.getBrokersConfiguration(), kafkaLogging, kafkaCluster.getKafkaVersion(), allowReconfiguration)
                    .rollingRestart(podNeedsRestart));
        }

        Future<ReconciliationState> getZookeeperDescription() {
            return zkSetOperations.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name))
                    .compose(sts -> {
                        Storage oldStorage = getOldStorage(sts);

                        if (sts != null && sts.getSpec() != null)   {
                            this.zkCurrentReplicas = sts.getSpec().getReplicas();
                        }

                        this.zkCluster = ZookeeperCluster.fromCrd(reconciliation, kafkaAssembly, versions, oldStorage, zkCurrentReplicas != null ? zkCurrentReplicas : 0);

                        // We are upgrading from previous Strimzi version which has a sidecars. The older sidecar
                        // configurations allowed only older versions of TLS to be used by default. But the Zookeeper
                        // native TLS support enabled by default only secure TLSv1.2. That is correct, but makes the
                        // upgrade hard since Kafka will be unable to connect. So in the first roll, we enable also
                        // older TLS versions in Zookeeper so that we can configure the Kafka sidecars to enable
                        // TLSv1.2 as well. This will be removed again in the next rolling update of Zookeeper -> done
                        // only when Kafka is ready for it.
                        if (sts != null
                                && sts.getSpec() != null
                                && sts.getSpec().getTemplate().getSpec().getContainers().size() > 1)   {
                            zkCluster.getConfiguration().setConfigOption("ssl.protocol", "TLS");
                            zkCluster.getConfiguration().setConfigOption("ssl.enabledProtocols", "TLSv1.2,TLSv1.1,TLSv1");
                        }

                        return Util.metricsAndLogging(reconciliation, configMapOperations, kafkaAssembly.getMetadata().getNamespace(),
                                zkCluster.getLogging(),
                                zkCluster.getMetricsConfigInCm());
                    })
                    .compose(metricsAndLogging -> {
                        ConfigMap logAndMetricsConfigMap = zkCluster.generateConfigurationConfigMap(metricsAndLogging);
                        this.zkMetricsAndLogsConfigMap = logAndMetricsConfigMap;

                        String loggingConfiguration = zkMetricsAndLogsConfigMap.getData().get(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG);
                        this.zkLoggingHash = Util.stringHash(loggingConfiguration);

                        return Future.succeededFuture(this);
                    });
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

        Future<ReconciliationState> zookeeperServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(reconciliation, namespace,
                    ZookeeperCluster.containerServiceAccountName(zkCluster.getCluster()),
                    zkCluster.generateServiceAccount()));
        }

        Future<ReconciliationState> zkService() {
            return withVoid(serviceOperations.reconcile(reconciliation, namespace, zkCluster.getServiceName(), zkCluster.generateService()));
        }

        Future<ReconciliationState> zkHeadlessService() {
            return withVoid(serviceOperations.reconcile(reconciliation, namespace, zkCluster.getHeadlessServiceName(), zkCluster.generateHeadlessService()));
        }

        Future<ReconciliationState> zkAncillaryCm() {
            return withVoid(configMapOperations.reconcile(reconciliation, namespace, zkCluster.getAncillaryConfigMapName(), zkMetricsAndLogsConfigMap));
        }

        /**
         * Reconciles Secret with certificates and evaluates whether any existing certificates inside changed.
         *
         * @param secretName    Name of the secret
         * @param secret        The new secret
         * @return              Future with True if the existing certificates changed and False if they didn't
         */
        Future<Boolean> updateCertificateSecretWithDiff(String secretName, Secret secret)   {
            return secretOperations.getAsync(namespace, secretName)
                    .compose(oldSecret -> secretOperations.reconcile(reconciliation, namespace, secretName, secret)
                            .map(res -> {
                                if (res instanceof ReconcileResult.Patched) {
                                    // The secret is patched and some changes to the existing certificates actually occured
                                    return ModelUtils.doExistingCertificatesDiffer(oldSecret, res.resource());
                                }

                                return false;
                            })
                    );
        }

        Future<ReconciliationState> zkNodesSecret() {
            return updateCertificateSecretWithDiff(ZookeeperCluster.nodesSecretName(name), zkCluster.generateNodesSecret())
                    .map(changed -> {
                        existingZookeeperCertsChanged = changed;
                        return this;
                    });
        }

        Future<ReconciliationState> zkNetPolicy() {
            return withVoid(networkPolicyOperator.reconcile(reconciliation, namespace, ZookeeperCluster.policyName(name), zkCluster.generateNetworkPolicy(operatorNamespace, operatorNamespaceLabels)));
        }

        Future<ReconciliationState> zkPodDisruptionBudget() {
            return withVoid(podDisruptionBudgetOperator.reconcile(reconciliation, namespace, zkCluster.getName(), zkCluster.generatePodDisruptionBudget()));
        }

        Future<ReconciliationState> zkStatefulSet() {
            StatefulSet zkSts = zkCluster.generateStatefulSet(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets);
            Annotations.annotations(zkSts.getSpec().getTemplate()).put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(getCaCertGeneration(this.clusterCa)));
            Annotations.annotations(zkSts.getSpec().getTemplate()).put(Annotations.ANNO_STRIMZI_LOGGING_HASH, zkLoggingHash);
            return withZkDiff(zkSetOperations.reconcile(reconciliation, namespace, zkCluster.getName(), zkSts));
        }

        Future<ReconciliationState> zkRollingUpdate() {
            // Scale-down and Scale-up might have change the STS. we should get a fresh one.
            return zkSetOperations.getAsync(namespace, ZookeeperCluster.zookeeperClusterName(name))
                    .compose(sts -> zkSetOperations.maybeRollingUpdate(reconciliation, sts,
                        pod -> getReasonsToRestartPod(zkDiffs.resource(), pod, existingZookeeperCertsChanged, this.clusterCa)))
                    .map(this);
        }

        /**
         * Prepares the Zookeeper connectionString
         * The format is host1:port1,host2:port2,...
         *
         * Used by the Zookeeper 3.5 Admin client for scaling
         *
         * @param connectToReplicas     Number of replicas from the ZK STS which should be used
         * @return                      The generated Zookeeper connection string
         */
        String zkConnectionString(int connectToReplicas, Function<Integer, String> zkNodeAddress)  {
            // Prepare Zoo connection string. We want to connect only to nodes which existed before
            // scaling and will exist after it is finished
            List<String> zooNodes = new ArrayList<>(connectToReplicas);

            for (int i = 0; i < connectToReplicas; i++)   {
                zooNodes.add(String.format("%s:%d",
                        zkNodeAddress.apply(i),
                        ZookeeperCluster.CLIENT_TLS_PORT));
            }

            return  String.join(",", zooNodes);
        }

        /**
         * Helper method for getting the required secrets with certificates and creating the ZookeeperScaler instance
         * for the given cluster. The ZookeeperScaler instance created by this method should be closed manually after
         * it is not used anymore.
         *
         * @param connectToReplicas     Number of pods from the Zookeeper STS which the scaler should use
         * @return                      Zookeeper scaler instance.
         */
        Future<ZookeeperScaler> zkScaler(int connectToReplicas)  {
            Future<Secret> clusterCaCertSecretFuture = secretOperations.getAsync(namespace, KafkaResources.clusterCaCertificateSecretName(name));
            Future<Secret> coKeySecretFuture = secretOperations.getAsync(namespace, ClusterOperator.secretName(name));

            return CompositeFuture.join(clusterCaCertSecretFuture, coKeySecretFuture)
                    .compose(compositeFuture -> {
                        // Handle Cluster CA for connecting to Zoo
                        Secret clusterCaCertSecret = compositeFuture.resultAt(0);
                        if (clusterCaCertSecret == null) {
                            return Future.failedFuture(Util.missingSecretException(namespace, KafkaCluster.clusterCaKeySecretName(name)));
                        }

                        // Handle CO key for connecting to Zoo
                        Secret coKeySecret = compositeFuture.resultAt(1);
                        if (coKeySecret == null) {
                            return Future.failedFuture(Util.missingSecretException(namespace, ClusterOperator.secretName(name)));
                        }

                        Function<Integer, String> zkNodeAddress = (Integer i) ->
                                DnsNameGenerator.podDnsNameWithoutClusterDomain(namespace,
                                        KafkaResources.zookeeperHeadlessServiceName(name), zkCluster.getPodName(i));

                        ZookeeperScaler zkScaler = zkScalerProvider.createZookeeperScaler(reconciliation, vertx, zkConnectionString(connectToReplicas, zkNodeAddress), zkNodeAddress, clusterCaCertSecret, coKeySecret, operationTimeoutMs);

                        return Future.succeededFuture(zkScaler);
                    });
        }

        Future<ReconciliationState> zkScalingUp() {
            int desired = zkCluster.getReplicas();

            if (zkCurrentReplicas != null
                    && zkCurrentReplicas < desired) {
                LOGGER.infoCr(reconciliation, "Scaling Zookeeper up from {} to {} replicas", zkCurrentReplicas, desired);

                return zkScaler(zkCurrentReplicas)
                        .compose(zkScaler -> {
                            Promise<ReconciliationState> scalingPromise = Promise.promise();

                            zkScalingUpByOne(zkScaler, zkCurrentReplicas, desired)
                                    .onComplete(res -> {
                                        zkScaler.close();

                                        if (res.succeeded())    {
                                            scalingPromise.complete(res.result());
                                        } else {
                                            LOGGER.warnCr(reconciliation, "Failed to scale Zookeeper", res.cause());
                                            scalingPromise.fail(res.cause());
                                        }
                                    });

                            return scalingPromise.future();
                        });
            } else {
                // No scaling up => do nothing
                return Future.succeededFuture(this);
            }
        }

        Future<ReconciliationState> zkScalingUpByOne(ZookeeperScaler zkScaler, int current, int desired) {
            if (current < desired) {
                return zkSetOperations.scaleUp(reconciliation, namespace, zkCluster.getName(), current + 1)
                        .compose(ignore -> podOperations.readiness(reconciliation, namespace, zkCluster.getPodName(current), 1_000, operationTimeoutMs))
                        .compose(ignore -> zkScaler.scale(current + 1))
                        .compose(ignore -> zkScalingUpByOne(zkScaler, current + 1, desired));
            } else {
                return Future.succeededFuture(this);
            }
        }

        Future<ReconciliationState> zkScalingDown() {
            int desired = zkCluster.getReplicas();

            if (zkCurrentReplicas != null
                    && zkCurrentReplicas > desired) {
                // With scaling
                LOGGER.infoCr(reconciliation, "Scaling Zookeeper down from {} to {} replicas", zkCurrentReplicas, desired);

                // No need to check for pod readiness since we run right after the readiness check
                return zkScaler(desired)
                        .compose(zkScaler -> {
                            Promise<ReconciliationState> scalingPromise = Promise.promise();

                            zkScalingDownByOne(zkScaler, zkCurrentReplicas, desired)
                                    .onComplete(res -> {
                                        zkScaler.close();

                                        if (res.succeeded())    {
                                            scalingPromise.complete(res.result());
                                        } else {
                                            LOGGER.warnCr(reconciliation, "Failed to scale Zookeeper", res.cause());
                                            scalingPromise.fail(res.cause());
                                        }
                                    });

                            return scalingPromise.future();
                        });
            } else {
                // No scaling down => do nothing
                return Future.succeededFuture(this);
            }
        }

        Future<ReconciliationState> zkScalingDownByOne(ZookeeperScaler zkScaler, int current, int desired) {
            if (current > desired) {
                return podsReady(zkCluster, current - 1)
                        .compose(ignore -> zkScaler.scale(current - 1))
                        .compose(ignore -> zkSetOperations.scaleDown(reconciliation, namespace, zkCluster.getName(), current - 1))
                        .compose(ignore -> zkScalingDownByOne(zkScaler, current - 1, desired));
            } else {
                return Future.succeededFuture(this);
            }
        }

        Future<ReconciliationState> zkScalingCheck() {
            // No scaling, but we should check the configuration
            // This can cover any previous failures in the Zookeeper reconfiguration
            LOGGER.debugCr(reconciliation, "Verifying that Zookeeper is configured to run with {} replicas", zkCurrentReplicas);

            // No need to check for pod readiness since we run right after the readiness check
            return zkScaler(zkCluster.getReplicas())
                    .compose(zkScaler -> {
                        Promise<ReconciliationState> scalingPromise = Promise.promise();

                        zkScaler.scale(zkCluster.getReplicas()).onComplete(res -> {
                            zkScaler.close();

                            if (res.succeeded())    {
                                scalingPromise.complete(this);
                            } else {
                                LOGGER.warnCr(reconciliation, "Failed to verify Zookeeper configuration", res.cause());
                                scalingPromise.fail(res.cause());
                            }
                        });

                        return scalingPromise.future();
                    });
        }

        Future<ReconciliationState> zkServiceEndpointReadiness() {
            return withVoid(serviceOperations.endpointReadiness(reconciliation, namespace, zkCluster.getServiceName(), 1_000, operationTimeoutMs));
        }

        Future<ReconciliationState> zkHeadlessServiceEndpointReadiness() {
            return withVoid(serviceOperations.endpointReadiness(reconciliation, namespace, zkCluster.getHeadlessServiceName(), 1_000, operationTimeoutMs));
        }

        Future<ReconciliationState> zkGenerateCertificates(Supplier<Date> dateSupplier) {
            Promise<ReconciliationState> resultPromise = Promise.promise();
            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        zkCluster.generateCertificates(kafkaAssembly, clusterCa, isMaintenanceTimeWindowsSatisfied(dateSupplier));
                        future.complete(this);
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                },
                true,
                resultPromise);
            return resultPromise.future();
        }

        /*test*/ Future<ReconciliationState> getKafkaClusterDescription() {
            return kafkaSetOperations.getAsync(namespace, KafkaCluster.kafkaClusterName(name))
                    .compose(sts -> {
                        Storage oldStorage = getOldStorage(sts);

                        this.kafkaCurrentReplicas = 0;
                        if (sts != null && sts.getSpec() != null)   {
                            this.kafkaCurrentReplicas = sts.getSpec().getReplicas();
                            this.currentStsVersion = Annotations.annotations(sts).get(ANNO_STRIMZI_IO_KAFKA_VERSION);
                            this.kafkaStsAlreadyExists = true;
                        }

                        this.kafkaCluster = KafkaCluster.fromCrd(reconciliation, kafkaAssembly, versions, oldStorage, kafkaCurrentReplicas);
                        this.kafkaBootstrapDnsName.addAll(ListenersUtils.alternativeNames(kafkaCluster.getListeners()));

                        //return Future.succeededFuture(this);
                        return podOperations.listAsync(namespace, this.kafkaCluster.getSelectorLabels());
                    }).compose(pods -> {
                        String lowestKafkaVersion = currentStsVersion;
                        String highestKafkaVersion = currentStsVersion;

                        for (Pod pod : pods)    {
                            // Collect the different annotations from the pods
                            String currentVersion = Annotations.stringAnnotation(pod, ANNO_STRIMZI_IO_KAFKA_VERSION, null);
                            String currentMessageFormat = Annotations.stringAnnotation(pod, KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION, null);
                            String currentIbp = Annotations.stringAnnotation(pod, KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION, null);

                            // We find the highest and lowest used Kafka version. This is used to detect any upgrades or
                            // downgrades which failed in the middle and continue with them.
                            if (currentVersion != null) {
                                if (highestKafkaVersion == null)  {
                                    highestKafkaVersion = currentVersion;
                                } else if (compareDottedVersions(highestKafkaVersion, currentVersion) < 0) {
                                    highestKafkaVersion = currentVersion;
                                }

                                if (lowestKafkaVersion == null)  {
                                    lowestKafkaVersion = currentVersion;
                                } else if (compareDottedVersions(lowestKafkaVersion, currentVersion) > 0) {
                                    lowestKafkaVersion = currentVersion;
                                }
                            }

                            // We find the highest used log.message.format.version. This is later used to validate
                            // upgrades or downgrades.
                            if (currentMessageFormat != null) {
                                if (highestLogMessageFormatVersion == null)  {
                                    highestLogMessageFormatVersion = currentMessageFormat;
                                } else if (compareDottedVersions(highestLogMessageFormatVersion, currentMessageFormat) < 0) {
                                    highestLogMessageFormatVersion = currentMessageFormat;
                                }
                            }

                            // We find the highest used inter.broker.protocol.version. This is later used to validate
                            // upgrades or downgrades.
                            if (currentIbp != null)  {
                                if (highestInterBrokerProtocolVersion == null)  {
                                    highestInterBrokerProtocolVersion = currentIbp;
                                } else if (compareDottedVersions(highestInterBrokerProtocolVersion, currentIbp) < 0) {
                                    highestInterBrokerProtocolVersion = currentIbp;
                                }
                            }
                        }

                        // We decide what is the current Kafka version used and create the KafkaVersionChange object
                        // describing the situation.
                        if (lowestKafkaVersion == null)   {
                            if (!kafkaStsAlreadyExists && pods.isEmpty())  {
                                // No version found in Pods or StatefulSet because they do not exist => This means we
                                // are dealing with a brand new Kafka cluster or a Kafka cluster with deleted
                                // StatefulSet and Pods. New cluster does not need an upgrade. And cluster without
                                // StatefulSet / Pods cannot be rolled. So we can just deploy a new one with the desired
                                // version. Therefore we can use the desired version and set the version change to noop.
                                this.versionChange = new KafkaVersionChange(kafkaCluster.getKafkaVersion(), kafkaCluster.getKafkaVersion());
                            } else {
                                // Either Pods or StatefulSet already exist. But none of them contains the version
                                // annotation. This suggests they are not created by the current versions of Strimzi.
                                // Without the annotation, we cannot detect the Kafka version and decide on upgrade.
                                LOGGER.warnCr(reconciliation, "Kafka Pods or StatefulSet exist, but do not contain the {} annotation to detect their version. Kafka upgrade cannot be detected.", ANNO_STRIMZI_IO_KAFKA_VERSION);
                                throw new KafkaUpgradeException("Kafka Pods or StatefulSet exist, but do not contain the " + ANNO_STRIMZI_IO_KAFKA_VERSION + " annotation to detect their version. Kafka upgrade cannot be detected.");
                            }
                        } else if (lowestKafkaVersion.equals(highestKafkaVersion)) {
                            // All brokers have the same version. We can use it as the current version.
                            this.versionChange = new KafkaVersionChange(versions.version(lowestKafkaVersion), kafkaCluster.getKafkaVersion());
                        } else if (compareDottedVersions(highestKafkaVersion, kafkaCluster.getKafkaVersion().version()) > 0)    {
                            // Highest Kafka version used by the brokers is higher then desired => suspected downgrade
                            this.versionChange = new KafkaVersionChange(versions.version(highestKafkaVersion), kafkaCluster.getKafkaVersion());
                        } else {
                            // Highest Kafka version used by the brokers is equal or lower than desired => suspected upgrade
                            this.versionChange = new KafkaVersionChange(versions.version(lowestKafkaVersion), kafkaCluster.getKafkaVersion());
                        }

                        return Future.succeededFuture(this);
                    });
        }

        Future<ReconciliationState> withKafkaDiff(Future<ReconcileResult<StatefulSet>> r) {
            return r.map(rr -> {
                this.kafkaDiffs = rr;
                return this;
            });
        }

        Future<ReconciliationState> kafkaInitServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(reconciliation, namespace,
                    kafkaCluster.getServiceAccountName(),
                    kafkaCluster.generateServiceAccount()));
        }

        Future<ReconciliationState> kafkaInitClusterRoleBinding() {
            ClusterRoleBinding desired = kafkaCluster.generateClusterRoleBinding(namespace);

            return withVoid(withIgnoreRbacError(reconciliation,
                    clusterRoleBindingOperations.reconcile(reconciliation,
                        KafkaResources.initContainerClusterRoleBindingName(name, namespace),
                        desired),
                    desired
            ));
        }

        Future<ReconciliationState> kafkaScaleDown() {
            return withVoid(kafkaSetOperations.scaleDown(reconciliation, namespace, kafkaCluster.getName(), kafkaCluster.getReplicas()));
        }

        /**
         * Makes sure all desired services are updated and the rest is deleted
         *
         * @return
         */
        Future<ReconciliationState> kafkaServices() {
            List<Service> services = new ArrayList<>();
            services.add(kafkaCluster.generateService());
            services.add(kafkaCluster.generateHeadlessService());
            services.addAll(kafkaCluster.generateExternalBootstrapServices());

            int replicas = kafkaCluster.getReplicas();
            for (int i = 0; i < replicas; i++) {
                services.addAll(kafkaCluster.generateExternalServices(i));
            }

            Future fut = serviceOperations.listAsync(namespace, kafkaCluster.getSelectorLabels())
                    .compose(existingServices -> {
                        List<Future> serviceFutures = new ArrayList<>(services.size());
                        List<String> existingServiceNames = existingServices.stream().map(svc -> svc.getMetadata().getName()).collect(Collectors.toList());

                        LOGGER.debugCr(reconciliation, "Reconciling existing Services {} against the desired services", existingServiceNames);

                        // Update desired services
                        for (Service service : services) {
                            String serviceName = service.getMetadata().getName();
                            existingServiceNames.remove(serviceName);
                            serviceFutures.add(serviceOperations.reconcile(reconciliation, namespace, serviceName, service));
                        }

                        LOGGER.debugCr(reconciliation, "Services {} should be deleted", existingServiceNames);

                        // Delete services which match our selector but are not desired anymore
                        for (String serviceName : existingServiceNames) {
                            serviceFutures.add(serviceOperations.reconcile(reconciliation, namespace, serviceName, null));
                        }

                        return CompositeFuture.join(serviceFutures);
                    });

            return withVoid(fut);
        }

        /**
         * Makes sure all desired routes are updated and the rest is deleted
         *
         * @return
         */
        Future<ReconciliationState> kafkaRoutes() {
            List<Route> routes = new ArrayList<>(kafkaCluster.generateExternalBootstrapRoutes());

            if (routes.size() > 0) {
                if (pfa.hasRoutes()) {
                    int replicas = kafkaCluster.getReplicas();
                    for (int i = 0; i < replicas; i++) {
                        routes.addAll(kafkaCluster.generateExternalRoutes(i));
                    }

                    Future fut = routeOperations.listAsync(namespace, kafkaCluster.getSelectorLabels())
                            .compose(existingRoutes -> {
                                List<Future> routeFutures = new ArrayList<>(routes.size());
                                List<String> existingRouteNames = existingRoutes.stream().map(route -> route.getMetadata().getName()).collect(Collectors.toList());

                                LOGGER.debugCr(reconciliation, "Reconciling existing Routes {} against the desired routes", existingRouteNames);

                                // Update desired routes
                                for (Route route : routes) {
                                    String routeName = route.getMetadata().getName();
                                    existingRouteNames.remove(routeName);
                                    routeFutures.add(routeOperations.reconcile(reconciliation, namespace, routeName, route));
                                }

                                LOGGER.debugCr(reconciliation, "Routes {} should be deleted", existingRouteNames);

                                // Delete routes which match our selector but are not desired anymore
                                for (String routeName : existingRouteNames) {
                                    routeFutures.add(routeOperations.reconcile(reconciliation, namespace, routeName, null));
                                }

                                return CompositeFuture.join(routeFutures);
                            });

                    return withVoid(fut);
                } else {
                    LOGGER.warnCr(reconciliation, "The OpenShift route API is not available in this Kubernetes cluster. Exposing Kafka cluster {} using routes is not possible.", name);
                    return withVoid(Future.failedFuture("The OpenShift route API is not available in this Kubernetes cluster. Exposing Kafka cluster " + name + " using routes is not possible."));
                }
            }

            return withVoid(Future.succeededFuture());
        }

        /**
         * Makes sure all desired ingresses are updated and the rest is deleted
         *
         * @return
         */
        Future<ReconciliationState> kafkaIngresses() {
            if (!pfa.hasIngressV1()) {
                return Future.succeededFuture(this);
            }

            List<Ingress> ingresses = new ArrayList<>(kafkaCluster.generateExternalBootstrapIngresses());

            int replicas = kafkaCluster.getReplicas();
            for (int i = 0; i < replicas; i++) {
                ingresses.addAll(kafkaCluster.generateExternalIngresses(i));
            }

            Future fut = ingressOperations.listAsync(namespace, kafkaCluster.getSelectorLabels())
                    .compose(existingIngresses -> {
                        List<Future> ingressFutures = new ArrayList<>(ingresses.size());
                        List<String> existingIngressNames = existingIngresses.stream().map(ingress -> ingress.getMetadata().getName()).collect(Collectors.toList());

                        LOGGER.debugCr(reconciliation, "Reconciling existing Ingresses {} against the desired ingresses", existingIngressNames);

                        // Update desired ingresses
                        for (Ingress ingress : ingresses) {
                            String ingressName = ingress.getMetadata().getName();
                            existingIngressNames.remove(ingressName);
                            ingressFutures.add(ingressOperations.reconcile(reconciliation, namespace, ingressName, ingress));
                        }

                        LOGGER.debugCr(reconciliation, "Ingresses {} should be deleted", existingIngressNames);

                        // Delete ingresses which match our selector but are not desired anymore
                        for (String ingressName : existingIngressNames) {
                            ingressFutures.add(ingressOperations.reconcile(reconciliation, namespace, ingressName, null));
                        }

                        return CompositeFuture.join(ingressFutures);
                    });

            return withVoid(fut);
        }

        /**
         * Makes sure all desired ingresses are updated and the rest is deleted
         *
         * @return
         */
        Future<ReconciliationState> kafkaIngressesV1Beta1() {
            if (pfa.hasIngressV1()) {
                return Future.succeededFuture(this);
            }

            List<io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress> ingresses = new ArrayList<>(kafkaCluster.generateExternalBootstrapIngressesV1Beta1());

            int replicas = kafkaCluster.getReplicas();
            for (int i = 0; i < replicas; i++) {
                ingresses.addAll(kafkaCluster.generateExternalIngressesV1Beta1(i));
            }

            Future fut = ingressV1Beta1Operations.listAsync(namespace, kafkaCluster.getSelectorLabels())
                    .compose(existingIngresses -> {
                        List<Future> ingressFutures = new ArrayList<>(ingresses.size());
                        List<String> existingIngressNames = existingIngresses.stream().map(ingress -> ingress.getMetadata().getName()).collect(Collectors.toList());

                        LOGGER.debugCr(reconciliation, "Reconciling existing v1beta1 Ingresses {} against the desired ingresses", existingIngressNames);

                        // Update desired ingresses
                        for (io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress ingress : ingresses) {
                            String ingressName = ingress.getMetadata().getName();
                            existingIngressNames.remove(ingressName);
                            ingressFutures.add(ingressV1Beta1Operations.reconcile(reconciliation, namespace, ingressName, ingress));
                        }

                        LOGGER.debugCr(reconciliation, "V1beta1 ingresses {} should be deleted", existingIngressNames);

                        // Delete ingresses which match our selector but are not desired anymore
                        for (String ingressName : existingIngressNames) {
                            ingressFutures.add(ingressV1Beta1Operations.reconcile(reconciliation, namespace, ingressName, null));
                        }

                        return CompositeFuture.join(ingressFutures);
                    });

            return withVoid(fut);
        }

        /**
         * Get the cluster Id of the Kafka cluster
         * 
         * @return
         */
        Future<ReconciliationState> kafkaGetClusterId() {
            return adminClientSecrets()
                .compose(compositeFuture -> {
                    LOGGER.debugCr(reconciliation, "Attempt to get clusterId");
                    Promise<ReconciliationState> resultPromise = Promise.promise();
                    vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                        future -> {
                            Admin kafkaAdmin = null;
                            try {
                                String bootstrapHostname = KafkaResources.bootstrapServiceName(this.name) + "." + this.namespace + ".svc:" + KafkaCluster.REPLICATION_PORT;
                                LOGGER.debugCr(reconciliation, "Creating AdminClient for clusterId using {}", bootstrapHostname);
                                kafkaAdmin = adminClientProvider.createAdminClient(bootstrapHostname, compositeFuture.resultAt(0), compositeFuture.resultAt(1), "cluster-operator");
                                kafkaStatus.setClusterId(kafkaAdmin.describeCluster().clusterId().get());
                            } catch (KafkaException e) {
                                LOGGER.warnCr(reconciliation, "Kafka exception getting clusterId {}", e.getMessage());
                            } catch (InterruptedException e) {
                                LOGGER.warnCr(reconciliation, "Interrupted exception getting clusterId {}", e.getMessage());
                            } catch (ExecutionException e) {
                                LOGGER.warnCr(reconciliation, "Execution exception getting clusterId {}", e.getMessage());
                            } finally {
                                if (kafkaAdmin != null) {
                                    kafkaAdmin.close();
                                }
                            }
                            future.complete(this);
                        },
                        true,
                        resultPromise);
                    return resultPromise.future();
                });
        }
        
        Future<ReconciliationState> kafkaInternalServicesReady()   {
            for (GenericKafkaListener listener : ListenersUtils.internalListeners(kafkaCluster.getListeners())) {
                boolean useServiceDnsDomain = (listener.getConfiguration() != null && listener.getConfiguration().getUseServiceDnsDomain() != null)
                        ? listener.getConfiguration().getUseServiceDnsDomain() : false;

                // Set status based on bootstrap service
                String bootstrapAddress = getInternalServiceHostname(ListenersUtils.backwardsCompatibleBootstrapServiceName(name, listener), useServiceDnsDomain);

                ListenerStatus ls = new ListenerStatusBuilder()
                        .withType(listener.getName())
                        .withAddresses(new ListenerAddressBuilder()
                                .withHost(bootstrapAddress)
                                .withPort(listener.getPort())
                                .build())
                        .build();

                addListenerStatus(ls);

                // Set advertised hostnames and ports
                for (int pod = 0; pod < kafkaCluster.getReplicas(); pod++) {
                    String brokerAddress;

                    if (useServiceDnsDomain) {
                        brokerAddress = DnsNameGenerator.podDnsNameWithClusterDomain(namespace, KafkaResources.brokersServiceName(name), KafkaResources.kafkaStatefulSetName(name) + "-" + pod);
                    } else {
                        brokerAddress = DnsNameGenerator.podDnsNameWithoutClusterDomain(namespace, KafkaResources.brokersServiceName(name), KafkaResources.kafkaStatefulSetName(name) + "-" + pod);
                    }

                    kafkaAdvertisedHostnames.add(kafkaCluster.getAdvertisedHostname(listener, pod, brokerAddress));
                    kafkaAdvertisedPorts.add(kafkaCluster.getAdvertisedPort(listener, pod, listener.getPort()));
                }
            }

            return Future.succeededFuture(this);
        }

        /**
         * Makes sure all services related to load balancers are ready and collects their addresses for Statuses,
         * certificates and advertised addresses. This method for all Load Balancer type listeners:
         *      1) Checks if the bootstrap service has been provisioned (has a loadbalancer address)
         *      2) Collects the relevant addresses and stores them for use in certificates and in CR status
         *      3) Checks it the broker services have been provisioned (have a loadbalancer address)
         *      4) Collects the loadbalancer addresses for certificates and advertised hostnames
         *
         * @return
         */
        Future<ReconciliationState> kafkaLoadBalancerServicesReady() {
            List<GenericKafkaListener> loadBalancerListeners = ListenersUtils.loadBalancerListeners(kafkaCluster.getListeners());
            List<Future> listenerFutures = new ArrayList<>(loadBalancerListeners.size());

            for (GenericKafkaListener listener : loadBalancerListeners) {
                String bootstrapServiceName = ListenersUtils.backwardsCompatibleBootstrapServiceName(name, listener);

                Future perListenerFut = serviceOperations.hasIngressAddress(reconciliation, namespace, bootstrapServiceName, 1_000, operationTimeoutMs)
                        .compose(res -> serviceOperations.getAsync(namespace, bootstrapServiceName))
                        .compose(svc -> {
                            String bootstrapAddress;

                            if (svc.getStatus().getLoadBalancer().getIngress().get(0).getHostname() != null)    {
                                bootstrapAddress = svc.getStatus().getLoadBalancer().getIngress().get(0).getHostname();
                            } else {
                                bootstrapAddress = svc.getStatus().getLoadBalancer().getIngress().get(0).getIp();
                            }

                            LOGGER.debugCr(reconciliation, "Found address {} for Service {}", bootstrapAddress, bootstrapServiceName);

                            kafkaBootstrapDnsName.add(bootstrapAddress);

                            ListenerStatus ls = new ListenerStatusBuilder()
                                    .withType(listener.getName())
                                    .withAddresses(new ListenerAddressBuilder()
                                            .withHost(bootstrapAddress)
                                            .withPort(listener.getPort())
                                            .build())
                                    .build();
                            addListenerStatus(ls);

                            return Future.succeededFuture();
                        })
                        .compose(res -> {
                            List<Future> perPodFutures = new ArrayList<>(kafkaCluster.getReplicas());

                            for (int pod = 0; pod < kafkaCluster.getReplicas(); pod++)  {
                                perPodFutures.add(
                                        serviceOperations.hasIngressAddress(reconciliation, namespace, ListenersUtils.backwardsCompatibleBrokerServiceName(name, pod, listener), 1_000, operationTimeoutMs)
                                );
                            }

                            return CompositeFuture.join(perPodFutures);
                        })
                        .compose(res -> {
                            List<Future> perPodFutures = new ArrayList<>(kafkaCluster.getReplicas());

                            for (int pod = 0; pod < kafkaCluster.getReplicas(); pod++)  {
                                final int podNumber = pod;
                                Future<Void> perBrokerFut = serviceOperations.getAsync(namespace, ListenersUtils.backwardsCompatibleBrokerServiceName(name, pod, listener))
                                        .compose(svc -> {
                                            String brokerAddress;

                                            if (svc.getStatus().getLoadBalancer().getIngress().get(0).getHostname() != null)    {
                                                brokerAddress = svc.getStatus().getLoadBalancer().getIngress().get(0).getHostname();
                                            } else {
                                                brokerAddress = svc.getStatus().getLoadBalancer().getIngress().get(0).getIp();
                                            }

                                            LOGGER.debugCr(reconciliation, "Found address {} for Service {}", brokerAddress, svc.getMetadata().getName());

                                            kafkaBrokerDnsNames.computeIfAbsent(podNumber, k -> new HashSet<>(2)).add(brokerAddress);

                                            String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, podNumber);
                                            if (advertisedHostname != null) {
                                                kafkaBrokerDnsNames.get(podNumber).add(ListenersUtils.brokerAdvertisedHost(listener, podNumber));
                                            }

                                            kafkaAdvertisedHostnames.add(kafkaCluster.getAdvertisedHostname(listener, podNumber, brokerAddress));
                                            kafkaAdvertisedPorts.add(kafkaCluster.getAdvertisedPort(listener, podNumber, listener.getPort()));

                                            return Future.succeededFuture();
                                        });

                                perPodFutures.add(perBrokerFut);
                            }

                            return CompositeFuture.join(perPodFutures);
                        });

                listenerFutures.add(perListenerFut);
            }

            return withVoid(CompositeFuture.join(listenerFutures));
        }

        /**
         * Makes sure all services related to node ports are ready and collects their addresses for Statuses,
         * certificates and advertised addresses. This method for all NodePort type listeners:
         *      1) Checks if the bootstrap service has been provisioned (has a node port)
         *      2) Collects the node port for use in CR status
         *      3) Checks it the broker services have been provisioned (have a node port)
         *      4) Collects the node ports for advertised hostnames
         *
         * @return
         */
        Future<ReconciliationState> kafkaNodePortServicesReady() {
            List<GenericKafkaListener> loadBalancerListeners = ListenersUtils.nodePortListeners(kafkaCluster.getListeners());
            List<Future> listenerFutures = new ArrayList<>(loadBalancerListeners.size());

            for (GenericKafkaListener listener : loadBalancerListeners) {
                String bootstrapServiceName = ListenersUtils.backwardsCompatibleBootstrapServiceName(name, listener);

                Future perListenerFut = serviceOperations.hasNodePort(reconciliation, namespace, bootstrapServiceName, 1_000, operationTimeoutMs)
                        .compose(res -> serviceOperations.getAsync(namespace, bootstrapServiceName))
                        .compose(svc -> {
                            Integer externalBootstrapNodePort = svc.getSpec().getPorts().get(0).getNodePort();
                            LOGGER.debugCr(reconciliation, "Found node port {} for Service {}", externalBootstrapNodePort, bootstrapServiceName);
                            kafkaBootstrapNodePorts.put(ListenersUtils.identifier(listener), externalBootstrapNodePort);

                            return Future.succeededFuture();
                        })
                        .compose(res -> {
                            List<Future> perPodFutures = new ArrayList<>(kafkaCluster.getReplicas());

                            for (int pod = 0; pod < kafkaCluster.getReplicas(); pod++)  {
                                perPodFutures.add(
                                        serviceOperations.hasNodePort(reconciliation, namespace, ListenersUtils.backwardsCompatibleBrokerServiceName(name, pod, listener), 1_000, operationTimeoutMs)
                                );
                            }

                            return CompositeFuture.join(perPodFutures);
                        })
                        .compose(res -> {
                            List<Future> perPodFutures = new ArrayList<>(kafkaCluster.getReplicas());

                            for (int pod = 0; pod < kafkaCluster.getReplicas(); pod++)  {
                                final int podNumber = pod;
                                Future<Void> perBrokerFut = serviceOperations.getAsync(namespace, ListenersUtils.backwardsCompatibleBrokerServiceName(name, pod, listener))
                                        .compose(svc -> {
                                            Integer externalBrokerNodePort = svc.getSpec().getPorts().get(0).getNodePort();
                                            LOGGER.debugCr(reconciliation, "Found node port {} for Service {}", externalBrokerNodePort, svc.getMetadata().getName());

                                            kafkaAdvertisedPorts.add(kafkaCluster.getAdvertisedPort(listener, podNumber, externalBrokerNodePort));

                                            String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, podNumber);

                                            if (advertisedHostname != null) {
                                                kafkaBrokerDnsNames.computeIfAbsent(podNumber, k -> new HashSet<>(1)).add(advertisedHostname);
                                            }

                                            kafkaAdvertisedHostnames.add(kafkaCluster.getAdvertisedHostname(listener, podNumber, nodePortAddressEnvVar(listener)));

                                            return Future.succeededFuture();
                                        });

                                perPodFutures.add(perBrokerFut);
                            }

                            return CompositeFuture.join(perPodFutures);
                        });

                listenerFutures.add(perListenerFut);
            }

            return withVoid(CompositeFuture.join(listenerFutures));
        }

        /**
         * Generates the name of the environment variable which will contain the advertised address for given listener. The
         * environment variable will be different for Node Port listeners which need to consume the address from the init
         * container corresponding to their preferred node.
         *
         * @param listener              The listener
         * @return                      The environment variable which will have the address
         */
        String nodePortAddressEnvVar(GenericKafkaListener listener)  {
            String preferredNodeAddressType;
            NodeAddressType preferredType = ListenersUtils.preferredNodeAddressType(listener);

            if (preferredType != null)  {
                preferredNodeAddressType = preferredType.toValue().toUpperCase(Locale.ENGLISH);
            } else {
                preferredNodeAddressType = "DEFAULT";
            }

            return String.format("${STRIMZI_NODEPORT_%s_ADDRESS}", preferredNodeAddressType);
        }

        Future<ReconciliationState> kafkaNodePortExternalListenerStatus() {
            List<Node> allNodes = new ArrayList<>();

            if (!ListenersUtils.nodePortListeners(kafkaCluster.getListeners()).isEmpty())   {
                return nodeOperator.listAsync(Labels.EMPTY)
                        .compose(result -> {
                            allNodes.addAll(result);
                            return podOperations.listAsync(namespace, kafkaCluster.getSelectorLabels());
                        })
                        .map(pods -> {
                            Map<Integer, Node> brokerNodes = new HashMap<>(kafkaCluster.getReplicas());

                            for (Pod broker : pods) {
                                String podName = broker.getMetadata().getName();
                                Integer podIndex = getPodIndexFromPodName(podName);

                                if (broker.getStatus() != null && broker.getStatus().getHostIP() != null) {
                                    String hostIP = broker.getStatus().getHostIP();
                                    Node podNode = allNodes.stream().filter(node -> {
                                        if (node.getStatus() != null && node.getStatus().getAddresses() != null)    {
                                            return null != node.getStatus().getAddresses().stream().filter(address -> hostIP.equals(address.getAddress())).findFirst().orElse(null);
                                        } else {
                                            return false;
                                        }
                                    }).findFirst().orElse(null);

                                    if (podNode != null) {
                                        brokerNodes.put(podIndex, podNode);
                                    }
                                }
                            }

                            for (GenericKafkaListener listener : ListenersUtils.nodePortListeners(kafkaCluster.getListeners())) {
                                // Set is used to ensure each node/port is listed only once. It is later converted to List.
                                Set<ListenerAddress> statusAddresses = new HashSet<>(brokerNodes.size());

                                for (Map.Entry<Integer, Node> entry : brokerNodes.entrySet())   {
                                    String advertisedHost = ListenersUtils.brokerAdvertisedHost(listener, entry.getKey());
                                    ListenerAddress address;

                                    if (advertisedHost != null)    {
                                        address = new ListenerAddressBuilder()
                                                .withHost(advertisedHost)
                                                .withPort(kafkaBootstrapNodePorts.get(ListenersUtils.identifier(listener)))
                                                .build();
                                    } else {
                                        address = new ListenerAddressBuilder()
                                                .withHost(NodeUtils.findAddress(entry.getValue().getStatus().getAddresses(), ListenersUtils.preferredNodeAddressType(listener)))
                                                .withPort(kafkaBootstrapNodePorts.get(ListenersUtils.identifier(listener)))
                                                .build();
                                    }

                                    statusAddresses.add(address);
                                }

                                ListenerStatus ls = new ListenerStatusBuilder()
                                        .withType(listener.getName())
                                        .withAddresses(new ArrayList<>(statusAddresses))
                                        .build();
                                addListenerStatus(ls);
                            }

                            return this;
                        });
            } else {
                return Future.succeededFuture(this);
            }
        }

        /**
         * Makes sure all routes are ready and collects their addresses for Statuses,
         * certificates and advertised addresses. This method for all routes:
         *      1) Checks if the bootstrap route has been provisioned (has a loadbalancer address)
         *      2) Collects the relevant addresses and stores them for use in certificates and in CR status
         *      3) Checks it the broker routes have been provisioned (have an address)
         *      4) Collects the route addresses for certificates and advertised hostnames
         *
         * @return
         */
        Future<ReconciliationState> kafkaRoutesReady() {
            List<GenericKafkaListener> routeListeners = ListenersUtils.routeListeners(kafkaCluster.getListeners());
            List<Future> listenerFutures = new ArrayList<>(routeListeners.size());

            for (GenericKafkaListener listener : routeListeners) {
                String bootstrapRouteName = ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(name, listener);

                Future perListenerFut = routeOperations.hasAddress(reconciliation, namespace, bootstrapRouteName, 1_000, operationTimeoutMs)
                        .compose(res -> routeOperations.getAsync(namespace, bootstrapRouteName))
                        .compose(route -> {
                            String bootstrapAddress = route.getStatus().getIngress().get(0).getHost();
                            LOGGER.debugCr(reconciliation, "Found address {} for Route {}", bootstrapAddress, bootstrapRouteName);

                            kafkaBootstrapDnsName.add(bootstrapAddress);

                            ListenerStatus ls = new ListenerStatusBuilder()
                                    .withType(listener.getName())
                                    .withAddresses(new ListenerAddressBuilder()
                                            .withHost(bootstrapAddress)
                                            .withPort(kafkaCluster.getRoutePort())
                                            .build())
                                    .build();
                            addListenerStatus(ls);

                            return Future.succeededFuture();
                        })
                        .compose(res -> {
                            List<Future> perPodFutures = new ArrayList<>(kafkaCluster.getReplicas());

                            for (int pod = 0; pod < kafkaCluster.getReplicas(); pod++)  {
                                perPodFutures.add(
                                        routeOperations.hasAddress(reconciliation, namespace, ListenersUtils.backwardsCompatibleBrokerServiceName(name, pod, listener), 1_000, operationTimeoutMs)
                                );
                            }

                            return CompositeFuture.join(perPodFutures);
                        })
                        .compose(res -> {
                            List<Future> perPodFutures = new ArrayList<>(kafkaCluster.getReplicas());

                            for (int pod = 0; pod < kafkaCluster.getReplicas(); pod++)  {
                                final int podNumber = pod;
                                Future<Void> perBrokerFut = routeOperations.getAsync(namespace, ListenersUtils.backwardsCompatibleBrokerServiceName(name, pod, listener))
                                        .compose(route -> {
                                            String brokerAddress = route.getStatus().getIngress().get(0).getHost();
                                            LOGGER.debugCr(reconciliation, "Found address {} for Route {}", brokerAddress, route.getMetadata().getName());

                                            kafkaBrokerDnsNames.computeIfAbsent(podNumber, k -> new HashSet<>(2)).add(brokerAddress);

                                            String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, podNumber);
                                            if (advertisedHostname != null) {
                                                kafkaBrokerDnsNames.get(podNumber).add(ListenersUtils.brokerAdvertisedHost(listener, podNumber));
                                            }

                                            kafkaAdvertisedHostnames.add(kafkaCluster.getAdvertisedHostname(listener, podNumber, brokerAddress));
                                            kafkaAdvertisedPorts.add(kafkaCluster.getAdvertisedPort(listener, podNumber, kafkaCluster.getRoutePort()));

                                            return Future.succeededFuture();
                                        });

                                perPodFutures.add(perBrokerFut);
                            }

                            return CompositeFuture.join(perPodFutures);
                        });

                listenerFutures.add(perListenerFut);
            }

            return withVoid(CompositeFuture.join(listenerFutures));
        }

        /**
         * Makes sure all ingresses are ready and collects their addresses for Statuses,
         * certificates and advertised addresses. This method for all ingresses:
         *      1) Checks if the bootstrap ingress has been provisioned (has a loadbalancer address)
         *      2) Collects the relevant addresses and stores them for use in certificates and in CR status
         *      3) Checks it the broker ingresses have been provisioned (have an address)
         *      4) Collects the route addresses for certificates and advertised hostnames
         *
         * @return
         */
        Future<ReconciliationState> kafkaIngressesReady() {
            if (!pfa.hasIngressV1()) {
                return Future.succeededFuture(this);
            }

            List<GenericKafkaListener> ingressListeners = ListenersUtils.ingressListeners(kafkaCluster.getListeners());
            List<Future> listenerFutures = new ArrayList<>(ingressListeners.size());

            for (GenericKafkaListener listener : ingressListeners) {
                String bootstrapIngressName = ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(name, listener);

                Future perListenerFut = ingressOperations.hasIngressAddress(reconciliation, namespace, bootstrapIngressName, 1_000, operationTimeoutMs)
                        .compose(res -> {
                            String bootstrapAddress = listener.getConfiguration().getBootstrap().getHost();
                            LOGGER.debugCr(reconciliation, "Using address {} for Ingress {}", bootstrapAddress, bootstrapIngressName);

                            kafkaBootstrapDnsName.add(bootstrapAddress);

                            ListenerStatus ls = new ListenerStatusBuilder()
                                    .withType(listener.getName())
                                    .withAddresses(new ListenerAddressBuilder()
                                            .withHost(bootstrapAddress)
                                            .withPort(kafkaCluster.getRoutePort())
                                            .build())
                                    .build();
                            addListenerStatus(ls);

                            // Check if broker ingresses are ready
                            List<Future> perPodFutures = new ArrayList<>(kafkaCluster.getReplicas());

                            for (int pod = 0; pod < kafkaCluster.getReplicas(); pod++)  {
                                perPodFutures.add(
                                        ingressOperations.hasIngressAddress(reconciliation, namespace, ListenersUtils.backwardsCompatibleBrokerServiceName(name, pod, listener), 1_000, operationTimeoutMs)
                                );
                            }

                            return CompositeFuture.join(perPodFutures);
                        })
                        .compose(res -> {
                            for (int pod = 0; pod < kafkaCluster.getReplicas(); pod++)  {
                                final int podNumber = pod;
                                String brokerAddress = listener.getConfiguration().getBrokers().stream()
                                        .filter(broker -> broker.getBroker() == podNumber)
                                        .map(GenericKafkaListenerConfigurationBroker::getHost)
                                        .findAny()
                                        .orElse(null);
                                LOGGER.debugCr(reconciliation, "Using address {} for Ingress {}", brokerAddress, ListenersUtils.backwardsCompatibleBrokerServiceName(name, pod, listener));

                                kafkaBrokerDnsNames.computeIfAbsent(pod, k -> new HashSet<>(2)).add(brokerAddress);

                                String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, podNumber);
                                if (advertisedHostname != null) {
                                    kafkaBrokerDnsNames.get(podNumber).add(ListenersUtils.brokerAdvertisedHost(listener, podNumber));
                                }

                                kafkaAdvertisedHostnames.add(kafkaCluster.getAdvertisedHostname(listener, pod, brokerAddress));
                                kafkaAdvertisedPorts.add(kafkaCluster.getAdvertisedPort(listener, pod, kafkaCluster.getIngressPort()));
                            }

                            return Future.succeededFuture();
                        });

                listenerFutures.add(perListenerFut);
            }

            return withVoid(CompositeFuture.join(listenerFutures));
        }

        /**
         * Makes sure all ingresses are ready and collects their addresses for Statuses,
         * certificates and advertised addresses. This method for all ingresses:
         *      1) Checks if the bootstrap ingress has been provisioned (has a loadbalancer address)
         *      2) Collects the relevant addresses and stores them for use in certificates and in CR status
         *      3) Checks it the broker ingresses have been provisioned (have an address)
         *      4) Collects the route addresses for certificates and advertised hostnames
         *
         * @return
         */
        Future<ReconciliationState> kafkaIngressesV1Beta1Ready() {
            if (pfa.hasIngressV1()) {
                return Future.succeededFuture(this);
            }

            List<GenericKafkaListener> ingressListeners = ListenersUtils.ingressListeners(kafkaCluster.getListeners());
            List<Future> listenerFutures = new ArrayList<>(ingressListeners.size());

            for (GenericKafkaListener listener : ingressListeners) {
                String bootstrapIngressName = ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(name, listener);

                Future perListenerFut = ingressV1Beta1Operations.hasIngressAddress(reconciliation, namespace, bootstrapIngressName, 1_000, operationTimeoutMs)
                        .compose(res -> {
                            String bootstrapAddress = listener.getConfiguration().getBootstrap().getHost();
                            LOGGER.debugCr(reconciliation, "Using address {} for v1beta1 Ingress {}", bootstrapAddress, bootstrapIngressName);

                            kafkaBootstrapDnsName.add(bootstrapAddress);

                            ListenerStatus ls = new ListenerStatusBuilder()
                                    .withType(listener.getName())
                                    .withAddresses(new ListenerAddressBuilder()
                                            .withHost(bootstrapAddress)
                                            .withPort(kafkaCluster.getRoutePort())
                                            .build())
                                    .build();
                            addListenerStatus(ls);

                            // Check if broker ingresses are ready
                            List<Future> perPodFutures = new ArrayList<>(kafkaCluster.getReplicas());

                            for (int pod = 0; pod < kafkaCluster.getReplicas(); pod++)  {
                                perPodFutures.add(
                                        ingressV1Beta1Operations.hasIngressAddress(reconciliation, namespace, ListenersUtils.backwardsCompatibleBrokerServiceName(name, pod, listener), 1_000, operationTimeoutMs)
                                );
                            }

                            return CompositeFuture.join(perPodFutures);
                        })
                        .compose(res -> {
                            for (int pod = 0; pod < kafkaCluster.getReplicas(); pod++)  {
                                final int podNumber = pod;
                                String brokerAddress = listener.getConfiguration().getBrokers().stream()
                                        .filter(broker -> broker.getBroker() == podNumber)
                                        .map(GenericKafkaListenerConfigurationBroker::getHost)
                                        .findAny()
                                        .orElse(null);
                                LOGGER.debugCr(reconciliation, "Using address {} for v1beta1 Ingress {}", brokerAddress, ListenersUtils.backwardsCompatibleBrokerServiceName(name, pod, listener));

                                kafkaBrokerDnsNames.computeIfAbsent(pod, k -> new HashSet<>(2)).add(brokerAddress);

                                String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, podNumber);
                                if (advertisedHostname != null) {
                                    kafkaBrokerDnsNames.get(podNumber).add(ListenersUtils.brokerAdvertisedHost(listener, podNumber));
                                }

                                kafkaAdvertisedHostnames.add(kafkaCluster.getAdvertisedHostname(listener, pod, brokerAddress));
                                kafkaAdvertisedPorts.add(kafkaCluster.getAdvertisedPort(listener, pod, kafkaCluster.getIngressPort()));
                            }

                            return Future.succeededFuture();
                        });

                listenerFutures.add(perListenerFut);
            }

            return withVoid(CompositeFuture.join(listenerFutures));
        }

        Future<ReconciliationState> kafkaGenerateCertificates(Supplier<Date> dateSupplier) {
            Promise<ReconciliationState> resultPromise = Promise.promise();
            vertx.createSharedWorkerExecutor("kubernetes-ops-pool").<ReconciliationState>executeBlocking(
                future -> {
                    try {
                        kafkaCluster.generateCertificates(kafkaAssembly,
                                clusterCa, kafkaBootstrapDnsName, kafkaBrokerDnsNames,
                                isMaintenanceTimeWindowsSatisfied(dateSupplier));
                        future.complete(this);
                    } catch (Throwable e) {
                        future.fail(e);
                    }
                },
                true,
                resultPromise);
            return resultPromise.future();
        }

        Future<ReconciliationState> customListenerCertificates() {
            List<String> secretNames = kafkaCluster.getListeners().stream()
                    .filter(listener -> listener.isTls()
                            && listener.getConfiguration() != null
                            && listener.getConfiguration().getBrokerCertChainAndKey() != null)
                    .map(listener -> listener.getConfiguration().getBrokerCertChainAndKey().getSecretName())
                    .distinct()
                    .collect(Collectors.toList());
            LOGGER.debugCr(reconciliation, "Validating secret {} with custom TLS listener certificates", secretNames);

            List<Future> secretFutures = new ArrayList<>(secretNames.size());
            Map<String, Secret> customSecrets = new HashMap<>(secretNames.size());

            for (String secretName : secretNames)   {
                Future fut = secretOperations.getAsync(namespace, secretName)
                        .compose(secret -> {
                            if (secret != null) {
                                customSecrets.put(secretName, secret);
                                LOGGER.debugCr(reconciliation, "Found secrets {} with custom TLS listener certificate", secretName);
                            }

                            return Future.succeededFuture();
                        });

                secretFutures.add(fut);
            }

            Future customCertificatesFuture = CompositeFuture.join(secretFutures)
                    .compose(res -> {
                        List<String> errors = new ArrayList<>();

                        for (GenericKafkaListener listener : kafkaCluster.getListeners())   {
                            if (listener.isTls()
                                    && listener.getConfiguration() != null
                                    && listener.getConfiguration().getBrokerCertChainAndKey() != null)  {
                                CertAndKeySecretSource customCert = listener.getConfiguration().getBrokerCertChainAndKey();
                                Secret secret = customSecrets.get(customCert.getSecretName());

                                if (secret != null) {
                                    if (!secret.getData().containsKey(customCert.getCertificate())) {
                                        errors.add("Secret " + customCert.getSecretName() + " does not contain certificate under the key " + customCert.getCertificate() + ".");
                                    } else if (!secret.getData().containsKey(customCert.getKey())) {
                                        errors.add("Secret " + customCert.getSecretName() + " does not contain custom certificate private key under the key " + customCert.getKey() + ".");
                                    } else  {
                                        byte[] publicKeyBytes = Base64.getDecoder().decode(secret.getData().get(customCert.getCertificate()));
                                        customListenerCertificates.put(listener.getName(), new String(publicKeyBytes, StandardCharsets.US_ASCII));
                                        customListenerCertificateThumbprints.put(listener.getName(), getCertificateThumbprint(secret, customCert));
                                    }
                                } else {
                                    errors.add("Secret " + customCert.getSecretName() + " with custom TLS certificate does not exist.");
                                }

                            }
                        }

                        if (errors.isEmpty())   {
                            return Future.succeededFuture();
                        } else {
                            LOGGER.errorCr(reconciliation, "Failed to process Secrets with custom certificates: {}", errors);
                            return Future.failedFuture(new InvalidResourceException("Failed to process Secrets with custom certificates: " + errors));
                        }
                    });

            return withVoid(customCertificatesFuture);
        }

        String getCertificateThumbprint(Secret certSecret, CertAndKeySecretSource customCertSecret)   {
            try {
                X509Certificate cert = Ca.cert(certSecret, customCertSecret.getCertificate());
                byte[] signature = MessageDigest.getInstance("SHA-256").digest(cert.getEncoded());
                return Base64.getEncoder().encodeToString(signature);
            } catch (NoSuchAlgorithmException | CertificateEncodingException e) {
                throw new RuntimeException("Failed to get certificate signature of " + customCertSecret.getCertificate() + " from Secret " + certSecret.getMetadata().getName(), e);
            }
        }

        Future<ConfigMap> getKafkaAncillaryCm() {
            return Util.metricsAndLogging(reconciliation, configMapOperations, namespace, kafkaCluster.getLogging(), kafkaCluster.getMetricsConfigInCm())
                .compose(metricsAndLoggingCm -> {
                    ConfigMap brokerCm = kafkaCluster.generateAncillaryConfigMap(metricsAndLoggingCm, kafkaAdvertisedHostnames, kafkaAdvertisedPorts, featureGates.controlPlaneListenerEnabled());
                    KafkaConfiguration kc = KafkaConfiguration.unvalidated(reconciliation, kafkaCluster.getBrokersConfiguration()); // has to be after generateAncillaryConfigMap() which generates the configuration

                    // if BROKER_ADVERTISED_HOSTNAMES_FILENAME or BROKER_ADVERTISED_PORTS_FILENAME changes, compute a hash and put it into annotation
                    String brokerConfiguration = brokerCm.getData().getOrDefault(KafkaCluster.BROKER_ADVERTISED_HOSTNAMES_FILENAME, "");
                    brokerConfiguration += brokerCm.getData().getOrDefault(KafkaCluster.BROKER_ADVERTISED_PORTS_FILENAME, "");
                    brokerConfiguration += brokerCm.getData().getOrDefault(KafkaCluster.BROKER_LISTENERS_FILENAME, "");

                    this.kafkaBrokerConfigurationHash = Util.stringHash(brokerConfiguration);
                    this.kafkaBrokerConfigurationHash += Util.stringHash(kc.unknownConfigsWithValues(kafkaCluster.getKafkaVersion()).toString());

                    String loggingConfiguration = brokerCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG);
                    this.kafkaLogging = loggingConfiguration;
                    this.kafkaLoggingAppendersHash = Util.stringHash(Util.getLoggingDynamicallyUnmodifiableEntries(loggingConfiguration));

                    return Future.succeededFuture(brokerCm);
                });
        }

        Future<ReconciliationState> kafkaAncillaryCm() {
            return withVoid(getKafkaAncillaryCm().compose(cm -> configMapOperations.reconcile(reconciliation, namespace, kafkaCluster.getAncillaryConfigMapName(), cm)));
        }

        Future<ReconciliationState> kafkaBrokersSecret() {
            return updateCertificateSecretWithDiff(KafkaCluster.brokersSecretName(name), kafkaCluster.generateBrokersSecret())
                    .map(changed -> {
                        existingKafkaCertsChanged = changed;
                        return this;
                    });
        }

        Future<ReconciliationState> kafkaJmxSecret() {
            if (kafkaCluster.isJmxAuthenticated()) {
                Future<Secret> secretFuture = secretOperations.getAsync(namespace, KafkaCluster.jmxSecretName(name));
                return secretFuture.compose(res -> {
                    if (res == null) {
                        return withVoid(secretOperations.reconcile(reconciliation, namespace, KafkaCluster.jmxSecretName(name),
                                kafkaCluster.generateJmxSecret()));
                    }
                    return withVoid(Future.succeededFuture(this));
                });

            }
            return withVoid(secretOperations.reconcile(reconciliation, namespace, KafkaCluster.jmxSecretName(name), null));
        }

        Future<ReconciliationState> kafkaNetPolicy() {
            return withVoid(networkPolicyOperator.reconcile(reconciliation, namespace, KafkaCluster.networkPolicyName(name), kafkaCluster.generateNetworkPolicy(operatorNamespace, operatorNamespaceLabels)));
        }

        Future<ReconciliationState> kafkaPodDisruptionBudget() {
            return withVoid(podDisruptionBudgetOperator.reconcile(reconciliation, namespace, kafkaCluster.getName(), kafkaCluster.generatePodDisruptionBudget()));
        }

        int getPodIndexFromPvcName(String pvcName)  {
            return Integer.parseInt(pvcName.substring(pvcName.lastIndexOf("-") + 1));
        }

        int getPodIndexFromPodName(String podName)  {
            return Integer.parseInt(podName.substring(podName.lastIndexOf("-") + 1));
        }

        Future<ReconciliationState> maybeResizeReconcilePvcs(List<PersistentVolumeClaim> pvcs, AbstractModel cluster) {
            List<Future> futures = new ArrayList<>(pvcs.size());

            for (PersistentVolumeClaim desiredPvc : pvcs)  {
                Promise<Void> resultPromise = Promise.promise();

                pvcOperations.getAsync(namespace, desiredPvc.getMetadata().getName()).onComplete(res -> {
                    if (res.succeeded())    {
                        PersistentVolumeClaim currentPvc = res.result();

                        if (currentPvc == null || currentPvc.getStatus() == null || !"Bound".equals(currentPvc.getStatus().getPhase())) {
                            // This branch handles the following conditions:
                            // * The PVC doesn't exist yet, we should create it
                            // * The PVC is not Bound and we should reconcile it
                            reconcilePvc(desiredPvc).onComplete(resultPromise);
                        } else if (currentPvc.getStatus().getConditions().stream().filter(cond -> "Resizing".equals(cond.getType()) && "true".equals(cond.getStatus().toLowerCase(Locale.ENGLISH))).findFirst().orElse(null) != null)  {
                            // The PVC is Bound but it is already resizing => Nothing to do, we should let it resize
                            LOGGER.debugCr(reconciliation, "The PVC {} is resizing, nothing to do", desiredPvc.getMetadata().getName());
                            resultPromise.complete();
                        } else if (currentPvc.getStatus().getConditions().stream().filter(cond -> "FileSystemResizePending".equals(cond.getType()) && "true".equals(cond.getStatus().toLowerCase(Locale.ENGLISH))).findFirst().orElse(null) != null)  {
                            // The PVC is Bound and resized but waiting for FS resizing => We need to restart the pod which is using it
                            String podName = cluster.getPodName(getPodIndexFromPvcName(desiredPvc.getMetadata().getName()));
                            fsResizingRestartRequest.add(podName);
                            LOGGER.infoCr(reconciliation, "The PVC {} is waiting for file system resizing and the pod {} needs to be restarted.", desiredPvc.getMetadata().getName(), podName);
                            resultPromise.complete();
                        } else {
                            // The PVC is Bound and resizing is not in progress => We should check if the SC supports resizing and check if size changed
                            Long currentSize = StorageUtils.parseMemory(currentPvc.getSpec().getResources().getRequests().get("storage"));
                            Long desiredSize = StorageUtils.parseMemory(desiredPvc.getSpec().getResources().getRequests().get("storage"));

                            if (!currentSize.equals(desiredSize))   {
                                // The sizes are different => we should resize (shrinking will be handled in StorageDiff, so we do not need to check that)
                                resizePvc(currentPvc, desiredPvc).onComplete(resultPromise);
                            } else  {
                                // size didn't changed, just reconcile
                                reconcilePvc(desiredPvc).onComplete(resultPromise);
                            }
                        }
                    } else {
                        resultPromise.fail(res.cause());
                    }
                });

                futures.add(resultPromise.future());
            }

            return withVoid(CompositeFuture.all(futures));
        }

        Future<Void> reconcilePvc(PersistentVolumeClaim desired)  {
            Promise<Void> resultPromise = Promise.promise();

            pvcOperations.reconcile(reconciliation, namespace, desired.getMetadata().getName(), desired).onComplete(pvcRes -> {
                if (pvcRes.succeeded()) {
                    resultPromise.complete();
                } else {
                    resultPromise.fail(pvcRes.cause());
                }
            });

            return resultPromise.future();
        }

        Future<Void> resizePvc(PersistentVolumeClaim current, PersistentVolumeClaim desired)  {
            Promise<Void> resultPromise = Promise.promise();

            String storageClassName = current.getSpec().getStorageClassName();

            if (storageClassName != null && !storageClassName.isEmpty()) {
                storageClassOperator.getAsync(storageClassName).onComplete(scRes -> {
                    if (scRes.succeeded()) {
                        StorageClass sc = scRes.result();

                        if (sc == null) {
                            LOGGER.warnCr(reconciliation, "Storage Class {} not found. PVC {} cannot be resized. Reconciliation will proceed without reconciling this PVC.", storageClassName, desired.getMetadata().getName());
                            resultPromise.complete();
                        } else if (sc.getAllowVolumeExpansion() == null || !sc.getAllowVolumeExpansion())    {
                            // Resizing not suported in SC => do nothing
                            LOGGER.warnCr(reconciliation, "Storage Class {} does not support resizing of volumes. PVC {} cannot be resized. Reconciliation will proceed without reconciling this PVC.", storageClassName, desired.getMetadata().getName());
                            resultPromise.complete();
                        } else  {
                            // Resizing supported by SC => We can reconcile the PVC to have it resized
                            LOGGER.infoCr(reconciliation, "Resizing PVC {} from {} to {}.", desired.getMetadata().getName(), current.getStatus().getCapacity().get("storage").getAmount(), desired.getSpec().getResources().getRequests().get("storage").getAmount());
                            pvcOperations.reconcile(reconciliation, namespace, desired.getMetadata().getName(), desired).onComplete(pvcRes -> {
                                if (pvcRes.succeeded()) {
                                    resultPromise.complete();
                                } else {
                                    resultPromise.fail(pvcRes.cause());
                                }
                            });
                        }
                    } else {
                        LOGGER.errorCr(reconciliation, "Storage Class {} not found. PVC {} cannot be resized.", storageClassName, desired.getMetadata().getName(), scRes.cause());
                        resultPromise.fail(scRes.cause());
                    }
                });
            } else {
                LOGGER.warnCr(reconciliation, "PVC {} does not use any Storage Class and cannot be resized. Reconciliation will proceed without reconciling this PVC.", desired.getMetadata().getName());
                resultPromise.complete();
            }

            return resultPromise.future();
        }

        Future<ReconciliationState> zkPvcs() {
            List<PersistentVolumeClaim> pvcs = zkCluster.generatePersistentVolumeClaims();

            return maybeResizeReconcilePvcs(pvcs, zkCluster);
        }

        Future<ReconciliationState> kafkaPvcs() {
            List<PersistentVolumeClaim> pvcs = kafkaCluster.generatePersistentVolumeClaims(kafkaCluster.getStorage());

            return maybeResizeReconcilePvcs(pvcs, kafkaCluster);
        }

        /**
         * Normally, the rolling update of Kafka brokers is done in the sequence best for Kafka (controller is rolled
         * last etc.). This method does a rolling update of Kafka brokers in sequence based on the pod index number.
         * This is needed in some special situation such as storage configuration changes.
         *
         * This method is using the regular rolling update mechanism. But in order to achieve the sequential rolling
         * update it calls it recursively with only subset of the pods being considered for rolling update. Thanks to
         * this, it achieves the right order regardless who is controller but still makes sure that the replicas are
         * in-sync.
         *
         * @param sts               StatefulSet which should be rolled
         * @param podNeedsRestart   Function to tell the rolling restart mechanism if given broker pod needs restart or not
         * @param nextPod           The sequence number of the next pod which should be considered for rolling
         * @param lastPod           Index of the last pod which should be considered for restart
         *
         * @return
         */
        Future<ReconciliationState> maybeRollKafkaInSequence(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart, int nextPod, int lastPod) {
            if (nextPod <= lastPod)  {
                final int podToRoll = nextPod;

                return maybeRollKafka(sts, pod -> {
                    if (pod != null && pod.getMetadata().getName().endsWith("-" + podToRoll))    {
                        return podNeedsRestart.apply(pod);
                    } else {
                        return new ArrayList<>();
                    }
                }, false)
                        .compose(ignore -> maybeRollKafkaInSequence(sts, podNeedsRestart, nextPod + 1, lastPod));
            } else {
                // All pods checked for sequential RU => nothing more to do
                return withVoid(Future.succeededFuture());
            }
        }

        /**
         * Checks if any Kafka broker needs rolling update to add or remove JBOD volumes. If it does, we trigger a
         * sequential rolling update because the pods need to be rolled in sequence to add or remove volumes.
         *
         * @return
         */
        Future<ReconciliationState> kafkaRollToAddOrRemoveVolumes() {
            Storage storage = kafkaCluster.getStorage();

            // If storage is not Jbod storage, we never add or remove volumes
            if (storage instanceof JbodStorage) {
                JbodStorage jbodStorage = (JbodStorage) storage;

                // We first check if any broker actually needs the rolling update. Only if at least one of them needs it,
                // we trigger it. This check helps to not go through the rolling update if not needed.
                return podOperations.listAsync(namespace, kafkaCluster.getSelectorLabels())
                        .compose(pods -> {
                            for (Pod pod : pods) {
                                if (!needsRestartBecauseAddedOrRemovedJbodVolumes(pod, jbodStorage, kafkaCurrentReplicas, kafkaCluster.getReplicas()).isEmpty())   {
                                    // At least one broker needs rolling update => we can trigger it without checking the other brokers
                                    LOGGER.debugCr(reconciliation, "Kafka brokers needs rolling update to add or remove JBOD volumes");

                                    return kafkaSetOperations.getAsync(namespace, KafkaCluster.kafkaClusterName(name))
                                            .compose(sts -> {
                                                if (sts != null) {
                                                    int lastPodIndex = Math.min(kafkaCurrentReplicas, kafkaCluster.getReplicas()) - 1;
                                                    return maybeRollKafkaInSequence(sts, podToCheck -> needsRestartBecauseAddedOrRemovedJbodVolumes(podToCheck, jbodStorage, kafkaCurrentReplicas, kafkaCluster.getReplicas()), 0, lastPodIndex);
                                                } else {
                                                    // STS does not exist => nothing to roll
                                                    return withVoid(Future.succeededFuture());
                                                }
                                            });
                                }
                            }

                            LOGGER.debugCr(reconciliation, "No rolling update of Kafka brokers due to added or removed JBOD volumes is needed");
                            return withVoid(Future.succeededFuture());
                        });
            } else {
                return withVoid(Future.succeededFuture());
            }
        }

        /**
         * Checks whether the particular pod needs restart because of added or removed JBOD volumes. This method returns
         * a list instead of boolean so that it can be used directly in the rolling update procedure and log proper
         * reasons for the restart.
         *
         * @param pod               Broker pod which should be considered for restart
         * @param desiredStorage    Desired storage configuration
         * @param currentReplicas   Number of Kafka replicas before this reconciliation
         * @param desiredReplicas   Number of desired Kafka replicas
         *
         * @return  Empty list when no restart is needed. List containing the restart reason if the restart is needed.
         */
        private List<String> needsRestartBecauseAddedOrRemovedJbodVolumes(Pod pod, JbodStorage desiredStorage, int currentReplicas, int desiredReplicas)  {
            if (pod != null
                    && pod.getMetadata() != null) {
                String jsonStorage = Annotations.stringAnnotation(pod, ANNO_STRIMZI_IO_STORAGE, null);

                if (jsonStorage != null) {
                    Storage currentStorage = ModelUtils.decodeStorageFromJson(jsonStorage);

                    if (new StorageDiff(reconciliation, currentStorage, desiredStorage, currentReplicas, desiredReplicas).isVolumesAddedOrRemoved())    {
                        return singletonList("JBOD volumes were added or removed");
                    }
                }
            }

            return new ArrayList<>();
        }

        StatefulSet getKafkaStatefulSet()   {
            StatefulSet kafkaSts = kafkaCluster.generateStatefulSet(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets);
            PodTemplateSpec template = kafkaSts.getSpec().getTemplate();

            // Annotations with CA generations to help with rolling updates when CA changes
            Annotations.annotations(template).put(
                    Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION,
                    String.valueOf(getCaCertGeneration(this.clusterCa)));
            Annotations.annotations(template).put(
                    Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION,
                    String.valueOf(getCaCertGeneration(this.clientsCa)));

            Annotations.annotations(template).put(Annotations.ANNO_STRIMZI_LOGGING_APPENDERS_HASH, kafkaLoggingAppendersHash);
            Annotations.annotations(template).put(KafkaCluster.ANNO_STRIMZI_BROKER_CONFIGURATION_HASH, kafkaBrokerConfigurationHash);

            // Annotations with custom cert thumbprints to help with rolling updates when they change
            if (!customListenerCertificateThumbprints.isEmpty()) {
                Annotations.annotations(template).put(
                        KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS,
                        customListenerCertificateThumbprints.toString());
            }

            return kafkaSts;
        }

        Future<ReconciliationState> kafkaStatefulSet() {
            return withKafkaDiff(kafkaSetOperations.reconcile(reconciliation, namespace, kafkaCluster.getName(), getKafkaStatefulSet()));
        }

        Future<ReconciliationState> kafkaRollingUpdate() {
            return withVoid(maybeRollKafka(kafkaDiffs.resource(), pod ->
                    getReasonsToRestartPod(kafkaDiffs.resource(), pod, existingKafkaCertsChanged, this.clusterCa, this.clientsCa)));
        }

        Future<ReconciliationState> kafkaScaleUp() {
            return withVoid(kafkaSetOperations.scaleUp(reconciliation, namespace, kafkaCluster.getName(), kafkaCluster.getReplicas()));
        }

        Future<ReconciliationState> zkPodsReady() {
            if (zkCurrentReplicas != null && zkCurrentReplicas < zkCluster.getReplicas())  {
                // When scaling up we wait only for old pods to be ready, the new ones were not created yet
                return podsReady(zkCluster, zkCurrentReplicas);
            } else {
                return podsReady(zkCluster);
            }
        }

        Future<ReconciliationState> kafkaPodsReady() {
            return podsReady(kafkaCluster);
        }

        Future<ReconciliationState> podsReady(AbstractModel model) {
            int replicas = model.getReplicas();
            return podsReady(model, replicas);
        }

        Future<ReconciliationState> podsReady(AbstractModel model, int replicas) {
            List<Future> podFutures = new ArrayList<>(replicas);

            for (int i = 0; i < replicas; i++) {
                LOGGER.debugCr(reconciliation, "Checking readiness of pod {}.", model.getPodName(i));
                podFutures.add(podOperations.readiness(reconciliation, namespace, model.getPodName(i), 1_000, operationTimeoutMs));
            }

            return withVoid(CompositeFuture.join(podFutures));
        }

        Future<ReconciliationState> kafkaServiceEndpointReady() {
            return withVoid(serviceOperations.endpointReadiness(reconciliation, namespace, kafkaCluster.getServiceName(), 1_000, operationTimeoutMs));
        }

        Future<ReconciliationState> kafkaHeadlessServiceEndpointReady() {
            return withVoid(serviceOperations.endpointReadiness(reconciliation, namespace, kafkaCluster.getHeadlessServiceName(), 1_000, operationTimeoutMs));
        }

        /**
         * Will check all Zookeeper pods whether the user requested the pod and PVC deletion through an annotation
         *
         * @return
         */
        Future<ReconciliationState> zkManualPodCleaning() {
            String stsName = ZookeeperCluster.zookeeperClusterName(name);
            Future<StatefulSet> futureSts = zkSetOperations.getAsync(namespace, stsName);
            Promise<Void> resultPromise = Promise.promise();

            futureSts.onComplete(res -> {
                if (res.succeeded())    {
                    List<PersistentVolumeClaim> desiredPvcs = zkCluster.generatePersistentVolumeClaims();
                    Future<List<PersistentVolumeClaim>> existingPvcsFuture = pvcOperations.listAsync(namespace, zkCluster.getSelectorLabels());

                    maybeCleanPodAndPvc(zkSetOperations, res.result(), desiredPvcs, existingPvcsFuture).onComplete(resultPromise);
                } else {
                    resultPromise.fail(res.cause());
                }
            });

            return withVoid(resultPromise.future());
        }

        /**
         * Will check all Kafka pods whether the user requested the pod and PVC deletion through an annotation
         *
         * @return
         */
        Future<ReconciliationState> kafkaManualPodCleaning() {
            String stsName = KafkaCluster.kafkaClusterName(name);
            Future<StatefulSet> futureSts = kafkaSetOperations.getAsync(namespace, stsName);
            Promise<Void> resultPromise = Promise.promise();

            futureSts.onComplete(res -> {
                if (res.succeeded())    {
                    StatefulSet sts = res.result();

                    // The storage can change when the JBOD volumes are added / removed etc.
                    // At this point, the STS has not been updated yet. So we use the old storage configuration to get the old PVCs.
                    // This is needed because the restarted pod will be created from old statefulset with old storage configuration.
                    List<PersistentVolumeClaim> desiredPvcs = kafkaCluster.generatePersistentVolumeClaims(getOldStorage(sts));

                    Future<List<PersistentVolumeClaim>> existingPvcsFuture = pvcOperations.listAsync(namespace, kafkaCluster.getSelectorLabels());

                    maybeCleanPodAndPvc(kafkaSetOperations, sts, desiredPvcs, existingPvcsFuture).onComplete(resultPromise);
                } else {
                    resultPromise.fail(res.cause());
                }
            });

            return withVoid(resultPromise.future());
        }

        /**
         * Internal method for checking Pods and PVCs for deletion. It goes through the pods and checks the annotations
         * to decide whether they should be deleted. For the first pod marked to be deleted, it will gather the existing
         * and desired PVCs to be able to delete and recreate them.
         *
         * This method exits with the first Pod it finds where deletion is required. The deletion is done asynchronously
         * so if we deleted multiple pods at the same time we would either need to sync the deletions of different pods
         * to not happen in parallel or have a risk that there will be several pods deleted at the same time (which can
         * affect availability). If multiple pods are marked for deletion, the next one will be deleted in the next loop.
         *
         * @param stsOperator           StatefulSet Operator for managing stateful sets
         * @param sts                   StatefulSet which owns the pods which should be checked for deletion
         * @param desiredPvcs           The list of PVCs which should exist
         * @param existingPvcsFuture    Future which will return a list of PVCs which actually exist
         * @return
         */
        Future<Void> maybeCleanPodAndPvc(StatefulSetOperator stsOperator, StatefulSet sts, List<PersistentVolumeClaim> desiredPvcs, Future<List<PersistentVolumeClaim>> existingPvcsFuture)  {
            if (sts != null) {
                LOGGER.debugCr(reconciliation, "Considering manual cleaning of Pods for StatefulSet {}", sts.getMetadata().getName());

                String stsName = sts.getMetadata().getName();

                for (int i = 0; i < sts.getSpec().getReplicas(); i++) {
                    String podName = stsName + "-" + i;
                    Pod pod = podOperations.get(namespace, podName);

                    if (pod != null) {
                        if (Annotations.booleanAnnotation(pod, AbstractScalableResourceOperator.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC, false)) {
                            LOGGER.debugCr(reconciliation, "Pod and PVCs for {} should be deleted based on annotation", podName);

                            return existingPvcsFuture
                                    .compose(existingPvcs -> {
                                        List<PersistentVolumeClaim> deletePvcs;

                                        if (existingPvcs != null) {
                                            deletePvcs = existingPvcs
                                                    .stream()
                                                    .filter(pvc -> pvc.getMetadata().getName().endsWith(podName))
                                                    .collect(Collectors.toList());
                                        } else {
                                            deletePvcs = new ArrayList<>(0);
                                        }

                                        List<PersistentVolumeClaim> createPvcs = desiredPvcs
                                                .stream()
                                                .filter(pvc -> pvc.getMetadata().getName().endsWith(podName))
                                                .collect(Collectors.toList());

                                        return cleanPodAndPvc(stsOperator, sts, podName, deletePvcs, createPvcs);
                                    });
                        }
                    }
                }
            }

            return Future.succeededFuture();
        }

        /**
         * This is an internal method which actually executes the deletion of the Pod and PVC. This is a non-trivial
         * since the PVC and the Pod are tightly coupled and one cannot be deleted without the other. Also, the
         * StatefulSet controller will recreate the deleted Pod and make it hard to recreate the PVCs manually.
         *
         * To address these, we:
         *     1. Delete the STS in non-cascading delete
         *     2. Delete the Pod
         *     3. Delete the PVC
         *     4. Wait for the Pod to be actually deleted
         *     5. Wait for the PVCs to be actually deleted
         *     6. Recreate the PVCs
         *     7. Recreate the STS (which will in turn recreate the Pod)
         *     5. Wait for Pod readiness
         *
         * @param stsOperator       StatefulSet Operator for managing stateful sets
         * @param sts               The current StatefulSet to which the cleaned pod belongs
         * @param podName           Name of the pod which should be deleted
         * @param deletePvcs        The list of PVCs which should be deleted
         * @param createPvcs        The list of PVCs which should be recreated
         * @return
         */
        Future<Void> cleanPodAndPvc(StatefulSetOperator stsOperator, StatefulSet sts, String podName, List<PersistentVolumeClaim> deletePvcs, List<PersistentVolumeClaim> createPvcs) {
            long pollingIntervalMs = 1_000;
            long timeoutMs = operationTimeoutMs;

            // We start by deleting the StatefulSet so that it doesn't interfere with the pod deletion process
            // The deletion has to be non-cascading so that the other pods are not affected
            Future<Void> fut = stsOperator.deleteAsync(reconciliation, namespace, sts.getMetadata().getName(), false)
                    .compose(ignored -> {
                        // After the StatefulSet is deleted, we can delete the pod which was marked for deletion
                        return podOperations.reconcile(reconciliation, namespace, podName, null);
                    })
                    .compose(ignored -> {
                        // With the pod deleting, we can delete all the PVCs belonging to this pod
                        List<Future> deleteResults = new ArrayList<>(deletePvcs.size());

                        for (PersistentVolumeClaim pvc : deletePvcs)    {
                            String pvcName = pvc.getMetadata().getName();
                            LOGGER.debugCr(reconciliation, "Deleting PVC {} for Pod {} based on {} annotation", pvcName, podName, AbstractScalableResourceOperator.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC);
                            deleteResults.add(pvcOperations.reconcile(reconciliation, namespace, pvcName, null));
                        }
                        return CompositeFuture.join(deleteResults);
                    })
                    .compose(ignored -> {
                        // The pod deletion just triggers it asynchronously
                        // We have to wait for the pod to be actually deleted
                        LOGGER.debugCr(reconciliation, "Checking if Pod {} has been deleted", podName);

                        Future<Void> waitForDeletion = podOperations.waitFor(reconciliation, namespace, podName, "deleted", pollingIntervalMs, timeoutMs, (ignore1, ignore2) -> {
                            Pod deletion = podOperations.get(namespace, podName);
                            LOGGER.traceCr(reconciliation, "Checking if Pod {} in namespace {} has been deleted or recreated", podName, namespace);
                            return deletion == null;
                        });

                        return waitForDeletion;
                    })
                    .compose(ignored -> {
                        // Once the pod is deleted, the PVCs should delete as well
                        // Faked PVCs on Minishift etc. might delete while the pod is running, real PVCs will not
                        List<Future> waitForDeletionResults = new ArrayList<>(deletePvcs.size());

                        for (PersistentVolumeClaim pvc : deletePvcs)    {
                            String pvcName = pvc.getMetadata().getName();
                            String uid = pvc.getMetadata().getUid();

                            LOGGER.debugCr(reconciliation, "Checking if PVC {} for Pod {} has been deleted", pvcName, podName);

                            Future<Void> waitForDeletion = pvcOperations.waitFor(reconciliation, namespace, pvcName, "deleted", pollingIntervalMs, timeoutMs, (ignore1, ignore2) -> {
                                PersistentVolumeClaim deletion = pvcOperations.get(namespace, pvcName);
                                LOGGER.traceCr(reconciliation, "Checking if {} {} in namespace {} has been deleted", pvc.getKind(), pvcName, namespace);
                                return deletion == null || (deletion.getMetadata() != null && !uid.equals(deletion.getMetadata().getUid()));
                            });

                            waitForDeletionResults.add(waitForDeletion);
                        }

                        return CompositeFuture.join(waitForDeletionResults);
                    })
                    .compose(ignored -> {
                        // Once everything was deleted, we can start recreating it.
                        // First we recreate the PVCs
                        List<Future> createResults = new ArrayList<>(createPvcs.size());

                        for (PersistentVolumeClaim pvc : createPvcs)    {
                            LOGGER.debugCr(reconciliation, "Reconciling PVC {} for Pod {} after it was deleted and maybe recreated by the pod", pvc.getMetadata().getName(), podName);
                            createResults.add(pvcOperations.reconcile(reconciliation, namespace, pvc.getMetadata().getName(), pvc));
                        }

                        return CompositeFuture.join(createResults);
                    })
                    .compose(ignored -> {
                        // After the PVCs we recreate the StatefulSet which will in turn recreate the missing Pod
                        // We cannot use the new StatefulSet here because there might have been some other changes for
                        // which we are not yet ready - e.g. changes to off-cluster access etc. which might not work
                        // without the CO creating some other infrastructure.
                        // Therefore we use the old STS and just remove some things such as Status, ResourceVersion, UID
                        // or self link. These will be recreated by Kubernetes after it is created.
                        sts.getMetadata().setResourceVersion(null);
                        sts.getMetadata().setSelfLink(null);
                        sts.getMetadata().setUid(null);
                        sts.setStatus(null);

                        return stsOperator.reconcile(reconciliation, namespace, sts.getMetadata().getName(), sts);
                    })
                    .compose(ignored -> podOperations.readiness(reconciliation, namespace, podName, pollingIntervalMs, timeoutMs));

            return fut;
        }

        /**
         * Deletion of PVCs after the cluster is deleted is handled by owner reference and garbage collection. However,
         * this would not help after scale-downs. Therefore we check if there are any PVCs which should not be present
         * and delete them when they are.
         *
         * This should be called only after the Statefulset reconciliation, rolling update and scale-down when the PVCs
         * are not used anymore by the pods.
         *
         * @return
         */
        Future<ReconciliationState> zkPersistentClaimDeletion() {
            Promise<ReconciliationState> resultPromise = Promise.promise();
            Future<List<PersistentVolumeClaim>> futurePvcs = pvcOperations.listAsync(namespace, zkCluster.getSelectorLabels());

            futurePvcs.onComplete(res -> {
                if (res.succeeded() && res.result() != null)    {
                    List<String> maybeDeletePvcs = res.result().stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());
                    List<String> desiredPvcs = zkCluster.generatePersistentVolumeClaims().stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());

                    persistentClaimDeletion(maybeDeletePvcs, desiredPvcs).onComplete(resultPromise);
                } else {
                    resultPromise.fail(res.cause());
                }
            });

            return resultPromise.future();
        }

        /**
         * Deletion of PVCs after the cluster is deleted is handled by owner reference and garbage collection. However,
         * this would not help after scale-downs. Therefore we check if there are any PVCs which should not be present
         * and delete them when they are.
         *
         * This should be called only after the Statefulset reconciliation, rolling update and scale-down when the PVCs
         * are not used anymore by the pods.
         *
         * @return
         */
        Future<ReconciliationState> kafkaPersistentClaimDeletion() {
            Promise<ReconciliationState> resultPromise = Promise.promise();
            Future<List<PersistentVolumeClaim>> futurePvcs = pvcOperations.listAsync(namespace, kafkaCluster.getSelectorLabels());

            futurePvcs.onComplete(res -> {
                if (res.succeeded() && res.result() != null)    {
                    List<String> maybeDeletePvcs = res.result().stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());
                    List<String> desiredPvcs = kafkaCluster.generatePersistentVolumeClaims(kafkaCluster.getStorage()).stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());

                    persistentClaimDeletion(maybeDeletePvcs, desiredPvcs).onComplete(resultPromise);
                } else {
                    resultPromise.fail(res.cause());
                }
            });

            return resultPromise.future();
        }

        /**
         * Internal method for deleting PVCs after scale-downs or disk removal from JBOD storage. It gets list of
         * existing and desired PVCs, diffs them and removes those which should not exist.
         *
         * @param maybeDeletePvcs   List of existing PVCs
         * @param desiredPvcs       List of PVCs which should exist
         * @return
         */
        Future<ReconciliationState> persistentClaimDeletion(List<String> maybeDeletePvcs, List<String> desiredPvcs) {
            List<Future> futures = new ArrayList<>();

            maybeDeletePvcs.removeAll(desiredPvcs);

            for (String pvcName : maybeDeletePvcs)  {
                LOGGER.debugCr(reconciliation, "Considering PVC {} for deletion", pvcName);

                if (Annotations.booleanAnnotation(pvcOperations.get(namespace, pvcName), AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM, false)) {
                    LOGGER.debugCr(reconciliation, "Deleting PVC {}", pvcName);
                    futures.add(pvcOperations.reconcile(reconciliation, namespace, pvcName, null));
                }
            }

            return withVoid(CompositeFuture.all(futures));
        }

        final Future<ReconciliationState> getEntityOperatorDescription() {
            this.entityOperator = EntityOperator.fromCrd(reconciliation, kafkaAssembly, versions);

            if (entityOperator != null) {
                EntityTopicOperator topicOperator = entityOperator.getTopicOperator();
                EntityUserOperator userOperator = entityOperator.getUserOperator();

                return CompositeFuture.join(
                            topicOperator == null ? Future.succeededFuture(null) :
                                Util.metricsAndLogging(reconciliation, configMapOperations, kafkaAssembly.getMetadata().getNamespace(), topicOperator.getLogging(), null),
                            userOperator == null ? Future.succeededFuture(null) :
                                Util.metricsAndLogging(reconciliation, configMapOperations, kafkaAssembly.getMetadata().getNamespace(), userOperator.getLogging(), null))
                        .compose(res -> {
                            MetricsAndLogging toMetricsAndLogging = res.resultAt(0);
                            MetricsAndLogging uoMetricsAndLogging = res.resultAt(1);

                            if (topicOperator != null)  {
                                this.topicOperatorMetricsAndLogsConfigMap = topicOperator.generateMetricsAndLogConfigMap(toMetricsAndLogging);
                            }

                            if (userOperator != null)   {
                                this.userOperatorMetricsAndLogsConfigMap = userOperator.generateMetricsAndLogConfigMap(uoMetricsAndLogging);
                            }

                            this.eoDeployment = entityOperator.generateDeployment(pfa.isOpenshift(), emptyMap(), imagePullPolicy, imagePullSecrets);
                            return Future.succeededFuture(this);
                        });
            } else {
                return Future.succeededFuture(this);
            }
        }

        // Deploy entity operator Role if entity operator is deployed
        Future<ReconciliationState> entityOperatorRole() {
            final Role role;
            if (isEntityOperatorDeployed()) {
                role = entityOperator.generateRole(namespace, namespace);
            } else {
                role = null;
            }

            return withVoid(roleOperations.reconcile(reconciliation,
                    namespace,
                    EntityOperator.getRoleName(name),
                    role));
        }

        // Deploy entity topic operator Role if entity operator is deployed
        Future<ReconciliationState> entityTopicOperatorRole() {
            final String topicWatchedNamespace;
            if (isEntityOperatorDeployed()
                    && entityOperator.getTopicOperator() != null
                    && entityOperator.getTopicOperator().getWatchedNamespace() != null
                    && !entityOperator.getTopicOperator().getWatchedNamespace().isEmpty()) {
                topicWatchedNamespace = entityOperator.getTopicOperator().getWatchedNamespace();
            } else {
                topicWatchedNamespace = namespace;
            }

            final Future<ReconcileResult<Role>> topicWatchedNamespaceFuture;
            if (!namespace.equals(topicWatchedNamespace)) {
                topicWatchedNamespaceFuture = roleOperations.reconcile(reconciliation,
                        topicWatchedNamespace,
                        EntityOperator.getRoleName(name),
                        entityOperator.generateRole(namespace, topicWatchedNamespace));
            } else {
                topicWatchedNamespaceFuture = Future.succeededFuture();
            }

            return withVoid(topicWatchedNamespaceFuture);
        }

        // Deploy entity user operator Role if entity operator is deployed
        Future<ReconciliationState> entityUserOperatorRole() {
            final String userWatchedNamespace;
            if (isEntityOperatorDeployed()
                    && entityOperator.getUserOperator() != null
                    && entityOperator.getUserOperator().getWatchedNamespace() != null
                    && !entityOperator.getUserOperator().getWatchedNamespace().isEmpty()) {
                userWatchedNamespace = entityOperator.getUserOperator().getWatchedNamespace();
            } else {
                userWatchedNamespace = namespace;
            }

            final Future<ReconcileResult<Role>> userWatchedNamespaceFuture;
            if (!namespace.equals(userWatchedNamespace)) {
                userWatchedNamespaceFuture = roleOperations.reconcile(reconciliation,
                        userWatchedNamespace,
                        EntityOperator.getRoleName(name),
                        entityOperator.generateRole(namespace, userWatchedNamespace));
            } else {
                userWatchedNamespaceFuture = Future.succeededFuture();
            }

            return withVoid(userWatchedNamespaceFuture);
        }

        Future<ReconciliationState> entityOperatorServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(reconciliation, namespace,
                    EntityOperator.entityOperatorServiceAccountName(name),
                    isEntityOperatorDeployed() ? entityOperator.generateServiceAccount() : null));
        }

        // Check for if the entity operator will be deployed as part of the reconciliation
        // Related resources need to know this to know whether to deploy
        private boolean isEntityOperatorDeployed() {
            return eoDeployment != null;
        }

        Future<ReconciliationState> entityOperatorTopicOpRoleBindingForRole() {
            // Don't deploy Role RoleBinding if the topic operator is not deployed,
            // or if the topic operator needs to watch a different namespace
            if (!isEntityOperatorDeployed()
                    || entityOperator.getTopicOperator() == null) {
                LOGGER.debugCr(reconciliation, "entityOperatorTopicOpRoleBindingForRole not required");
                return withVoid(roleBindingOperations.reconcile(reconciliation,
                        namespace,
                        EntityTopicOperator.roleBindingForRoleName(name),
                        null));
            }


            final String watchedNamespace;

            if (entityOperator.getTopicOperator().getWatchedNamespace() != null
                    && !entityOperator.getTopicOperator().getWatchedNamespace().isEmpty()) {
                watchedNamespace = entityOperator.getTopicOperator().getWatchedNamespace();
            } else {
                watchedNamespace = namespace;
            }

            final Future<ReconcileResult<RoleBinding>> watchedNamespaceFuture;

            if (!namespace.equals(watchedNamespace)) {
                watchedNamespaceFuture = roleBindingOperations.reconcile(reconciliation,
                        watchedNamespace,
                        EntityTopicOperator.roleBindingForRoleName(name),
                        entityOperator.getTopicOperator().generateRoleBindingForRole(namespace, watchedNamespace));
            } else {
                watchedNamespaceFuture = Future.succeededFuture();
            }

            // Create role binding for the the UI runs in (it needs to access the CA etc.)
            Future<ReconcileResult<RoleBinding>> ownNamespaceFuture = roleBindingOperations.reconcile(reconciliation,
                    namespace,
                    EntityTopicOperator.roleBindingForRoleName(name),
                    entityOperator.getTopicOperator().generateRoleBindingForRole(namespace, namespace));

            return withVoid(CompositeFuture.join(ownNamespaceFuture, watchedNamespaceFuture));
        }

        Future<ReconciliationState> entityOperatorUserOpRoleBindingForRole() {
            // Don't deploy Role RoleBinding if the user operator is not deployed,
            // or if the user operator needs to watch a different namespace
            if (!isEntityOperatorDeployed()
                    || entityOperator.getUserOperator() == null) {
                LOGGER.debugCr(reconciliation, "entityOperatorUserOpRoleBindingForRole not required");
                return withVoid(roleBindingOperations.reconcile(reconciliation,
                        namespace,
                        EntityUserOperator.roleBindingForRoleName(name),
                        null));
            }


            Future<ReconcileResult<RoleBinding>> ownNamespaceFuture;
            Future<ReconcileResult<RoleBinding>> watchedNamespaceFuture;

            String watchedNamespace = namespace;

            if (entityOperator.getUserOperator().getWatchedNamespace() != null
                    && !entityOperator.getUserOperator().getWatchedNamespace().isEmpty()) {
                watchedNamespace = entityOperator.getUserOperator().getWatchedNamespace();
            }

            if (!namespace.equals(watchedNamespace)) {
                watchedNamespaceFuture = roleBindingOperations.reconcile(reconciliation,
                        watchedNamespace,
                        EntityUserOperator.roleBindingForRoleName(name),
                        entityOperator.getUserOperator().generateRoleBindingForRole(namespace, watchedNamespace));
            } else {
                watchedNamespaceFuture = Future.succeededFuture();
            }

            // Create role binding for the the UI runs in (it needs to access the CA etc.)
            ownNamespaceFuture = roleBindingOperations.reconcile(reconciliation,
                    namespace,
                    EntityUserOperator.roleBindingForRoleName(name),
                    entityOperator.getUserOperator().generateRoleBindingForRole(namespace, namespace));


            return withVoid(CompositeFuture.join(ownNamespaceFuture, watchedNamespaceFuture));
        }

        Future<ReconciliationState> entityOperatorTopicOpAncillaryCm() {
            return withVoid(configMapOperations.reconcile(reconciliation, namespace,
                    isEntityOperatorDeployed() && entityOperator.getTopicOperator() != null ?
                            entityOperator.getTopicOperator().getAncillaryConfigMapName() : EntityTopicOperator.metricAndLogConfigsName(name),
                    topicOperatorMetricsAndLogsConfigMap));
        }

        Future<ReconciliationState> entityOperatorUserOpAncillaryCm() {
            return withVoid(configMapOperations.reconcile(reconciliation, namespace,
                    isEntityOperatorDeployed() && entityOperator.getUserOperator() != null ?
                            entityOperator.getUserOperator().getAncillaryConfigMapName() : EntityUserOperator.metricAndLogConfigsName(name),
                    userOperatorMetricsAndLogsConfigMap));
        }

        Future<ReconciliationState> entityOperatorDeployment() {
            if (this.entityOperator != null && isEntityOperatorDeployed()) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.entityOperator.getName());
                return future.compose(dep -> {
                    // getting the current cluster CA generation from the current deployment, if exists
                    int clusterCaCertGeneration = getCaCertGeneration(this.clusterCa);
                    int clientsCaCertGeneration = getCaCertGeneration(this.clientsCa);

                    Annotations.annotations(eoDeployment.getSpec().getTemplate()).put(
                            Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(clusterCaCertGeneration));
                    Annotations.annotations(eoDeployment.getSpec().getTemplate()).put(
                            Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, String.valueOf(clientsCaCertGeneration));
                    return deploymentOperations.reconcile(reconciliation, namespace, EntityOperator.entityOperatorName(name), eoDeployment);
                }).compose(recon -> {
                    if (recon instanceof ReconcileResult.Noop)   {
                        // Lets check if we need to roll the deployment manually
                        if (existingEntityOperatorCertsChanged) {
                            return entityOperatorRollingUpdate();
                        }
                    }

                    // No need to roll, we patched the deployment (and it will roll it self) or we created a new one
                    return Future.succeededFuture(this);
                });
            } else  {
                return withVoid(deploymentOperations.reconcile(reconciliation, namespace, EntityOperator.entityOperatorName(name), null));
            }
        }

        Future<ReconciliationState> entityOperatorRollingUpdate() {
            return withVoid(deploymentOperations.rollingUpdate(reconciliation, namespace, EntityOperator.entityOperatorName(name), operationTimeoutMs));
        }

        Future<ReconciliationState> entityOperatorReady() {
            if (this.entityOperator != null && isEntityOperatorDeployed()) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.entityOperator.getName());
                return future.compose(dep -> {
                    return withVoid(deploymentOperations.waitForObserved(reconciliation, namespace, this.entityOperator.getName(), 1_000, operationTimeoutMs));
                }).compose(dep -> {
                    return withVoid(deploymentOperations.readiness(reconciliation, namespace, this.entityOperator.getName(), 1_000, operationTimeoutMs));
                }).map(i -> this);
            }
            return withVoid(Future.succeededFuture());
        }

        Future<ReconciliationState> entityOperatorSecret(Supplier<Date> dateSupplier) {
            return updateCertificateSecretWithDiff(EntityOperator.secretName(name), entityOperator == null ? null : entityOperator.generateSecret(clusterCa, isMaintenanceTimeWindowsSatisfied(dateSupplier)))
                    .map(changed -> {
                        existingEntityOperatorCertsChanged = changed;
                        return this;
                    });
        }

        private boolean isPodUpToDate(StatefulSet sts, Pod pod) {
            final int stsGeneration = StatefulSetOperator.getStsGeneration(sts);
            final int podGeneration = StatefulSetOperator.getPodGeneration(pod);
            LOGGER.debugCr(reconciliation, "Rolling update of {}/{}: pod {} has {}={}; sts has {}={}",
                    sts.getMetadata().getNamespace(), sts.getMetadata().getName(), pod.getMetadata().getName(),
                    StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, podGeneration,
                    StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, stsGeneration);
            return stsGeneration == podGeneration;
        }

        /*test*/ final Future<ReconciliationState> getCruiseControlDescription() {
            CruiseControl cruiseControl = CruiseControl.fromCrd(reconciliation, kafkaAssembly, versions);

            if (cruiseControl != null) {
                return Util.metricsAndLogging(reconciliation, configMapOperations, kafkaAssembly.getMetadata().getNamespace(),
                        cruiseControl.getLogging(), cruiseControl.getMetricsConfigInCm())
                        .compose(metricsAndLogging -> {
                            ConfigMap logAndMetricsConfigMap = cruiseControl.generateMetricsAndLogConfigMap(metricsAndLogging);

                            Map<String, String> annotations = singletonMap(CruiseControl.ANNO_STRIMZI_IO_LOGGING, logAndMetricsConfigMap.getData().get(ANCILLARY_CM_KEY_LOG_CONFIG));

                            this.cruiseControlMetricsAndLogsConfigMap = logAndMetricsConfigMap;
                            this.cruiseControl = cruiseControl;
                            this.ccDeployment = cruiseControl.generateDeployment(pfa.isOpenshift(), annotations, imagePullPolicy, imagePullSecrets);

                            return Future.succeededFuture(this);
                        });
            } else {
                return withVoid(Future.succeededFuture());
            }
        }

        Future<ReconciliationState> cruiseControlServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(reconciliation, namespace,
                    CruiseControl.cruiseControlServiceAccountName(name),
                    ccDeployment != null ? cruiseControl.generateServiceAccount() : null));
        }

        Future<ReconciliationState> cruiseControlAncillaryCm() {
            return withVoid(configMapOperations.reconcile(reconciliation, namespace,
                    ccDeployment != null && cruiseControl != null ?
                            cruiseControl.getAncillaryConfigMapName() : CruiseControl.metricAndLogConfigsName(name),
                    cruiseControlMetricsAndLogsConfigMap));
        }

        Future<ReconciliationState> cruiseControlSecret(Supplier<Date> dateSupplier) {
            return updateCertificateSecretWithDiff(CruiseControl.secretName(name), cruiseControl == null ? null : cruiseControl.generateSecret(clusterCa, isMaintenanceTimeWindowsSatisfied(dateSupplier)))
                    .map(changed -> {
                        existingCruiseControlCertsChanged = changed;
                        return this;
                    });
        }

        Future<ReconciliationState> cruiseControlDeployment() {
            if (this.cruiseControl != null && ccDeployment != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.cruiseControl.getName());
                return future.compose(dep -> {
                    return deploymentOperations.reconcile(reconciliation, namespace, this.cruiseControl.getName(), ccDeployment);
                }).compose(recon -> {
                    if (recon instanceof ReconcileResult.Noop)   {
                        // Lets check if we need to roll the deployment manually
                        if (existingCruiseControlCertsChanged) {
                            return cruiseControlRollingUpdate();
                        }
                    }

                    // No need to roll, we patched the deployment (and it will roll it self) or we created a new one
                    return Future.succeededFuture(this);
                });
            } else {
                return withVoid(deploymentOperations.reconcile(reconciliation, namespace, CruiseControl.cruiseControlName(name), null));
            }
        }

        Future<ReconciliationState> cruiseControlRollingUpdate() {
            return withVoid(deploymentOperations.rollingUpdate(reconciliation, namespace, CruiseControl.cruiseControlName(name), operationTimeoutMs));
        }

        Future<ReconciliationState> cruiseControlService() {
            return withVoid(serviceOperations.reconcile(reconciliation, namespace, CruiseControl.cruiseControlServiceName(name), cruiseControl != null ? cruiseControl.generateService() : null));
        }

        Future<ReconciliationState> cruiseControlReady() {
            if (this.cruiseControl != null && ccDeployment != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.cruiseControl.getName());
                return future.compose(dep -> {
                    return withVoid(deploymentOperations.waitForObserved(reconciliation, namespace, this.cruiseControl.getName(), 1_000, operationTimeoutMs));
                }).compose(dep -> {
                    return withVoid(deploymentOperations.readiness(reconciliation, namespace, this.cruiseControl.getName(), 1_000, operationTimeoutMs));
                }).map(i -> this);
            }
            return withVoid(Future.succeededFuture());
        }

        Future<ReconciliationState> cruiseControlNetPolicy() {
            return withVoid(networkPolicyOperator.reconcile(reconciliation, namespace, CruiseControl.policyName(name),
                    cruiseControl != null ? cruiseControl.generateNetworkPolicy(operatorNamespace, operatorNamespaceLabels) : null));
        }

        private boolean isPodCaCertUpToDate(Pod pod, Ca ca) {
            final int caCertGeneration = getCaCertGeneration(ca);
            String podAnnotation = getCaCertAnnotation(ca);
            final int podCaCertGeneration =
                    Annotations.intAnnotation(pod, podAnnotation, Ca.INIT_GENERATION);
            return caCertGeneration == podCaCertGeneration;
        }

        private boolean isCustomCertUpToDate(StatefulSet sts, Pod pod, String annotation) {
            final String stsThumbprint = Annotations.stringAnnotation(sts.getSpec().getTemplate(), annotation, "");
            final String podThumbprint = Annotations.stringAnnotation(pod, annotation, "");
            LOGGER.debugCr(reconciliation, "Rolling update of {}/{}: pod {} has {}={}; sts has {}={}",
                    sts.getMetadata().getNamespace(), sts.getMetadata().getName(), pod.getMetadata().getName(),
                    annotation, podThumbprint,
                    annotation, stsThumbprint);
            return podThumbprint.equals(stsThumbprint);
        }

        /**
         * @param sts Stateful set to which pod belongs
         * @param pod Pod to restart
         * @param cas Certificate authorities to be checked for changes
         * @return null or empty if the restart is not needed, reason String otherwise
         */
        private List<String> getReasonsToRestartPod(StatefulSet sts, Pod pod,
                                                           boolean nodeCertsChange,
                                                           Ca... cas) {
            if (pod == null)    {
                // When the Pod doesn't exist, it doesn't need to be restarted.
                // It will be created with new configuration.
                return new ArrayList<>();
            }

            boolean isPodUpToDate = isPodUpToDate(sts, pod);
            boolean areCustomListenerCertsUpToDate = isCustomCertUpToDate(sts, pod, KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS);

            List<String> reasons = new ArrayList<>(3);

            for (Ca ca: cas) {
                if (ca.certRenewed()) {
                    reasons.add(ca + " certificate renewal");
                }
                if (ca.certsRemoved()) {
                    reasons.add(ca + " certificate removal");
                }
                if (!isPodCaCertUpToDate(pod, ca)) {
                    reasons.add("Pod has old " + ca + " certificate generation");
                }
            }
            if (!isPodUpToDate) {
                reasons.add("Pod has old generation");
            }
            if (fsResizingRestartRequest.contains(pod.getMetadata().getName()))   {
                reasons.add("file system needs to be resized");
            }
            if (!areCustomListenerCertsUpToDate) {
                reasons.add("custom certificate one or more listeners changed");
            }
            if (nodeCertsChange) {
                reasons.add("server certificates changed");
            }
            if (!reasons.isEmpty()) {
                LOGGER.debugCr(reconciliation, "Rolling pod {} due to {}",
                        pod.getMetadata().getName(), reasons);
            }
            return reasons;
        }

        private boolean isMaintenanceTimeWindowsSatisfied(Supplier<Date> dateSupplier) {
            String currentCron = null;
            try {
                boolean isSatisfiedBy = getMaintenanceTimeWindows() == null || getMaintenanceTimeWindows().isEmpty();
                if (!isSatisfiedBy) {
                    Date date = dateSupplier.get();
                    for (String cron : getMaintenanceTimeWindows()) {
                        currentCron = cron;
                        CronExpression cronExpression = new CronExpression(cron);
                        // the user defines the cron expression in "UTC/GMT" timezone but CO pod
                        // can be running on a different one, so setting it on the cron expression
                        cronExpression.setTimeZone(TimeZone.getTimeZone("GMT"));
                        if (cronExpression.isSatisfiedBy(date)) {
                            isSatisfiedBy = true;
                            break;
                        }
                    }
                }
                return isSatisfiedBy;
            } catch (ParseException e) {
                LOGGER.warnCr(reconciliation, "The provided maintenance time windows list contains {} which is not a valid cron expression", currentCron);
                return false;
            }
        }

        private List<String> getMaintenanceTimeWindows() {
            return kafkaAssembly.getSpec().getMaintenanceTimeWindows();
        }

        private int getCaCertGeneration(Ca ca) {
            return Annotations.intAnnotation(ca.caCertSecret(), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION,
                    Ca.INIT_GENERATION);
        }

        private String getCaCertAnnotation(Ca ca) {
            return ca instanceof ClientsCa ?
                    Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION :
                    Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION;
        }

        Future<ReconciliationState> clusterOperatorSecret(Supplier<Date> dateSupplier) {
            oldCoSecret = clusterCa.clusterOperatorSecret();

            Labels labels = Labels.fromResource(kafkaAssembly)
                    .withStrimziKind(reconciliation.kind())
                    .withStrimziCluster(reconciliation.name())
                    .withKubernetesName(Labels.APPLICATION_NAME)
                    .withKubernetesInstance(reconciliation.name())
                    .withKubernetesPartOf(reconciliation.name())
                    .withKubernetesManagedBy(AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);

            OwnerReference ownerRef = new OwnerReferenceBuilder()
                    .withApiVersion(kafkaAssembly.getApiVersion())
                    .withKind(kafkaAssembly.getKind())
                    .withName(kafkaAssembly.getMetadata().getName())
                    .withUid(kafkaAssembly.getMetadata().getUid())
                    .withBlockOwnerDeletion(false)
                    .withController(false)
                    .build();

            Secret secret = ModelUtils.buildSecret(reconciliation, clusterCa, clusterCa.clusterOperatorSecret(), namespace,
                    ClusterOperator.secretName(name), "cluster-operator", "cluster-operator",
                    labels, null, ownerRef, isMaintenanceTimeWindowsSatisfied(dateSupplier));

            return withVoid(secretOperations.reconcile(reconciliation, namespace, ClusterOperator.secretName(name),
                    secret));
        }

        private Storage getOldStorage(StatefulSet sts)  {
            Storage storage = null;

            if (sts != null)    {
                String jsonStorage = Annotations.stringAnnotation(sts, ANNO_STRIMZI_IO_STORAGE, null);

                if (jsonStorage != null)    {
                    storage = ModelUtils.decodeStorageFromJson(jsonStorage);
                }
            }

            return storage;
        }

        void addListenerStatus(ListenerStatus ls)    {
            List<ListenerStatus> current = kafkaStatus.getListeners();
            ArrayList<ListenerStatus> desired;

            if (current != null) {
                desired = new ArrayList<>(current.size() + 1);
                desired.addAll(current);
            } else {
                desired = new ArrayList<>(1);
            }

            desired.add(ls);

            kafkaStatus.setListeners(desired);
        }

        Future<ReconciliationState> kafkaCustomCertificatesToStatus() {
            for (GenericKafkaListener listener : kafkaCluster.getListeners())   {
                if (listener.isTls())   {
                    LOGGER.debugCr(reconciliation, "Adding certificate to status for listener: {}", listener.getName());
                    addCertificateToListener(listener.getName(), customListenerCertificates.get(listener.getName()));
                }
            }

            return Future.succeededFuture(this);
        }

        void addCertificateToListener(String type, String certificate)    {
            if (certificate == null)    {
                // When custom certificate is not used, use the current CA certificate
                certificate = new String(clusterCa.currentCaCertBytes(), StandardCharsets.US_ASCII);
            }

            List<ListenerStatus> listeners = kafkaStatus.getListeners();

            if (listeners != null) {
                ListenerStatus listener = listeners.stream().filter(listenerType -> type.equals(listenerType.getType())).findFirst().orElse(null);

                if (listener != null) {
                    listener.setCertificates(singletonList(certificate));
                }
            }
        }

        String getInternalServiceHostname(String serviceName, boolean useServiceDnsDomain)    {
            if (useServiceDnsDomain)    {
                return DnsNameGenerator.serviceDnsNameWithClusterDomain(namespace, serviceName);
            } else {
                return DnsNameGenerator.serviceDnsNameWithoutClusterDomain(namespace, serviceName);
            }
        }

        private final Future<ReconciliationState> getKafkaExporterDescription() {
            this.kafkaExporter = KafkaExporter.fromCrd(reconciliation, kafkaAssembly, versions);
            this.exporterDeployment = kafkaExporter.generateDeployment(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets);
            return Future.succeededFuture(this);
        }

        Future<ReconciliationState> kafkaExporterServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(reconciliation, namespace,
                    KafkaExporter.containerServiceAccountName(name),
                    exporterDeployment != null ? kafkaExporter.generateServiceAccount() : null));
        }

        Future<ReconciliationState> kafkaExporterSecret(Supplier<Date> dateSupplier) {
            return updateCertificateSecretWithDiff(KafkaExporter.secretName(name), kafkaExporter == null ? null : kafkaExporter.generateSecret(clusterCa, isMaintenanceTimeWindowsSatisfied(dateSupplier)))
                    .map(changed -> {
                        existingKafkaExporterCertsChanged = changed;
                        return this;
                    });
        }

        Future<ReconciliationState> kafkaExporterDeployment() {
            if (this.kafkaExporter != null && this.exporterDeployment != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.kafkaExporter.getName());
                return future.compose(dep -> {
                    // getting the current cluster CA generation from the current deployment, if exists
                    int caCertGeneration = getCaCertGeneration(this.clusterCa);
                    Annotations.annotations(exporterDeployment.getSpec().getTemplate()).put(
                            Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(caCertGeneration));
                    return deploymentOperations.reconcile(reconciliation, namespace, KafkaExporter.kafkaExporterName(name), exporterDeployment);
                })
                .compose(recon -> {
                    if (recon instanceof ReconcileResult.Noop)   {
                        // Lets check if we need to roll the deployment manually
                        if (existingKafkaExporterCertsChanged) {
                            return kafkaExporterRollingUpdate();
                        }
                    }

                    // No need to roll, we patched the deployment (and it will roll it self) or we created a new one
                    return Future.succeededFuture(this);
                });
            } else  {
                return withVoid(deploymentOperations.reconcile(reconciliation, namespace, KafkaExporter.kafkaExporterName(name), null));
            }
        }

        Future<ReconciliationState> kafkaExporterRollingUpdate() {
            return withVoid(deploymentOperations.rollingUpdate(reconciliation, namespace, KafkaExporter.kafkaExporterName(name), operationTimeoutMs));
        }

        Future<ReconciliationState> kafkaExporterReady() {
            if (this.kafkaExporter != null && exporterDeployment != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace, this.kafkaExporter.getName());
                return future.compose(dep -> {
                    return withVoid(deploymentOperations.waitForObserved(reconciliation, namespace, this.kafkaExporter.getName(), 1_000, operationTimeoutMs));
                }).compose(dep -> {
                    return withVoid(deploymentOperations.readiness(reconciliation, namespace, this.kafkaExporter.getName(), 1_000, operationTimeoutMs));
                }).map(i -> this);
            }
            return withVoid(Future.succeededFuture());
        }

        Future<ReconciliationState> getJmxTransDescription() {
            try {
                int numOfBrokers = kafkaCluster.getReplicas();
                this.jmxTrans = JmxTrans.fromCrd(reconciliation, kafkaAssembly, versions);
                if (this.jmxTrans != null) {
                    this.jmxTransConfigMap = jmxTrans.generateJmxTransConfigMap(kafkaAssembly.getSpec().getJmxTrans(), numOfBrokers);
                    this.jmxTransDeployment = jmxTrans.generateDeployment(imagePullPolicy, imagePullSecrets);
                }

                return Future.succeededFuture(this);
            } catch (Throwable e) {
                return Future.failedFuture(e);
            }
        }

        Future<ReconciliationState> jmxTransConfigMap() {
            return withVoid(configMapOperations.reconcile(reconciliation, namespace,
                    JmxTrans.jmxTransConfigName(name),
                    jmxTransConfigMap));
        }


        Future<ReconciliationState> jmxTransServiceAccount() {
            return withVoid(serviceAccountOperations.reconcile(reconciliation, namespace,
                    JmxTrans.containerServiceAccountName(name),
                    jmxTrans != null ? jmxTrans.generateServiceAccount() : null));
        }

        Future<ReconciliationState> jmxTransDeployment() {
            if (this.jmxTrans != null && this.jmxTransDeployment != null) {
                return deploymentOperations.getAsync(namespace, this.jmxTrans.getName()).compose(dep -> {
                    return configMapOperations.getAsync(namespace, jmxTransConfigMap.getMetadata().getName()).compose(res -> {
                        String resourceVersion = res.getMetadata().getResourceVersion();
                        // getting the current cluster CA generation from the current deployment, if it exists
                        int caCertGeneration = getCaCertGeneration(this.clusterCa);
                        Annotations.annotations(jmxTransDeployment.getSpec().getTemplate()).put(
                                Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(caCertGeneration));
                        Annotations.annotations(jmxTransDeployment.getSpec().getTemplate()).put(
                                JmxTrans.CONFIG_MAP_ANNOTATION_KEY, resourceVersion);
                        return withVoid(deploymentOperations.reconcile(reconciliation, namespace, JmxTrans.jmxTransName(name),
                                jmxTransDeployment));
                    });
                });
            } else {
                return withVoid(deploymentOperations.reconcile(reconciliation, namespace, JmxTrans.jmxTransName(name), null));
            }
        }

        Future<ReconciliationState> jmxTransDeploymentReady() {
            if (this.jmxTrans != null && jmxTransDeployment != null) {
                Future<Deployment> future = deploymentOperations.getAsync(namespace,  this.jmxTrans.getName());
                return future.compose(dep -> {
                    return withVoid(deploymentOperations.waitForObserved(reconciliation, namespace,  this.jmxTrans.getName(), 1_000, operationTimeoutMs));
                }).compose(dep -> {
                    return withVoid(deploymentOperations.readiness(reconciliation, namespace, this.jmxTrans.getName(), 1_000, operationTimeoutMs));
                }).map(i -> this);
            }
            return withVoid(Future.succeededFuture());
        }

    }

    /* test */ Date dateSupplier() {
        return new Date();
    }

    @Override
    protected KafkaStatus createStatus() {
        return new KafkaStatus();
    }

    /**
     * Deletes the ClusterRoleBinding which as a cluster-scoped resource cannot be deleted by the ownerReference
     *
     * @param reconciliation    The Reconciliation identification
     * @return                  Future indicating the result of the deletion
     */
    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        return withIgnoreRbacError(reconciliation, clusterRoleBindingOperations.reconcile(reconciliation, KafkaResources.initContainerClusterRoleBindingName(reconciliation.name(), reconciliation.namespace()), null), null)
                .map(Boolean.FALSE); // Return FALSE since other resources are still deleted by garbage collection
    }
}
