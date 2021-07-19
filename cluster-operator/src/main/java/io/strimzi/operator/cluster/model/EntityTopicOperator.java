/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.OrderedProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Represents the Topic Operator deployment
 */
public class EntityTopicOperator extends AbstractModel {
    protected static final String APPLICATION_NAME = "entity-topic-operator";

    protected static final String TOPIC_OPERATOR_CONTAINER_NAME = "topic-operator";
    private static final String NAME_SUFFIX = "-entity-topic-operator";
    protected static final String METRICS_AND_LOG_CONFIG_SUFFIX = NAME_SUFFIX + "-config";

    // Port configuration
    protected static final int HEALTHCHECK_PORT = 8080;
    protected static final String HEALTHCHECK_PORT_NAME = "healthcheck";

    // Topic Operator configuration keys
    public static final String ENV_VAR_RESOURCE_LABELS = "STRIMZI_RESOURCE_LABELS";
    public static final String ENV_VAR_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    public static final String ENV_VAR_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";
    public static final String ENV_VAR_WATCHED_NAMESPACE = "STRIMZI_NAMESPACE";
    public static final String ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    public static final String ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS = "STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS";
    public static final String ENV_VAR_TOPIC_METADATA_MAX_ATTEMPTS = "STRIMZI_TOPIC_METADATA_MAX_ATTEMPTS";
    public static final String ENV_VAR_SECURITY_PROTOCOL = "STRIMZI_SECURITY_PROTOCOL";

    public static final String ENV_VAR_TLS_ENABLED = "STRIMZI_TLS_ENABLED";

    public static final Probe DEFAULT_HEALTHCHECK_OPTIONS = new ProbeBuilder()
            .withInitialDelaySeconds(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY)
            .withTimeoutSeconds(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT).build();

    // Volume name of the temporary volume used by the TO container
    // Because the container shares the pod with other containers, it needs to have unique name
    /*test*/ static final String TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME = "strimzi-to-tmp";

    // Kafka bootstrap servers and Zookeeper nodes can't be specified in the JSON
    private String kafkaBootstrapServers;
    private String zookeeperConnect;

    private String watchedNamespace;
    private int reconciliationIntervalMs;
    private int zookeeperSessionTimeoutMs;
    private String resourceLabels;
    private int topicMetadataMaxAttempts;
    protected List<ContainerEnvVar> templateContainerEnvVars;
    protected SecurityContext templateContainerSecurityContext;

    /**
     * @param reconciliation   The reconciliation
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     */
    protected EntityTopicOperator(Reconciliation reconciliation, HasMetadata resource) {
        super(reconciliation, resource, APPLICATION_NAME);
        this.name = topicOperatorName(cluster);
        this.readinessPath = "/";
        this.readinessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.livenessPath = "/";
        this.livenessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;

        // new KafkaStreamsTopicStore needs a bit more to start
        this.startupProbeOptions = new ProbeBuilder()
                .withPeriodSeconds(10)
                .withFailureThreshold(12)
                .build();

        // create a default configuration
        this.kafkaBootstrapServers = defaultBootstrapServers(cluster);
        this.zookeeperConnect = defaultZookeeperConnect(cluster);
        this.watchedNamespace = namespace;
        this.reconciliationIntervalMs = EntityTopicOperatorSpec.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1_000;
        this.zookeeperSessionTimeoutMs = EntityTopicOperatorSpec.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS * 1_000;
        this.resourceLabels = ModelUtils.defaultResourceLabels(cluster);
        this.topicMetadataMaxAttempts = EntityTopicOperatorSpec.DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS;

        this.ancillaryConfigMapName = metricAndLogConfigsName(cluster);
        this.logAndMetricsConfigVolumeName = "entity-topic-operator-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/opt/topic-operator/custom-config/";
    }

    public void setWatchedNamespace(String watchedNamespace) {
        this.watchedNamespace = watchedNamespace;
    }

    public String getWatchedNamespace() {
        return watchedNamespace;
    }

    public void setResourceLabels(String resourceLabels) {
        this.resourceLabels = resourceLabels;
    }

    public String getResourceLabels() {
        return resourceLabels;
    }

    public void setReconciliationIntervalMs(int reconciliationIntervalMs) {
        this.reconciliationIntervalMs = reconciliationIntervalMs;
    }

    public int getReconciliationIntervalMs() {
        return reconciliationIntervalMs;
    }

    public void setZookeeperSessionTimeoutMs(int zookeeperSessionTimeoutMs) {
        this.zookeeperSessionTimeoutMs = zookeeperSessionTimeoutMs;
    }

    public int getZookeeperSessionTimeoutMs() {
        return zookeeperSessionTimeoutMs;
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public void setTopicMetadataMaxAttempts(int topicMetadataMaxAttempts) {
        this.topicMetadataMaxAttempts = topicMetadataMaxAttempts;
    }

    public int getTopicMetadataMaxAttempts() {
        return topicMetadataMaxAttempts;
    }

    protected static String defaultZookeeperConnect(String cluster) {
        return String.format("%s:%d", "localhost", EntityTopicOperatorSpec.DEFAULT_ZOOKEEPER_PORT);
    }

    public void setZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
    }

    public String getZookeeperConnect() {
        return zookeeperConnect;
    }

    protected static String defaultBootstrapServers(String cluster) {
        return KafkaCluster.serviceName(cluster) + ":" + EntityTopicOperatorSpec.DEFAULT_BOOTSTRAP_SERVERS_PORT;
    }

    public static String topicOperatorName(String cluster) {
        return cluster + NAME_SUFFIX;
    }

    public static String metricAndLogConfigsName(String cluster) {
        return cluster + METRICS_AND_LOG_CONFIG_SUFFIX;
    }

    /**
     * Get the name of the TO role binding given the name of the {@code cluster}.
     * @param cluster The cluster name.
     * @return The name of the role binding.
     */
    public static String roleBindingForRoleName(String cluster) {
        return cluster + "-entity-topic-operator-role";
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "entityTopicOperatorDefaultLoggingProperties";
    }

    @Override
    public String getAncillaryConfigMapKeyLogConfig() {
        return "log4j2.properties";
    }

    /**
     * Create an Entity Topic Operator from given desired resource
     *
     * @param reconciliation The reconciliation
     * @param kafkaAssembly desired resource with cluster configuration containing the Entity Topic Operator one
     * @return Entity Topic Operator instance, null if not configured in the ConfigMap
     */
    public static EntityTopicOperator fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly) {
        EntityTopicOperator result = null;
        EntityOperatorSpec entityOperatorSpec = kafkaAssembly.getSpec().getEntityOperator();
        if (entityOperatorSpec != null) {

            EntityTopicOperatorSpec topicOperatorSpec = entityOperatorSpec.getTopicOperator();
            if (topicOperatorSpec != null) {

                String namespace = kafkaAssembly.getMetadata().getNamespace();
                result = new EntityTopicOperator(reconciliation, kafkaAssembly);

                result.setOwnerReference(kafkaAssembly);
                String image = topicOperatorSpec.getImage();
                if (image == null) {
                    image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE, "quay.io/strimzi/operator:latest");
                }
                result.setImage(image);
                result.setWatchedNamespace(topicOperatorSpec.getWatchedNamespace() != null ? topicOperatorSpec.getWatchedNamespace() : namespace);
                result.setReconciliationIntervalMs(topicOperatorSpec.getReconciliationIntervalSeconds() * 1_000);
                result.setZookeeperSessionTimeoutMs(topicOperatorSpec.getZookeeperSessionTimeoutSeconds() * 1_000);
                result.setTopicMetadataMaxAttempts(topicOperatorSpec.getTopicMetadataMaxAttempts());
                result.setLogging(topicOperatorSpec.getLogging());
                result.setGcLoggingEnabled(topicOperatorSpec.getJvmOptions() == null ? DEFAULT_JVM_GC_LOGGING_ENABLED : topicOperatorSpec.getJvmOptions().isGcLoggingEnabled());
                if (topicOperatorSpec.getJvmOptions() != null) {
                    result.setJavaSystemProperties(topicOperatorSpec.getJvmOptions().getJavaSystemProperties());
                }
                result.setJvmOptions(topicOperatorSpec.getJvmOptions());
                result.setResources(topicOperatorSpec.getResources());
                if (topicOperatorSpec.getStartupProbe() != null) {
                    result.setStartupProbe(topicOperatorSpec.getStartupProbe());
                }
                if (topicOperatorSpec.getReadinessProbe() != null) {
                    result.setReadinessProbe(topicOperatorSpec.getReadinessProbe());
                }
                if (topicOperatorSpec.getLivenessProbe() != null) {
                    result.setLivenessProbe(topicOperatorSpec.getLivenessProbe());
                }
            }
        }
        return result;
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {

        return singletonList(new ContainerBuilder()
                .withName(TOPIC_OPERATOR_CONTAINER_NAME)
                .withImage(getImage())
                .withArgs("/opt/strimzi/bin/topic_operator_run.sh")
                .withEnv(getEnvVars())
                .withPorts(singletonList(createContainerPort(HEALTHCHECK_PORT_NAME, HEALTHCHECK_PORT, "TCP")))
                .withStartupProbe(ProbeGenerator.httpProbe(startupProbeOptions, livenessPath + "healthy", HEALTHCHECK_PORT_NAME))
                .withLivenessProbe(ProbeGenerator.httpProbe(livenessProbeOptions, livenessPath + "healthy", HEALTHCHECK_PORT_NAME))
                .withReadinessProbe(ProbeGenerator.httpProbe(readinessProbeOptions, readinessPath + "ready", HEALTHCHECK_PORT_NAME))
                .withResources(getResources())
                .withVolumeMounts(getVolumeMounts())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .withSecurityContext(templateContainerSecurityContext)
                .build());
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(ENV_VAR_RESOURCE_LABELS, resourceLabels));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers));
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_CONNECT, zookeeperConnect));
        varList.add(buildEnvVar(ENV_VAR_WATCHED_NAMESPACE, watchedNamespace));
        varList.add(buildEnvVar(ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS, Integer.toString(reconciliationIntervalMs)));
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS, Integer.toString(zookeeperSessionTimeoutMs)));
        varList.add(buildEnvVar(ENV_VAR_TOPIC_METADATA_MAX_ATTEMPTS, String.valueOf(topicMetadataMaxAttempts)));
        varList.add(buildEnvVar(ENV_VAR_SECURITY_PROTOCOL, EntityTopicOperatorSpec.DEFAULT_SECURITY_PROTOCOL));
        varList.add(buildEnvVar(ENV_VAR_TLS_ENABLED, Boolean.toString(true)));
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));
        EntityOperator.javaOptions(varList, getJvmOptions(), javaSystemProperties);

        // Add shared environment variables used for all containers
        varList.addAll(getRequiredEnvVars());

        addContainerEnvsToExistingEnvs(varList, templateContainerEnvVars);

        return varList;
    }

    public List<Volume> getVolumes() {
        return asList(createTempDirVolume(TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME),
                VolumeUtils.createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigMapName));
    }

    private List<VolumeMount> getVolumeMounts() {
        return asList(createTempDirVolumeMount(TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME),
                VolumeUtils.createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath),
                VolumeUtils.createVolumeMount(EntityOperator.TLS_SIDECAR_EO_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT),
                VolumeUtils.createVolumeMount(EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT));
    }

    @Override
    protected String getRoleName() {
        return EntityOperator.getRoleName(cluster);
    }

    public RoleBinding generateRoleBindingForRole(String namespace, String watchedNamespace) {
        Subject ks = new SubjectBuilder()
                .withKind("ServiceAccount")
                .withName(EntityOperator.entityOperatorServiceAccountName(cluster))
                .withNamespace(namespace)
                .build();

        RoleRef roleRef = new RoleRefBuilder()
                .withName(getRoleName())
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("Role")
                .build();

        RoleBinding rb = generateRoleBinding(
                roleBindingForRoleName(cluster),
                watchedNamespace,
                roleRef,
                singletonList(ks)
        );

        // We set OwnerReference only within the same namespace since it does not work cross-namespace
        if (!namespace.equals(watchedNamespace)) {
            rb.getMetadata().setOwnerReferences(Collections.emptyList());
        }

        return rb;
    }

    public void setContainerEnvVars(List<ContainerEnvVar> envVars) {
        templateContainerEnvVars = envVars;
    }

    public void setContainerSecurityContext(SecurityContext securityContext) {
        templateContainerSecurityContext = securityContext;
    }

    /**
     * Transforms properties to log4j2 properties file format and adds property for reloading the config
     * @param properties map with properties
     * @return modified string with monitorInterval
     */
    @Override
    public String createLog4jProperties(OrderedProperties properties) {
        if (!properties.asMap().keySet().contains("monitorInterval")) {
            properties.addPair("monitorInterval", "30");
        }
        return super.createLog4jProperties(properties);
    }

}
