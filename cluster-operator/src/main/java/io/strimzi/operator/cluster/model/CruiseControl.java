/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HTTPHeader;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LifecycleBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.apps.RollingUpdateDeploymentBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRuleBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeerBuilder;
import io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.template.CruiseControlTemplate;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.cruisecontrol.Capacity;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.strimzi.operator.cluster.model.CruiseControlConfiguration.CRUISE_CONTROL_GOALS;
import static io.strimzi.operator.cluster.model.CruiseControlConfiguration.CRUISE_CONTROL_DEFAULT_ANOMALY_DETECTION_GOALS;
import static io.strimzi.operator.cluster.model.VolumeUtils.createConfigMapVolume;
import static io.strimzi.operator.cluster.model.VolumeUtils.createSecretVolume;
import static io.strimzi.operator.cluster.model.VolumeUtils.createVolumeMount;

public class CruiseControl extends AbstractModel {
    protected static final String APPLICATION_NAME = "cruise-control";

    public static final String CRUISE_CONTROL_METRIC_REPORTER = "com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter";

    protected static final String CRUISE_CONTROL_CONTAINER_NAME = "cruise-control";

    public static final String HTTP_SCHEME = "HTTP";
    public static final String HTTPS_SCHEME = "HTTPS";

    // Fields used for Cruise Control API authentication
    public static final String API_ADMIN_NAME = "admin";
    public static final String API_ADMIN_ROLE = "ADMIN";
    public static final String API_USER_NAME = "user";
    public static final String API_USER_PASSWORD = "user";
    public static final String API_USER_ROLE = "USER";
    public static final String API_ADMIN_PASSWORD_KEY = APPLICATION_NAME + ".apiAdminPassword";
    public static final String API_USER_PASSWORD_KEY = APPLICATION_NAME + ".apiUserPassword";
    public static final String API_AUTH_FILE_KEY = APPLICATION_NAME + ".apiAuthFile";

    protected static final String TLS_SIDECAR_NAME = "tls-sidecar";
    protected static final String TLS_SIDECAR_CC_CERTS_VOLUME_NAME = "cc-certs";
    protected static final String TLS_SIDECAR_CC_CERTS_VOLUME_MOUNT = "/etc/tls-sidecar/cc-certs/";
    protected static final String TLS_SIDECAR_CA_CERTS_VOLUME_NAME = "cluster-ca-certs";
    protected static final String TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT = "/etc/tls-sidecar/cluster-ca-certs/";
    protected static final String LOG_AND_METRICS_CONFIG_VOLUME_NAME = "cruise-control-metrics-and-logging";
    protected static final String LOG_AND_METRICS_CONFIG_VOLUME_MOUNT = "/opt/cruise-control/custom-config/";
    private static final String CC_AUTH_CREDENTIALS_FILENAME = API_AUTH_FILE_KEY;

    private static final String NAME_SUFFIX = "-cruise-control";

    // Volume name of the temporary volume used by the TLS sidecar container
    // Because the container shares the pod with other containers, it needs to have unique name
    /*test*/ static final String TLS_SIDECAR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME = "strimzi-tls-sidecar-tmp";

    public static final String ANNO_STRIMZI_IO_LOGGING = Annotations.STRIMZI_DOMAIN + "logging";

    public static final String ENV_VAR_CRUISE_CONTROL_METRICS_ENABLED = "CRUISE_CONTROL_METRICS_ENABLED";

    private String zookeeperConnect;

    // Configuration defaults
    protected static final int DEFAULT_REPLICAS = 1;
    public static final boolean DEFAULT_CRUISE_CONTROL_METRICS_ENABLED = false;
    public static final boolean DEFAULT_WEBSERVER_SECURITY_ENABLED = true;
    public static final boolean DEFAULT_WEBSERVER_SSL_ENABLED = true;

    // Default probe settings (liveness and readiness) for health checks
    protected static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    protected static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;

    public static final Probe DEFAULT_HEALTHCHECK_OPTIONS = new ProbeBuilder()
            .withInitialDelaySeconds(DEFAULT_HEALTHCHECK_DELAY)
            .withTimeoutSeconds(DEFAULT_HEALTHCHECK_TIMEOUT)
            .build();

    private TlsSidecar tlsSidecar;
    private String tlsSidecarImage;
    private String minInsyncReplicas = "1";
    public String uriScheme;
    public boolean authorizationEnabled;
    private Double brokerDiskMiBCapacity;
    private int brokerCpuUtilizationCapacity;
    private Double brokerInboundNetworkKiBPerSecondCapacity;
    private Double brokerOuboundNetworkKiBPerSecondCapacity;

    public static final String REST_API_PORT_NAME = "rest-api";
    public static final int REST_API_PORT = 9090;
    protected static final int DEFAULT_BOOTSTRAP_SERVERS_PORT = 9091;
    public static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";

    // Cruise Control configuration keys (EnvVariables)
    protected static final String ENV_VAR_CRUISE_CONTROL_CONFIGURATION = "CRUISE_CONTROL_CONFIGURATION";
    protected static final String ENV_VAR_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";
    protected static final String ENV_VAR_STRIMZI_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    protected static final String ENV_VAR_MIN_INSYNC_REPLICAS = "MIN_INSYNC_REPLICAS";
    protected static final String ENV_VAR_BROKER_DISK_MIB_CAPACITY = "BROKER_DISK_MIB_CAPACITY";
    protected static final String ENV_VAR_BROKER_CPU_UTILIZATION_CAPACITY = "BROKER_CPU_UTILIZATION_CAPACITY";
    protected static final String ENV_VAR_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY = "BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY";
    protected static final String ENV_VAR_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY = "BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY";

    // Templates
    protected List<ContainerEnvVar> templateCruiseControlContainerEnvVars;
    protected List<ContainerEnvVar> templateTlsSidecarContainerEnvVars;

    protected SecurityContext templateCruiseControlContainerSecurityContext;
    protected SecurityContext templateTlsSidecarContainerSecurityContext;

    private boolean isDeployed;

    /**
     * Constructor
     *
     * @param reconciliation The reconciliation
     * @param resource  Kubernetes resource with metadata containing the namespace and cluster name
     */
    protected CruiseControl(Reconciliation reconciliation, HasMetadata resource) {
        super(reconciliation, resource, APPLICATION_NAME);
        this.name = CruiseControlResources.deploymentName(cluster);
        this.serviceName = CruiseControlResources.serviceName(cluster);
        this.ancillaryConfigMapName = metricAndLogConfigsName(cluster);
        this.replicas = DEFAULT_REPLICAS;
        this.readinessPath = "/kafkacruisecontrol/state";
        this.livenessPath = "/kafkacruisecontrol/state";
        this.livenessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.readinessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.mountPath = "/var/lib/kafka";
        this.logAndMetricsConfigVolumeName = LOG_AND_METRICS_CONFIG_VOLUME_NAME;
        this.logAndMetricsConfigMountPath = LOG_AND_METRICS_CONFIG_VOLUME_MOUNT;
        this.isMetricsEnabled = DEFAULT_CRUISE_CONTROL_METRICS_ENABLED;

        this.zookeeperConnect = defaultZookeeperConnect(cluster);
    }

    public static String metricAndLogConfigsName(String cluster) {
        return CruiseControlResources.logAndMetricsConfigMapName(cluster);
    }

    protected void setTlsSidecar(TlsSidecar tlsSidecar) {
        this.tlsSidecar = tlsSidecar;
    }

    protected static String defaultZookeeperConnect(String cluster) {
        return ZookeeperCluster.serviceName(cluster) + ":" + ZookeeperCluster.CLIENT_TLS_PORT;
    }

    protected static String defaultBootstrapServers(String cluster) {
        return KafkaCluster.serviceName(cluster) + ":" + DEFAULT_BOOTSTRAP_SERVERS_PORT;
    }

    @SuppressWarnings("deprecation")
    public static String encodeToBase64(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.US_ASCII));
    }

    public static String getAuthCredentialsFile() {
        return TLS_SIDECAR_CC_CERTS_VOLUME_MOUNT + CC_AUTH_CREDENTIALS_FILENAME;
    }

    private static boolean isEnabled(CruiseControlConfiguration config, String s1, String s2) {
        String s = config.getConfigOption(s1, s2);
        return Boolean.parseBoolean(s);
    }

    public static boolean isApiAuthorizationEnabled(CruiseControlConfiguration config) {
        return isEnabled(config, CruiseControlConfigurationParameters.CRUISE_CONTROL_WEBSERVER_SECURITY_ENABLE.getValue(), Boolean.toString(DEFAULT_WEBSERVER_SECURITY_ENABLED));
    }

    public static boolean isApiAuthenticationEnabled(CruiseControlConfiguration config) {
        return isEnabled(config, CruiseControlConfigurationParameters.CRUISE_CONTROL_WEBSERVER_SSL_ENABLE.getValue(), Boolean.toString(DEFAULT_WEBSERVER_SSL_ENABLED));
    }

    public static HTTPHeader generateAuthHttpHeader(String user, String password) {
        String headerName = "Authorization";
        String headerValue = "Basic " + encodeToBase64(String.join(":", user, password));

        return new HTTPHeader(headerName, headerValue);
    }

    public static CruiseControl fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly, KafkaVersion.Lookup versions) {
        CruiseControl cruiseControl = null;
        CruiseControlSpec spec = kafkaAssembly.getSpec().getCruiseControl();
        KafkaClusterSpec kafkaClusterSpec = kafkaAssembly.getSpec().getKafka();

        if (spec != null) {
            cruiseControl = new CruiseControl(reconciliation, kafkaAssembly);
            cruiseControl.isDeployed = true;

            cruiseControl.setReplicas(DEFAULT_REPLICAS);
            String image = spec.getImage();
            if (image == null) {
                image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE, versions.kafkaImage(kafkaClusterSpec.getImage(), versions.defaultVersion().version()));
            }
            cruiseControl.setImage(image);

            TlsSidecar tlsSidecar = spec.getTlsSidecar();
            if (tlsSidecar == null) {
                tlsSidecar = new TlsSidecar();
            }

            String tlsSideCarImage = tlsSidecar.getImage();
            if (tlsSideCarImage == null) {
                tlsSideCarImage = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_TLS_SIDECAR_CRUISE_CONTROL_IMAGE, versions.kafkaImage(kafkaClusterSpec.getImage(), versions.defaultVersion().version()));
            }

            tlsSidecar.setImage(tlsSideCarImage);
            cruiseControl.tlsSidecarImage = tlsSideCarImage;
            cruiseControl.setTlsSidecar(tlsSidecar);

            cruiseControl = cruiseControl.updateConfiguration(spec);
            CruiseControlConfiguration c = (CruiseControlConfiguration) cruiseControl.getConfiguration();
            Boolean sslEnabled = isApiAuthenticationEnabled(c);
            if (sslEnabled) {
                cruiseControl.uriScheme = HTTPS_SCHEME;
            } else {
                cruiseControl.uriScheme = HTTP_SCHEME;
            }

            cruiseControl.authorizationEnabled = isApiAuthorizationEnabled(c);

            KafkaConfiguration configuration = new KafkaConfiguration(reconciliation, kafkaClusterSpec.getConfig().entrySet());
            if (configuration.getConfigOption(MIN_INSYNC_REPLICAS) != null) {
                cruiseControl.minInsyncReplicas = configuration.getConfigOption(MIN_INSYNC_REPLICAS);
            }

            Capacity capacity = new Capacity(kafkaAssembly.getSpec());
            cruiseControl.brokerDiskMiBCapacity = capacity.getDiskMiB();
            cruiseControl.brokerCpuUtilizationCapacity = capacity.getCpuUtilization();
            cruiseControl.brokerInboundNetworkKiBPerSecondCapacity = capacity.getInboundNetworkKiBPerSecond();
            cruiseControl.brokerOuboundNetworkKiBPerSecondCapacity = capacity.getOutboundNetworkKiBPerSecond();

            // Parse different types of metrics configurations
            ModelUtils.parseMetrics(cruiseControl, spec);

            if (spec.getReadinessProbe() != null) {
                cruiseControl.setReadinessProbe(spec.getReadinessProbe());
            }

            if (spec.getLivenessProbe() != null) {
                cruiseControl.setLivenessProbe(spec.getLivenessProbe());
            }

            Logging logging = spec.getLogging();
            cruiseControl.setLogging(logging == null ? new InlineLogging() : logging);

            cruiseControl.setGcLoggingEnabled(spec.getJvmOptions() == null ? DEFAULT_JVM_GC_LOGGING_ENABLED : spec.getJvmOptions().isGcLoggingEnabled());
            cruiseControl.setJvmOptions(spec.getJvmOptions());

            cruiseControl.setResources(spec.getResources());
            cruiseControl.setOwnerReference(kafkaAssembly);
            cruiseControl = updateTemplate(spec, cruiseControl);
        }

        return cruiseControl;
    }

    public CruiseControl updateConfiguration(CruiseControlSpec spec) {
        CruiseControlConfiguration userConfiguration = new CruiseControlConfiguration(reconciliation, spec.getConfig().entrySet());
        for (Map.Entry<String, String> defaultEntry : CruiseControlConfiguration.getCruiseControlDefaultPropertiesMap().entrySet()) {
            if (userConfiguration.getConfigOption(defaultEntry.getKey()) == null) {
                userConfiguration.setConfigOption(defaultEntry.getKey(), defaultEntry.getValue());
            }
        }
        // Ensure that the configured anomaly.detection.goals are a sub-set of the default goals
        checkGoals(userConfiguration);
        this.setConfiguration(userConfiguration);
        return this;
    }

    /**
     *  This method ensures that the checks in cruise-control/src/main/java/com/linkedin/kafka/cruisecontrol/config/KafkaCruiseControlConfig.java
     *  sanityCheckGoalNames() method (L118)  don't fail if a user submits custom default goals that have less members then the default
     *  anomaly.detection.goals.
     * @param configuration The configuration instance to be checked.
     * @throws UnsupportedOperationException If the configuration contains self.healing.goals configurations.
     */
    public void checkGoals(CruiseControlConfiguration configuration) {
        // If self healing goals are defined then these take precedence.
        // Right now, self.healing.goals must either be null or an empty list
        if (configuration.getConfigOption(CruiseControlConfigurationParameters.CRUISE_CONTROL_SELF_HEALING_CONFIG_KEY.toString()) != null) {
            String selfHealingGoalsString = configuration.getConfigOption(CruiseControlConfigurationParameters.CRUISE_CONTROL_SELF_HEALING_CONFIG_KEY.toString());
            List<String> selfHealingGoals = Arrays.asList(selfHealingGoalsString.split("\\s*,\\s*"));
            if (!selfHealingGoals.isEmpty()) {
                throw new UnsupportedOperationException("Cruise Control's self healing functionality is not currently supported. Please remove " +
                        CruiseControlConfigurationParameters.CRUISE_CONTROL_SELF_HEALING_CONFIG_KEY + " config");
            }
        }

        // If no anomaly detection goals have been defined by the user, the defaults defined in Cruise Control will be used.
        String anomalyGoalsString = configuration.getConfigOption(CruiseControlConfigurationParameters.CRUISE_CONTROL_ANOMALY_DETECTION_CONFIG_KEY.toString(), CRUISE_CONTROL_DEFAULT_ANOMALY_DETECTION_GOALS);
        Set<String> anomalyDetectionGoals = new HashSet<>(Arrays.asList(anomalyGoalsString.split("\\s*,\\s*")));

        String defaultGoalsString = configuration.getConfigOption(CruiseControlConfigurationParameters.CRUISE_CONTROL_DEFAULT_GOALS_CONFIG_KEY.toString(), CRUISE_CONTROL_GOALS);
        Set<String> defaultGoals = new HashSet<>(Arrays.asList(defaultGoalsString.split("\\s*,\\s*")));

        // Remove all the goals which are present in the default goals set from the anomaly detection goals
        anomalyDetectionGoals.removeAll(defaultGoals);

        if (!anomalyDetectionGoals.isEmpty()) {
            // If the anomaly detection goals contain goals which are not in the default goals then the CC startup
            // checks will fail, so we make the anomaly goals match the default goals
            configuration.setConfigOption(CruiseControlConfigurationParameters.CRUISE_CONTROL_ANOMALY_DETECTION_CONFIG_KEY.toString(), defaultGoalsString);
            LOGGER.warnCr(reconciliation, "Anomaly goals contained goals which are not in the configured default goals. Anomaly goals have " +
                    "been changed to match the specified default goals.");
        }
    }

    public static CruiseControl updateTemplate(CruiseControlSpec spec, CruiseControl cruiseControl) {
        if (spec.getTemplate() != null) {
            CruiseControlTemplate template = spec.getTemplate();

            ModelUtils.parsePodTemplate(cruiseControl, template.getPod());
            ModelUtils.parseInternalServiceTemplate(cruiseControl, template.getApiService());

            if (template.getDeployment() != null && template.getDeployment().getMetadata() != null) {
                cruiseControl.templateDeploymentLabels = template.getDeployment().getMetadata().getLabels();
                cruiseControl.templateDeploymentAnnotations = template.getDeployment().getMetadata().getAnnotations();
            }

            if (template.getCruiseControlContainer() != null && template.getCruiseControlContainer().getEnv() != null) {
                cruiseControl.templateCruiseControlContainerEnvVars = template.getCruiseControlContainer().getEnv();
            }

            if (template.getTlsSidecarContainer() != null && template.getTlsSidecarContainer().getEnv() != null) {
                cruiseControl.templateTlsSidecarContainerEnvVars = template.getTlsSidecarContainer().getEnv();
            }

            if (template.getCruiseControlContainer() != null && template.getCruiseControlContainer().getSecurityContext() != null) {
                cruiseControl.templateCruiseControlContainerSecurityContext = template.getCruiseControlContainer().getSecurityContext();
            }

            if (template.getTlsSidecarContainer() != null && template.getTlsSidecarContainer().getSecurityContext() != null) {
                cruiseControl.templateTlsSidecarContainerSecurityContext = template.getTlsSidecarContainer().getSecurityContext();
            }

            if (template.getServiceAccount() != null && template.getServiceAccount().getMetadata() != null) {
                cruiseControl.templateServiceAccountLabels = template.getServiceAccount().getMetadata().getLabels();
                cruiseControl.templateServiceAccountAnnotations = template.getServiceAccount().getMetadata().getAnnotations();
            }

            ModelUtils.parsePodDisruptionBudgetTemplate(cruiseControl, template.getPodDisruptionBudget());
        }
        return cruiseControl;
    }

    public static String cruiseControlName(String cluster) {
        return CruiseControlResources.deploymentName(cluster);
    }

    public static String cruiseControlServiceName(String cluster) {
        return CruiseControlResources.serviceName(cluster);
    }

    public List<HTTPHeader> getHttpHeaders() {
        if (this.authorizationEnabled) {
            return Arrays.asList(generateAuthHttpHeader(API_USER_NAME, API_USER_PASSWORD));
        }
        return null;
    }

    public Service generateService() {
        if (!isDeployed()) {
            return null;
        }

        List<ServicePort> ports = Collections.singletonList(createServicePort(REST_API_PORT_NAME, REST_API_PORT, REST_API_PORT, "TCP"));
        return createService("ClusterIP", ports, templateServiceAnnotations);
    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(1);

        portList.add(createContainerPort(REST_API_PORT_NAME, REST_API_PORT, "TCP"));

        if (isMetricsEnabled) {
            portList.add(createContainerPort(METRICS_PORT_NAME, METRICS_PORT, "TCP"));
        }

        return portList;
    }

    protected List<Volume> getVolumes(boolean isOpenShift) {
        return Arrays.asList(createTempDirVolume(),
                createTempDirVolume(TLS_SIDECAR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME),
                createSecretVolume(TLS_SIDECAR_CC_CERTS_VOLUME_NAME, CruiseControl.secretName(cluster), isOpenShift),
                createSecretVolume(TLS_SIDECAR_CA_CERTS_VOLUME_NAME, AbstractModel.clusterCaCertSecretName(cluster), isOpenShift),
                createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigMapName));
    }

    protected List<VolumeMount> getVolumeMounts() {
        return Arrays.asList(createTempDirVolumeMount(),
                createVolumeMount(CruiseControl.TLS_SIDECAR_CC_CERTS_VOLUME_NAME, CruiseControl.TLS_SIDECAR_CC_CERTS_VOLUME_MOUNT),
                createVolumeMount(CruiseControl.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, CruiseControl.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT),
                createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));
    }

    public Deployment generateDeployment(boolean isOpenShift, Map<String, String> annotations, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        if (!isDeployed()) {
            return null;
        }

        DeploymentStrategy updateStrategy = new DeploymentStrategyBuilder()
                .withType("RollingUpdate")
                .withRollingUpdate(new RollingUpdateDeploymentBuilder()
                        .withMaxSurge(new IntOrString(1))
                        .withMaxUnavailable(new IntOrString(0))
                        .build())
                .build();

        return createDeployment(
                updateStrategy,
                Collections.emptyMap(),
                Collections.emptyMap(),
                getMergedAffinity(),
                getInitContainers(imagePullPolicy),
                getContainers(imagePullPolicy),
                getVolumes(isOpenShift),
                imagePullSecrets);
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {
        List<Container> containers = new ArrayList<>(2);
        Container container = new ContainerBuilder()
                .withName(CRUISE_CONTROL_CONTAINER_NAME)
                .withImage(getImage())
                .withCommand("/opt/cruise-control/cruise_control_run.sh")
                .withEnv(getEnvVars())
                .withPorts(getContainerPortList())
                .withLivenessProbe(ProbeGenerator.httpProbe(livenessProbeOptions, livenessPath, REST_API_PORT_NAME, uriScheme, getHttpHeaders()))
                .withReadinessProbe(ProbeGenerator.httpProbe(readinessProbeOptions, readinessPath, REST_API_PORT_NAME, uriScheme, getHttpHeaders()))
                .withResources(getResources())
                .withVolumeMounts(getVolumeMounts())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .withSecurityContext(templateCruiseControlContainerSecurityContext)
                .build();

        String tlsSidecarImage = this.tlsSidecarImage;
        if (tlsSidecar != null && tlsSidecar.getImage() != null) {
            tlsSidecarImage = tlsSidecar.getImage();
        }

        Container tlsSidecarContainer = new ContainerBuilder()
                .withName(TLS_SIDECAR_NAME)
                .withImage(tlsSidecarImage)
                .withCommand("/opt/stunnel/cruise_control_stunnel_run.sh")
                .withLivenessProbe(ProbeGenerator.tlsSidecarLivenessProbe(tlsSidecar))
                .withReadinessProbe(ProbeGenerator.tlsSidecarReadinessProbe(tlsSidecar))
                .withResources(tlsSidecar != null ? tlsSidecar.getResources() : null)
                .withEnv(getTlsSidecarEnvVars())
                .withVolumeMounts(createTempDirVolumeMount(TLS_SIDECAR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME),
                        createVolumeMount(TLS_SIDECAR_CC_CERTS_VOLUME_NAME, TLS_SIDECAR_CC_CERTS_VOLUME_MOUNT),
                        createVolumeMount(TLS_SIDECAR_CA_CERTS_VOLUME_NAME, TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT))
                .withLifecycle(new LifecycleBuilder().withNewPreStop().withNewExec()
                        .withCommand("/opt/stunnel/cruise_control_stunnel_pre_stop.sh",
                                String.valueOf(templateTerminationGracePeriodSeconds))
                        .endExec().endPreStop().build())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, tlsSidecarImage))
                .withSecurityContext(templateTlsSidecarContainerSecurityContext)
                .build();

        containers.add(container);
        containers.add(tlsSidecarContainer);

        return containers;
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();

        varList.add(buildEnvVar(ENV_VAR_CRUISE_CONTROL_METRICS_ENABLED, String.valueOf(isMetricsEnabled)));
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_KAFKA_BOOTSTRAP_SERVERS, String.valueOf(defaultBootstrapServers(cluster))));
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));
        varList.add(buildEnvVar(ENV_VAR_MIN_INSYNC_REPLICAS, String.valueOf(minInsyncReplicas)));

        varList.add(buildEnvVar(ENV_VAR_BROKER_DISK_MIB_CAPACITY, String.valueOf(brokerDiskMiBCapacity)));
        varList.add(buildEnvVar(ENV_VAR_BROKER_CPU_UTILIZATION_CAPACITY, String.valueOf(brokerCpuUtilizationCapacity)));
        varList.add(buildEnvVar(ENV_VAR_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY, String.valueOf(brokerInboundNetworkKiBPerSecondCapacity)));
        varList.add(buildEnvVar(ENV_VAR_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY,  String.valueOf(brokerOuboundNetworkKiBPerSecondCapacity)));

        heapOptions(varList, 1.0, 0L);
        jvmPerformanceOptions(varList);

        if (configuration != null && !configuration.getConfiguration().isEmpty()) {
            varList.add(buildEnvVar(ENV_VAR_CRUISE_CONTROL_CONFIGURATION, configuration.getConfiguration()));
        }

        // Add shared environment variables used for all containers
        varList.addAll(getRequiredEnvVars());

        addContainerEnvsToExistingEnvs(varList, templateCruiseControlContainerEnvVars);

        return varList;
    }

    /**
     * Generates the PodDisruptionBudget.
     *
     * @return The PodDisruptionBudget.
     */
    public PodDisruptionBudget generatePodDisruptionBudget() {
        return createPodDisruptionBudget();
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "cruiseControlDefaultLoggingProperties";
    }

    @Override
    protected String getServiceAccountName() {
        return CruiseControlResources.serviceAccountName(cluster);
    }

    /**
     * Get the name of the Cruise Control service account given the name of the {@code cluster}.
     * @param cluster The cluster name
     * @return The name of the Cruise Control service account.
     */
    public static String cruiseControlServiceAccountName(String cluster) {
        return CruiseControlResources.serviceAccountName(cluster);
    }

    protected List<EnvVar> getTlsSidecarEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(ModelUtils.tlsSidecarLogEnvVar(tlsSidecar));
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_CONNECT, zookeeperConnect));

        // Add shared environment variables used for all containers
        varList.addAll(getRequiredEnvVars());

        addContainerEnvsToExistingEnvs(varList, templateTlsSidecarContainerEnvVars);

        return varList;
    }

    /**
     * Generates the name of the Cruise Control secret with certificates for connecting to Kafka brokers
     *
     * @param kafkaCluster  Name of the Kafka Custom Resource
     * @return  Name of the Cruise Control secret
     */
    public static String secretName(String kafkaCluster) {
        return CruiseControlResources.secretName(kafkaCluster);
    }

    /**
     * Returns whether the Cruise Control is enabled or not
     *
     * @return True if Cruise Control is enabled. False otherwise.
     */
    private boolean isDeployed() {
        return isDeployed;
    }

    /**
     * Creates Cruise Control API authorization usernames, passwords, and credentials file
     *
     * @return Map containing Cruise Control API authorization credentials
     */
    public static Map<String, String> generateCruiseControlApiCredentials() {
        PasswordGenerator passwordGenerator = new PasswordGenerator(16);
        String apiAdminPassword = passwordGenerator.generate();

        /*
         * Create Cruise Control API authorization credentials file following Jetty's
         *  HashLoginService's file format: username: password [,rolename ...]
         */
        String authCredentialsFile =
                API_ADMIN_NAME + ": " + apiAdminPassword + "," + API_ADMIN_ROLE + "\n" +
                API_USER_NAME + ": " + API_USER_PASSWORD + "," + API_USER_ROLE + "\n";

        Map<String, String> data = new HashMap<>(7);
        data.put(API_ADMIN_PASSWORD_KEY, encodeToBase64(apiAdminPassword));
        data.put(API_USER_PASSWORD_KEY, encodeToBase64(API_USER_PASSWORD));
        data.put(API_AUTH_FILE_KEY, encodeToBase64(authCredentialsFile));

        return data;
    }

    /**
     * Generate the Secret containing the Cruise Control certificate signed by the cluster CA certificate used for TLS based
     * internal communication with Kafka and Zookeeper.
     * It also contains the related Cruise Control private key.
     *
     * @param clusterCa The cluster CA.
     * @param isMaintenanceTimeWindowsSatisfied Indicates whether we are in the maintenance window or not.
     *                                          This is used for certificate renewals
     * @return The generated Secret.
     */
    public Secret generateSecret(ClusterCa clusterCa, boolean isMaintenanceTimeWindowsSatisfied) {
        if (!isDeployed()) {
            return null;
        }

        Secret secret = clusterCa.cruiseControlSecret();

        // Generate data used for API authorization
        Map<String, String> data = new HashMap<>(7);
        if (secret == null) {
            data = generateCruiseControlApiCredentials();
        } else {
            data.put(API_ADMIN_PASSWORD_KEY, secret.getData().get(API_ADMIN_PASSWORD_KEY));
            data.put(API_USER_PASSWORD_KEY, secret.getData().get(API_USER_PASSWORD_KEY));
            data.put(API_AUTH_FILE_KEY, secret.getData().get(API_AUTH_FILE_KEY));
        }

        return ModelUtils.buildSecret(reconciliation, clusterCa, secret, namespace, CruiseControl.secretName(cluster), name,
                "cruise-control", labels, data, createOwnerReference(),
                isMaintenanceTimeWindowsSatisfied);
    }

    /**
     * @param cluster The name of the cluster.
     * @return The name of the network policy.
     */
    public static String policyName(String cluster) {
        return cluster + NETWORK_POLICY_KEY_SUFFIX + NAME_SUFFIX;
    }

    /**
     * Generates the NetworkPolicies relevant for Cruise Control
     *
     * @param operatorNamespace                             Namespace where the Strimzi Cluster Operator runs. Null if not configured.
     * @param operatorNamespaceLabels                       Labels of the namespace where the Strimzi Cluster Operator runs. Null if not configured.
     *
     * @return The network policy.
     */
    public NetworkPolicy generateNetworkPolicy(String operatorNamespace, Labels operatorNamespaceLabels) {
        List<NetworkPolicyIngressRule> rules = new ArrayList<>(1);

        // CO can access the REST API
        NetworkPolicyIngressRule restApiRule = new NetworkPolicyIngressRuleBuilder()
                .addNewPort()
                    .withNewPort(REST_API_PORT)
                    .withProtocol("TCP")
                .endPort()
                .build();

        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                .withNewPodSelector() // cluster operator
                .addToMatchLabels(Labels.STRIMZI_KIND_LABEL, "cluster-operator")
                .endPodSelector()
                .build();
        ModelUtils.setClusterOperatorNetworkPolicyNamespaceSelector(clusterOperatorPeer, namespace, operatorNamespace, operatorNamespaceLabels);

        restApiRule.setFrom(Collections.singletonList(clusterOperatorPeer));

        rules.add(restApiRule);

        if (isMetricsEnabled) {
            NetworkPolicyIngressRule metricsRule = new NetworkPolicyIngressRuleBuilder()
                    .addNewPort()
                        .withNewPort(METRICS_PORT)
                        .withProtocol("TCP")
                    .endPort()
                    .withFrom()
                    .build();

            rules.add(metricsRule);
        }

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
                .withNewMetadata()
                    .withName(policyName(cluster))
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withNewPodSelector()
                        .addToMatchLabels(Labels.STRIMZI_NAME_LABEL, cruiseControlName(cluster))
                    .endPodSelector()
                .withIngress(rules)
                .endSpec()
                .build();

        LOGGER.traceCr(reconciliation, "Created network policy {}", networkPolicy);
        return networkPolicy;
    }

    @Override
    public String getAncillaryConfigMapKeyLogConfig() {
        return "log4j2.properties";
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
