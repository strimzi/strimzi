/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.apps.RollingUpdateDeploymentBuilder;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaExporterSpec;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.template.KafkaExporterTemplate;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.common.Reconciliation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KafkaExporter extends AbstractModel {
    protected static final String APPLICATION_NAME = "kafka-exporter";

    // Configuration for mounting certificates
    protected static final String KAFKA_EXPORTER_CERTS_VOLUME_NAME = "kafka-exporter-certs";
    protected static final String KAFKA_EXPORTER_CERTS_VOLUME_MOUNT = "/etc/kafka-exporter/kafka-exporter-certs/";
    protected static final String CLUSTER_CA_CERTS_VOLUME_NAME = "cluster-ca-certs";
    protected static final String CLUSTER_CA_CERTS_VOLUME_MOUNT = "/etc/kafka-exporter/cluster-ca-certs/";

    // Configuration defaults
    private static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    private static final int DEFAULT_HEALTHCHECK_TIMEOUT = 15;
    private static final int DEFAULT_HEALTHCHECK_PERIOD = 30;
    public static final Probe READINESS_PROBE_OPTIONS = new ProbeBuilder().withTimeoutSeconds(DEFAULT_HEALTHCHECK_TIMEOUT).withInitialDelaySeconds(DEFAULT_HEALTHCHECK_DELAY).withPeriodSeconds(DEFAULT_HEALTHCHECK_PERIOD).build();

    protected static final String ENV_VAR_KAFKA_EXPORTER_LOGGING = "KAFKA_EXPORTER_LOGGING";
    protected static final String ENV_VAR_KAFKA_EXPORTER_KAFKA_VERSION = "KAFKA_EXPORTER_KAFKA_VERSION";
    protected static final String ENV_VAR_KAFKA_EXPORTER_GROUP_REGEX = "KAFKA_EXPORTER_GROUP_REGEX";
    protected static final String ENV_VAR_KAFKA_EXPORTER_TOPIC_REGEX = "KAFKA_EXPORTER_TOPIC_REGEX";
    protected static final String ENV_VAR_KAFKA_EXPORTER_KAFKA_SERVER = "KAFKA_EXPORTER_KAFKA_SERVER";
    protected static final String ENV_VAR_KAFKA_EXPORTER_ENABLE_SARAMA = "KAFKA_EXPORTER_ENABLE_SARAMA";

    protected String groupRegex = ".*";
    protected String topicRegex = ".*";
    protected boolean saramaLoggingEnabled;
    protected String logging;
    protected String version;

    private boolean isDeployed;

    protected List<ContainerEnvVar> templateContainerEnvVars;
    protected SecurityContext templateContainerSecurityContext;

    /**
     * Constructor
     *
     * @param reconciliation The reconciliation
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     */
    protected KafkaExporter(Reconciliation reconciliation, HasMetadata resource) {
        super(reconciliation, resource, APPLICATION_NAME);
        this.name = KafkaExporterResources.deploymentName(cluster);
        this.replicas = 1;
        this.readinessPath = "/metrics";
        this.readinessProbeOptions = READINESS_PROBE_OPTIONS;
        this.livenessPath = "/metrics";
        this.livenessProbeOptions = READINESS_PROBE_OPTIONS;

        this.saramaLoggingEnabled = false;
        this.mountPath = "/var/lib/kafka";

        // Kafka Exporter is all about metrics - they are always enabled
        this.isMetricsEnabled = true;

    }

    public static KafkaExporter fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly, KafkaVersion.Lookup versions) {
        KafkaExporter kafkaExporter = new KafkaExporter(reconciliation, kafkaAssembly);

        KafkaExporterSpec spec = kafkaAssembly.getSpec().getKafkaExporter();
        if (spec != null) {
            kafkaExporter.isDeployed = true;

            kafkaExporter.setResources(spec.getResources());

            if (spec.getReadinessProbe() != null) {
                kafkaExporter.setReadinessProbe(spec.getReadinessProbe());
            }

            if (spec.getLivenessProbe() != null) {
                kafkaExporter.setLivenessProbe(spec.getLivenessProbe());
            }

            kafkaExporter.setGroupRegex(spec.getGroupRegex());
            kafkaExporter.setTopicRegex(spec.getTopicRegex());

            String image = spec.getImage();
            if (image == null) {
                KafkaClusterSpec kafkaClusterSpec = kafkaAssembly.getSpec().getKafka();
                image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE, versions.kafkaImage(kafkaClusterSpec.getImage(), versions.defaultVersion().version()));
            }
            kafkaExporter.setImage(image);

            kafkaExporter.setLogging(spec.getLogging());
            kafkaExporter.setSaramaLoggingEnabled(spec.getEnableSaramaLogging());

            if (spec.getTemplate() != null) {
                KafkaExporterTemplate template = spec.getTemplate();

                if (template.getDeployment() != null && template.getDeployment().getMetadata() != null) {
                    kafkaExporter.templateDeploymentLabels = template.getDeployment().getMetadata().getLabels();
                    kafkaExporter.templateDeploymentAnnotations = template.getDeployment().getMetadata().getAnnotations();
                }

                if (template.getContainer() != null && template.getContainer().getEnv() != null) {
                    kafkaExporter.templateContainerEnvVars = template.getContainer().getEnv();
                }

                if (template.getContainer() != null && template.getContainer().getSecurityContext() != null) {
                    kafkaExporter.templateContainerSecurityContext = template.getContainer().getSecurityContext();
                }

                if (template.getServiceAccount() != null && template.getServiceAccount().getMetadata() != null) {
                    kafkaExporter.templateServiceAccountLabels = template.getServiceAccount().getMetadata().getLabels();
                    kafkaExporter.templateServiceAccountAnnotations = template.getServiceAccount().getMetadata().getAnnotations();
                }

                ModelUtils.parsePodTemplate(kafkaExporter, template.getPod());
            }

            kafkaExporter.setVersion(versions.version(kafkaAssembly.getSpec().getKafka().getVersion()).version());
            kafkaExporter.setOwnerReference(kafkaAssembly);
        } else {
            kafkaExporter.isDeployed = false;
        }

        return kafkaExporter;
    }

    protected void setSaramaLoggingEnabled(boolean saramaLoggingEnabled) {
        this.saramaLoggingEnabled = saramaLoggingEnabled;
    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(1);
        portList.add(createContainerPort(METRICS_PORT_NAME, METRICS_PORT, "TCP"));
        return portList;
    }

    public Deployment generateDeployment(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
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
                imagePullSecrets
        );
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {
        List<Container> containers = new ArrayList<>(1);

        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(getImage())
                .withCommand("/opt/kafka-exporter/kafka_exporter_run.sh")
                .withEnv(getEnvVars())
                .withPorts(getContainerPortList())
                .withLivenessProbe(ProbeGenerator.httpProbe(livenessProbeOptions, livenessPath, METRICS_PORT_NAME))
                .withReadinessProbe(ProbeGenerator.httpProbe(readinessProbeOptions, readinessPath, METRICS_PORT_NAME))
                .withResources(getResources())
                .withVolumeMounts(createTempDirVolumeMount(),
                        VolumeUtils.createVolumeMount(KAFKA_EXPORTER_CERTS_VOLUME_NAME, KAFKA_EXPORTER_CERTS_VOLUME_MOUNT),
                        VolumeUtils.createVolumeMount(CLUSTER_CA_CERTS_VOLUME_NAME, CLUSTER_CA_CERTS_VOLUME_MOUNT))
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .withSecurityContext(templateContainerSecurityContext)
                .build();

        containers.add(container);

        return containers;
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();

        varList.add(buildEnvVar(ENV_VAR_KAFKA_EXPORTER_LOGGING, logging));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_EXPORTER_KAFKA_VERSION, version));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_EXPORTER_GROUP_REGEX, groupRegex));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_EXPORTER_TOPIC_REGEX, topicRegex));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_EXPORTER_KAFKA_SERVER, KafkaCluster.serviceName(cluster) + ":" + KafkaCluster.REPLICATION_PORT));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_EXPORTER_ENABLE_SARAMA, String.valueOf(saramaLoggingEnabled)));

        // Add shared environment variables used for all containers
        varList.addAll(getRequiredEnvVars());

        addContainerEnvsToExistingEnvs(varList, templateContainerEnvVars);

        return varList;
    }

    private List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>(3);

        volumeList.add(createTempDirVolume());
        volumeList.add(VolumeUtils.createSecretVolume(KAFKA_EXPORTER_CERTS_VOLUME_NAME, KafkaExporter.secretName(cluster), isOpenShift));
        volumeList.add(VolumeUtils.createSecretVolume(CLUSTER_CA_CERTS_VOLUME_NAME, AbstractModel.clusterCaCertSecretName(cluster), isOpenShift));

        return volumeList;
    }

    /**
     * Generates the name of the Kafka Exporter deployment
     *
     * @param kafkaCluster  Name of the Kafka Custom Resource
     * @return  Name of the Kafka Exporter deployment
     */
    public static String kafkaExporterName(String kafkaCluster) {
        return KafkaExporterResources.deploymentName(kafkaCluster);
    }

    /**
     * Generates the name of the Kafka Exporter secret with certificates for connecting to Kafka brokers
     *
     * @param kafkaCluster  Name of the Kafka Custom Resource
     * @return  Name of the Kafka Exporter secret
     */
    public static String secretName(String kafkaCluster) {
        return KafkaExporterResources.secretName(kafkaCluster);
    }

    /**
     * Get the name of the Kafka Exporter service account given the name of the {@code kafkaCluster}.
     * @param kafkaCluster The cluster name
     * @return The name of the KE service account.
     */
    public static String containerServiceAccountName(String kafkaCluster) {
        return kafkaExporterName(kafkaCluster);
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return null;
    }

    private void setGroupRegex(String groupRegex) {
        this.groupRegex = groupRegex;
    }

    private void setTopicRegex(String topicRegex) {
        this.topicRegex = topicRegex;
    }

    @Override
    protected String getServiceAccountName() {
        return KafkaExporterResources.serviceAccountName(cluster);
    }

    private void setLogging(String logging) {
        this.logging = logging;
    }

    private void setVersion(String version) {
        this.version = version;
    }

    /**
     * Returns whether the Kafka Exporter is enabled or not
     *
     * @return True if Kafka exporter is enabled. False otherwise.
     */
    private boolean isDeployed() {
        return isDeployed;
    }

    /**
     * Generate the Secret containing the Kafka Exporter certificate signed by the cluster CA certificate used for TLS based
     * internal communication with Kafka and Zookeeper.
     * It also contains the related Kafka Exporter private key.
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
        Secret secret = clusterCa.kafkaExporterSecret();
        return ModelUtils.buildSecret(reconciliation, clusterCa, secret, namespace, KafkaExporter.secretName(cluster), name,
                "kafka-exporter", labels, null, createOwnerReference(), isMaintenanceTimeWindowsSatisfied);
    }
}
