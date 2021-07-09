/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.Spec;
import io.strimzi.api.kafka.model.status.Status;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.enums.DefaultNetworkPolicy;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.templates.kubernetes.NetworkPolicyTemplates;
import io.strimzi.systemtest.utils.StUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.List;
import java.util.stream.Collectors;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class NetworkPolicyResource implements ResourceType<NetworkPolicy> {

    private static final Logger LOGGER = LogManager.getLogger(NetworkPolicyResource.class);

    @Override
    public String getKind() {
        return Constants.NETWORK_POLICY;
    }
    @Override
    public NetworkPolicy get(String namespace, String name) {
        return ResourceManager.kubeClient().namespace(namespace).getNetworkPolicy(name);
    }
    @Override
    public void create(NetworkPolicy resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).createNetworkPolicy(resource);
    }
    @Override
    public void delete(NetworkPolicy resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).deleteNetworkPolicy(resource.getMetadata().getName());
    }
    @Override
    public boolean waitForReadiness(NetworkPolicy resource) {
        return resource != null;
    }

    /**
     * Method for allowing network policies for ClusterOperator
     */
    public static void allowNetworkPolicySettingsForClusterOperator(ExtensionContext extensionContext) {
        allowNetworkPolicySettingsForClusterOperator(extensionContext, kubeClient().getNamespace());
    }

    public static void allowNetworkPolicySettingsForClusterOperator(ExtensionContext extensionContext, String namespace) {
        String clusterOperatorKind = "cluster-operator";
        LabelSelector labelSelector = new LabelSelectorBuilder()
            .addToMatchLabels(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_CLIENTS_LABEL_VALUE)
            .build();

        LOGGER.info("Apply NetworkPolicy access to {} from pods with LabelSelector {}", clusterOperatorKind, labelSelector);

        NetworkPolicy networkPolicy = NetworkPolicyTemplates.networkPolicyBuilder(namespace, clusterOperatorKind, labelSelector)
            .editSpec()
                .editFirstIngress()
                    .addNewPort()
                        .withNewPort(Constants.CLUSTER_OPERATOR_METRICS_PORT)
                        .withProtocol("TCP")
                    .endPort()
                .endIngress()
                .withNewPodSelector()
                    .addToMatchLabels("strimzi.io/kind", clusterOperatorKind)
                .endPodSelector()
            .endSpec()
            .build();

        LOGGER.debug("Going to apply the following NetworkPolicy: {}", networkPolicy.toString());
        ResourceManager.getInstance().createResource(extensionContext, networkPolicy);
        LOGGER.info("Network policy for LabelSelector {} successfully applied", labelSelector);
    }

    public static void allowNetworkPolicySettingsForEntityOperator(ExtensionContext extensionContext, String clusterName) {
        allowNetworkPolicySettingsForEntityOperator(extensionContext, clusterName, kubeClient().getNamespace());
    }

    public static void allowNetworkPolicySettingsForEntityOperator(ExtensionContext extensionContext, String clusterName, String namespace) {
        LabelSelector labelSelector = new LabelSelectorBuilder()
                .addToMatchLabels(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_CLIENTS_LABEL_VALUE)
                .build();

        String eoDeploymentName = KafkaResources.entityOperatorDeploymentName(clusterName);

        LOGGER.info("Apply NetworkPolicy access to {} from pods with LabelSelector {}", eoDeploymentName, labelSelector);

        NetworkPolicy networkPolicy = NetworkPolicyTemplates.networkPolicyBuilder(namespace, eoDeploymentName, labelSelector)
            .editSpec()
                .editFirstIngress()
                    .addNewPort()
                        .withNewPort(Constants.TOPIC_OPERATOR_METRICS_PORT)
                        .withProtocol("TCP")
                    .endPort()
                    .addNewPort()
                        .withNewPort(Constants.USER_OPERATOR_METRICS_PORT)
                        .withProtocol("TCP")
                    .endPort()
                .endIngress()
                .withNewPodSelector()
                    .addToMatchLabels("strimzi.io/cluster", clusterName)
                    .addToMatchLabels("strimzi.io/kind", Kafka.RESOURCE_KIND)
                    .addToMatchLabels("strimzi.io/name", eoDeploymentName)
                .endPodSelector()
            .endSpec()
            .build();

        LOGGER.debug("Going to apply the following NetworkPolicy: {}", networkPolicy.toString());
        ResourceManager.getInstance().createResource(extensionContext, networkPolicy);
        LOGGER.info("Network policy for LabelSelector {} successfully applied", labelSelector);
    }

    public static void allowNetworkPolicySettingsForKafkaExporter(ExtensionContext extensionContext, String clusterName) {
        allowNetworkPolicySettingsForKafkaExporter(extensionContext, clusterName, kubeClient().getNamespace());
    }

    public static void allowNetworkPolicySettingsForKafkaExporter(ExtensionContext extensionContext, String clusterName, String namespace) {
        String kafkaExporterDeploymentName = KafkaExporterResources.deploymentName(clusterName);
        LabelSelector labelSelector = new LabelSelectorBuilder()
                .addToMatchLabels(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_CLIENTS_LABEL_VALUE)
                .build();

        LOGGER.info("Apply NetworkPolicy access to {} from pods with LabelSelector {}", kafkaExporterDeploymentName, labelSelector);

        NetworkPolicy networkPolicy = NetworkPolicyTemplates.networkPolicyBuilder(namespace, kafkaExporterDeploymentName, labelSelector)
            .editSpec()
                .editFirstIngress()
                    .addNewPort()
                        .withNewPort(Constants.COMPONENTS_METRICS_PORT)
                        .withProtocol("TCP")
                    .endPort()
                .endIngress()
                .withNewPodSelector()
                    .addToMatchLabels("strimzi.io/cluster", clusterName)
                    .addToMatchLabels("strimzi.io/kind", Kafka.RESOURCE_KIND)
                    .addToMatchLabels("strimzi.io/name", kafkaExporterDeploymentName)
                .endPodSelector()
            .endSpec()
            .build();

        LOGGER.debug("Going to apply the following NetworkPolicy: {}", networkPolicy.toString());
        ResourceManager.getInstance().createResource(extensionContext, networkPolicy);
        LOGGER.info("Network policy for LabelSelector {} successfully applied", labelSelector);
    }

    /**
     * Method for allowing network policies for Connect
     * @param resource mean Connect resource
     * @param deploymentName name of resource deployment - for setting strimzi.io/name
     */
    public static void allowNetworkPolicySettingsForResource(ExtensionContext extensionContext, HasMetadata resource, String deploymentName) {
        LabelSelector labelSelector = new LabelSelectorBuilder()
            .addToMatchLabels(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_CLIENTS_LABEL_VALUE)
            .build();

        final String namespaceName = StUtils.isParallelNamespaceTest(extensionContext) && !Environment.isNamespaceRbacScope() ?
            // if parallel namespace test use namespace from store and if RBAC is enable we don't run tests in parallel mode and with that said we don't create another namespaces
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.NAMESPACE_KEY).toString() :
            // otherwise use resource namespace
            resource.getMetadata().getNamespace();

        if (kubeClient(namespaceName).listPods(namespaceName, labelSelector).size() == 0) {
            List<String> pods = kubeClient(namespaceName).listPods(namespaceName).stream()
                .map(pod -> pod.getMetadata().getName()).collect(Collectors.toList());
            LOGGER.error("Pods inside {} namespace are {}", namespaceName, pods.toString());
            throw new RuntimeException("You did not create the Kafka Client instance(pod) before using the " + resource.getKind() + " in namespace:" + namespaceName);
        }

        LOGGER.info("Apply NetworkPolicy access to {} from pods with LabelSelector {}", deploymentName, labelSelector);

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
            .withApiVersion("networking.k8s.io/v1")
            .withKind(Constants.NETWORK_POLICY)
            .withNewMetadata()
                .withName(resource.getMetadata().getName() + "-allow")
                .withNamespace(namespaceName)
            .endMetadata()
            .withNewSpec()
                .addNewIngress()
                    .addNewFrom()
                        .withPodSelector(labelSelector)
                    .endFrom()
                    .addNewPort()
                        .withNewPort(8083)
                        .withProtocol("TCP")
                    .endPort()
                    .addNewPort()
                        .withNewPort(9404)
                        .withProtocol("TCP")
                    .endPort()
                    .addNewPort()
                        .withNewPort(8080)
                        .withProtocol("TCP")
                    .endPort()
                    .addNewPort()
                        .withNewPort(Constants.JMX_PORT)
                        .withProtocol("TCP")
                    .endPort()
                .endIngress()
                .withNewPodSelector()
                    .addToMatchLabels("strimzi.io/cluster", resource.getMetadata().getName())
                    .addToMatchLabels("strimzi.io/kind", resource.getKind())
                    .addToMatchLabels("strimzi.io/name", deploymentName)
                .endPodSelector()
                .withPolicyTypes("Ingress")
            .endSpec()
            .build();

        LOGGER.debug("Going to apply the following NetworkPolicy: {}", networkPolicy.toString());
        ResourceManager.getInstance().createResource(extensionContext, networkPolicy);
        LOGGER.info("Network policy for LabelSelector {} successfully applied", labelSelector);
    }

    public static void applyDefaultNetworkPolicySettings(ExtensionContext extensionContext, List<String> namespaces) {
        for (String namespace : namespaces) {
            if (Environment.DEFAULT_TO_DENY_NETWORK_POLICIES) {
                NetworkPolicyTemplates.applyDefaultNetworkPolicy(namespace, DefaultNetworkPolicy.DEFAULT_TO_DENY);
            } else {
                NetworkPolicyTemplates.applyDefaultNetworkPolicy(namespace, DefaultNetworkPolicy.DEFAULT_TO_ALLOW);
            }
            LOGGER.info("NetworkPolicy successfully set to: {} for namespace: {}", Environment.DEFAULT_TO_DENY_NETWORK_POLICIES, namespace);
        }
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> void deployNetworkPolicyForResource(ExtensionContext extensionContext, T resource, String deploymentName) {
        if (Environment.DEFAULT_TO_DENY_NETWORK_POLICIES) {
            allowNetworkPolicySettingsForResource(extensionContext, resource, deploymentName);
        }
    }
}
