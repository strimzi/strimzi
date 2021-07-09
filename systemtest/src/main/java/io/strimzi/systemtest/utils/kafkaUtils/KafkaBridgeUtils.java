/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.templates.kubernetes.ServiceTemplates;
import io.strimzi.operator.common.model.Labels;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.resources.crd.KafkaBridgeResource.kafkaBridgeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaBridgeUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaBridgeUtils.class);

    private KafkaBridgeUtils() {}

    public static Service createBridgeNodePortService(String clusterName, String namespace, String serviceName) {
        Map<String, String> map = new HashMap<>();
        map.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        map.put(Labels.STRIMZI_KIND_LABEL, "KafkaBridge");
        map.put(Labels.STRIMZI_NAME_LABEL, clusterName + "-bridge");

        // Create node port service for expose bridge outside Kubernetes
        return ServiceTemplates.getSystemtestsServiceResource(serviceName, Constants.HTTP_BRIDGE_DEFAULT_PORT, namespace, "TCP")
                    .editSpec()
                        .withType("NodePort")
                        .withSelector(map)
                    .endSpec().build();
    }

    // ======================================= MESSAGE CHECKING ======================================================

    /**
     * Wait until KafkaBridge is in desired state
     * @param namespaceName Namespace name
     * @param clusterName name of KafkaBridge cluster
     * @param state desired state
     */
    public static boolean waitForKafkaBridgeStatus(String namespaceName, String clusterName, Enum<?> state) {
        KafkaBridge kafkaBridge = kafkaBridgeClient().inNamespace(namespaceName).withName(clusterName).get();
        return ResourceManager.waitForResourceStatus(kafkaBridgeClient(), kafkaBridge.getKind(), namespaceName,
            kafkaBridge.getMetadata().getName(), state, ResourceOperation.getTimeoutForResourceReadiness(kafkaBridge.getKind()));
    }

    public static boolean waitForKafkaBridgeReady(String namespaceName, String clusterName) {
        return waitForKafkaBridgeStatus(namespaceName, clusterName, Ready);
    }

    public static boolean waitForKafkaBridgeReady(String clusterName) {
        return waitForKafkaBridgeStatus(kubeClient().getNamespace(), clusterName, Ready);
    }

    public static boolean waitForKafkaBridgeNotReady(String clusterName) {
        return waitForKafkaBridgeStatus(kubeClient().getNamespace(), clusterName, NotReady);
    }
}
