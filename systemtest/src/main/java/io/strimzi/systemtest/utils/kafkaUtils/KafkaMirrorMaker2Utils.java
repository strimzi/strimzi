/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.systemtest.resources.ResourceManager;

import static io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaMirrorMaker2Utils {

    private KafkaMirrorMaker2Utils() {}

    /**
     * Wait until KafkaMirrorMaker2 will be in desired state
     * @param clusterName name of KafkaMirrorMaker2 cluster
     * @param state desired state
     */
    public static void waitUntilKafkaMirrorMaker2Status(String clusterName, String state) {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2Client().inNamespace(kubeClient().getNamespace()).withName(clusterName).get();
        ResourceManager.waitForStatus(kafkaMirrorMaker2Client(), kafkaMirrorMaker2, state);
    }

    public static void waitForKafkaMirrorMaker2Ready(String clusterName) {
        waitForKafkaMirrorMaker2Status(clusterName, "Ready");
    }

    public static void waitForKafkaMirrorMaker2NotReady(String clusterName) {
        waitForKafkaMirrorMaker2Status(clusterName, "NotReady");
    }
}
