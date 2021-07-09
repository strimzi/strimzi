/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.controllers;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.TreeMap;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class StatefulSetUtils {

    private static final Logger LOGGER = LogManager.getLogger(StatefulSetUtils.class);
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(Constants.STATEFUL_SET);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion(Constants.STATEFUL_SET);

    private StatefulSetUtils() { }

    /**
     * Returns a map of pod name to resource version for the pods currently in the given statefulset.
     * @param namespaceName Namespace name
     * @param name  The StatefulSet name
     * @return A map of pod name to resource version for pods in the given StatefulSet.
     */
    public static Map<String, String> ssSnapshot(String namespaceName, String name) {
        StatefulSet statefulSet = kubeClient(namespaceName).getStatefulSet(namespaceName, name);
        LabelSelector selector = statefulSet.getSpec().getSelector();
        return PodUtils.podSnapshot(namespaceName, selector);
    }

    public static Map<String, String> ssSnapshot(String name) {
        return ssSnapshot(kubeClient().getNamespace(), name);
    }

    /**
     * Method to check that all pods for expected StatefulSet were rolled
     * @param namespaceName Namespace name
     * @param name StatefulSet name
     * @param snapshot Snapshot of pods for StatefulSet before the rolling update
     * @return true when the pods for StatefulSet are recreated
     */
    public static boolean ssHasRolled(String namespaceName, String name, Map<String, String> snapshot) {
        boolean log = true;
        if (log) {
            LOGGER.debug("Existing snapshot: {}", new TreeMap<>(snapshot));
        }
        LabelSelector selector = null;
        int times = 60;
        do {
            selector = kubeClient(namespaceName).getStatefulSetSelectors(namespaceName, name);
            if (selector == null) {
                if (times-- == 0) {
                    throw new RuntimeException("Retry failed");
                }
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        } while (selector == null);

        Map<String, String> map = PodUtils.podSnapshot(namespaceName, selector);
        if (log) {
            LOGGER.debug("Current snapshot: {}", new TreeMap<>(map));
        }
        // rolled when all the pods in snapshot have a different version in map
        map.keySet().retainAll(snapshot.keySet());
        if (log) {
            LOGGER.debug("Pods in common: {}", new TreeMap<>(map));
        }
        for (Map.Entry<String, String> e : map.entrySet()) {
            String currentResourceVersion = e.getValue();
            String resourceName = e.getKey();
            String oldResourceVersion = snapshot.get(resourceName);
            if (oldResourceVersion.equals(currentResourceVersion)) {
                if (log) {
                    LOGGER.debug("At least {} hasn't rolled", resourceName);
                }
                return false;
            }
        }
        if (log) {
            LOGGER.debug("All pods seem to have rolled");
        }
        return true;
    }

    public static boolean ssHasRolled(String name, Map<String, String> snapshot) {
        return ssHasRolled(kubeClient().getNamespace(), name, snapshot);
    }

    /**
     *  Method to wait when StatefulSet will be recreated after rolling update
     * @param namespaceName Namespace name
     * @param name StatefulSet name
     * @param snapshot Snapshot of pods for StatefulSet before the rolling update
     * @return The snapshot of the StatefulSet after rolling update with Uid for every pod
     */
    public static Map<String, String> waitTillSsHasRolled(String namespaceName, String name, Map<String, String> snapshot) {
        LOGGER.info("Waiting for StatefulSet {} rolling update", name);
        TestUtils.waitFor("StatefulSet " + name + " rolling update",
            Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, ResourceOperation.timeoutForPodsOperation(snapshot.size()), () -> {
                try {
                    return ssHasRolled(namespaceName, name, snapshot);
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            });
        LOGGER.info("StatefulSet {} rolling update finished", name);
        return ssSnapshot(namespaceName, name);
    }

    public static Map<String, String> waitTillSsHasRolled(String name, Map<String, String> snapshot) {
        return waitTillSsHasRolled(kubeClient().getNamespace(), name, snapshot);
    }

    /**
     *  Method to wait when StatefulSet will be recreated after rolling update with wait for all pods ready
     * @param namespaceName Namespace name
     * @param name StatefulSet name
     * @param expectedPods Expected number of pods
     * @param snapshot Snapshot of pods for StatefulSet before the rolling update
     * @return The snapshot of the StatefulSet after rolling update with Uid for every pod
     */
    public static Map<String, String> waitTillSsHasRolled(String namespaceName, String name, int expectedPods, Map<String, String> snapshot) {
        waitTillSsHasRolled(namespaceName, name, snapshot);
        waitForAllStatefulSetPodsReady(namespaceName, name, expectedPods, READINESS_TIMEOUT);
        return ssSnapshot(namespaceName, name);
    }

    public static Map<String, String> waitTillSsHasRolled(String name, int expectedPods, Map<String, String> snapshot) {
        return waitTillSsHasRolled(kubeClient().getNamespace(), name, expectedPods, snapshot);
    }

    /**
     * Wait until the STS is ready and all of its Pods are also ready with custom timeout.
     *
     * @param namespaceName Namespace name
     * @param statefulSetName The name of the StatefulSet
     * @param expectPods The number of pods expected.
     */
    public static void waitForAllStatefulSetPodsReady(String namespaceName, String statefulSetName, int expectPods, long timeout) {
        String resourceName = statefulSetName.contains("-kafka") ? statefulSetName.replace("-kafka", "") : statefulSetName.replace("-zookeeper", "");

        LOGGER.info("Waiting for StatefulSet {} to be ready", statefulSetName);
        TestUtils.waitFor("StatefulSet " + statefulSetName + " to be ready", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, timeout,
            () -> kubeClient(namespaceName).getStatefulSetStatus(namespaceName, statefulSetName),
            () -> ResourceManager.logCurrentResourceStatus(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(resourceName).get()));

        LOGGER.info("Waiting for {} Pod(s) of StatefulSet {} to be ready", expectPods, statefulSetName);
        PodUtils.waitForPodsReady(namespaceName, kubeClient(namespaceName).getStatefulSetSelectors(namespaceName, statefulSetName), expectPods, true,
            () -> ResourceManager.logCurrentResourceStatus(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(resourceName).get()));
        LOGGER.info("StatefulSet {} is ready", statefulSetName);
    }

    public static void waitForAllStatefulSetPodsReady(String statefulSetName, int expectPods, long timeout) {
        waitForAllStatefulSetPodsReady(kubeClient().getNamespace(), statefulSetName, expectPods, timeout);
    }

    /**
     * Wait until the STS is ready and all of its Pods are also ready with default timeout.
     *
     * @param statefulSetName The name of the StatefulSet
     * @param expectPods The number of pods expected.
     */
    public static void waitForAllStatefulSetPodsReady(String statefulSetName, int expectPods) {
        waitForAllStatefulSetPodsReady(statefulSetName, expectPods, READINESS_TIMEOUT);
    }

    /**
     * Wait until the given StatefulSet has been deleted.
     * @param namespaceName Namespace name
     * @param name The name of the StatefulSet.
     */
    public static void waitForStatefulSetDeletion(String namespaceName, String name) {
        LOGGER.debug("Waiting for StatefulSet {} deletion", name);
        TestUtils.waitFor("StatefulSet " + name + " to be deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION, DELETION_TIMEOUT,
            () -> {
                if (kubeClient(namespaceName).getStatefulSet(namespaceName, name) == null) {
                    return true;
                } else {
                    LOGGER.warn("StatefulSet {} is not deleted yet! Triggering force delete by cmd client!", name);
                    cmdKubeClient(namespaceName).deleteByName("statefulset", name);
                    return false;
                }
            });
        LOGGER.debug("StatefulSet {} was deleted", name);
    }

    /**
     * Wait until the given StatefulSet has been recovered.
     * @param name The name of the StatefulSet.
     */
    public static void waitForStatefulSetRecovery(String name, String statefulSetUid) {
        LOGGER.info("Waiting for StatefulSet {}-{} recovery in namespace {}", name, statefulSetUid, kubeClient().getNamespace());
        TestUtils.waitFor("StatefulSet " + name + " to be recovered", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_RECOVERY,
            () -> !kubeClient().getStatefulSetUid(name).equals(statefulSetUid));
        LOGGER.info("StatefulSet {} was recovered", name);
    }

    public static void waitForStatefulSetLabelsChange(String namespaceName, String statefulSetName, Map<String, String> labels) {
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            boolean isK8sTag = entry.getKey().equals("controller-revision-hash") || entry.getKey().equals("statefulset.kubernetes.io/pod-name");
            boolean isStrimziTag = entry.getKey().startsWith(Labels.STRIMZI_DOMAIN);
            // ignoring strimzi.io and k8s labels
            if (!(isStrimziTag || isK8sTag)) {
                LOGGER.info("Waiting for Stateful set label change {} -> {}", entry.getKey(), entry.getValue());
                TestUtils.waitFor("Waits for StatefulSet label change " + entry.getKey() + " -> " + entry.getValue(), Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                    Constants.GLOBAL_TIMEOUT, () ->
                        kubeClient(namespaceName).getStatefulSet(namespaceName, statefulSetName).getMetadata().getLabels().get(entry.getKey()).equals(entry.getValue())
                );
            }
        }
    }

    public static void waitForStatefulSetLabelsDeletion(String namespaceName, String statefulSetName, String... labelKeys) {
        for (final String labelKey : labelKeys) {
            LOGGER.info("Waiting for StatefulSet label {} change to {}", labelKey, null);
            TestUtils.waitFor("Waiting for StatefulSet label" + labelKey + " change to " + null, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
                DELETION_TIMEOUT, () ->
                    kubeClient(namespaceName).getStatefulSet(namespaceName, statefulSetName).getMetadata().getLabels().get(labelKey) == null
            );
            LOGGER.info("StatefulSet label {} change to {}", labelKey, null);
        }
    }

    public static void waitForNoRollingUpdate(String namespaceName, String statefulSetName, Map<String, String> pods) {
        // alternative to sync hassling AtomicInteger one could use an integer array instead
        // not need to be final because reference to the array does not get another array assigned
        int[] i = {0};

        TestUtils.waitFor("Waiting for stability of rolling update will be not triggered", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                if (!StatefulSetUtils.ssHasRolled(namespaceName, statefulSetName, pods)) {
                    LOGGER.info("{} pods didn't roll. Remaining seconds for stability: {}", pods.toString(),
                            Constants.GLOBAL_RECONCILIATION_COUNT - i[0]);
                    return i[0]++ == Constants.GLOBAL_RECONCILIATION_COUNT;
                } else {
                    throw new RuntimeException(pods.toString() + " pods are rolling!");
                }
            }
        );
    }
}
