/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.systemtest.Constants.GLOBAL_RECONCILIATION_COUNT;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaConnectorUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectorUtils.class);
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(KafkaConnector.RESOURCE_KIND);

    private KafkaConnectorUtils() {}

    /**
     * WaitForStabilityConnector method, verifying stability of connector
     * @param namespaceName Namespace name
     * @param connectorName connector name
     * @param connectPodName Connect pod name
     */
    public static void waitForConnectorStability(String namespaceName, String connectorName, String connectPodName) {
        // alternative to sync hassling AtomicInteger one could use an integer array instead
        // not need to be final because reference to the array does not get another array assigned
        int[] i = {0};

        TestUtils.waitFor("Waiting for stability of connector " + connectorName, Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                String availableConnectors = getCreatedConnectors(namespaceName, connectPodName);
                if (availableConnectors.contains(connectorName)) {
                    LOGGER.info("Connector with name {} is present. Remaining seconds for stability {}", connectorName,
                            Constants.GLOBAL_RECONCILIATION_COUNT - i[0]);
                    return i[0]++ == (Constants.GLOBAL_RECONCILIATION_COUNT);
                } else {
                    throw new RuntimeException("Connector" + connectorName + " is not stable!");
                }
            }, () -> ResourceManager.logCurrentResourceStatus(KafkaConnectorResource.kafkaConnectorClient().inNamespace(namespaceName).withName(connectorName).get())
        );
    }

    public static void waitForConnectorStability(String connectorName, String connectPodName) {
        waitForConnectorStability(kubeClient().getNamespace(), connectorName, connectPodName);
    }

    /**
     * Wait until KafkaConnector is in desired state
     * @param namespaceName Namespace name
     * @param connectorName name of KafkaConnector
     * @param state desired state
     */
    public static boolean waitForConnectorStatus(String namespaceName, String connectorName, Enum<?>  state) {
        KafkaConnector kafkaConnector = KafkaConnectorResource.kafkaConnectorClient().inNamespace(namespaceName).withName(connectorName).get();
        return ResourceManager.waitForResourceStatus(KafkaConnectorResource.kafkaConnectorClient(), kafkaConnector, state);
    }

    public static boolean waitForConnectorReady(String namespaceName, String connectorName) {
        return waitForConnectorStatus(namespaceName, connectorName, Ready);
    }

    public static boolean waitForConnectorReady(String connectorName) {
        return waitForConnectorReady(kubeClient().getNamespace(), connectorName);
    }

    public static boolean waitForConnectorNotReady(String namespaceName, String connectorName) {
        return waitForConnectorStatus(namespaceName, connectorName, NotReady);
    }

    public static boolean waitForConnectorNotReady(String connectorName) {
        return waitForConnectorNotReady(kubeClient().getNamespace(), connectorName);
    }

    public static String getCreatedConnectors(String namespaceName, String connectPodName) {
        return cmdKubeClient(namespaceName).execInPod(connectPodName, "/bin/bash", "-c",
                "curl -X GET http://localhost:8083/connectors"
        ).out();
    }

    public static void waitForConnectorDeletion(String connectorName) {
        TestUtils.waitFor(connectorName + " connector deletion", Constants.GLOBAL_POLL_INTERVAL, READINESS_TIMEOUT, () -> {
            if (KafkaConnectorResource.kafkaConnectorClient().inNamespace(kubeClient().getNamespace()).withName(connectorName).get() == null) {
                return true;
            } else {
                LOGGER.info("KafkaConnector: {} is not deleted yet, triggering force delete", connectorName);
                cmdKubeClient().deleteByName(KafkaConnector.RESOURCE_KIND, connectorName);
                return false;
            }
        }, () -> ResourceManager.logCurrentResourceStatus(KafkaConnectorResource.kafkaConnectorClient().inNamespace(kubeClient().getNamespace()).withName(connectorName).get()));
    }

    public static void createFileSinkConnector(String namespaceName, String podName, String topicName, String sinkFileName, String apiUrl) {
        cmdKubeClient(namespaceName).execInPod(podName, "/bin/bash", "-c",
            "curl -X POST -H \"Content-Type: application/json\" " + "--data '{ \"name\": \"sink-test\", " +
                "\"config\": " + "{ \"connector.class\": \"FileStreamSink\", " +
                "\"tasks.max\": \"1\", \"topics\": \"" + topicName + "\"," + " \"file\": \"" + sinkFileName + "\" } }' " +
                apiUrl + "/connectors"
        );
    }

    public static void createFileSinkConnector(String podName, String topicName, String sinkFileName, String apiUrl) {
        createFileSinkConnector(kubeClient().getNamespace(), podName, topicName, sinkFileName, apiUrl);
    }

    public static void waitForConnectorsTaskMaxChange(String namespaceName, String connectorName, int taskMax) {
        TestUtils.waitFor("Wait for KafkaConnector taskMax will change", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.GLOBAL_TIMEOUT,
            () -> (KafkaConnectorResource.kafkaConnectorClient().inNamespace(namespaceName)
                .withName(connectorName).get().getSpec().getTasksMax() == taskMax)
                && (KafkaConnectorResource.kafkaConnectorClient().inNamespace(namespaceName)
                .withName(connectorName).get().getStatus().getTasksMax() == taskMax)
        );
    }

    public static String getConnectorSpecFromConnectAPI(String namespaceName, String podName, String connectorName) {
        return cmdKubeClient(namespaceName).execInPod(podName, "/bin/bash", "-c",
            "curl http://localhost:8083/connectors/" + connectorName).out();
    }

    public static String getConnectorConfig(String namespaceName, String podName, String connectorName, String apiUrl) {
        return cmdKubeClient(namespaceName).execInPod(podName, "/bin/bash", "-c", "curl http://" + apiUrl + ":8083/connectors/" +
            connectorName + "/config").out();
    }

    public static String waitForConnectorConfigUpdate(String namespaceName, String podName, String connectorName, String oldConfig, String apiUrl) {
        TestUtils.waitFor("Wait for KafkaConnector config will contain desired config", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
            ResourceOperation.getTimeoutForResourceReadiness(KafkaConnector.RESOURCE_KIND),
            () -> !oldConfig.equals(getConnectorConfig(namespaceName, podName, connectorName, apiUrl)));
        return getConnectorConfig(namespaceName, podName, connectorName, apiUrl);
    }

    /**
     * Checks stability of Connector's spec on Connect API, which should be same like before changes
     */
    public static void waitForConnectorSpecFromConnectAPIStability(String namespaceName, String podName, String connectorName, String oldSpec) {
        int[] stableCounter = {0};

        TestUtils.waitFor("Connector's spec will be stable", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT, () -> {
            if (getConnectorSpecFromConnectAPI(namespaceName, podName, connectorName).equals(oldSpec)) {
                stableCounter[0]++;
                if (stableCounter[0] == Constants.GLOBAL_STABILITY_OFFSET_COUNT) {
                    LOGGER.info("Connector's spec is stable for {} polls intervals", stableCounter[0]);
                    return true;
                }
            } else {
                LOGGER.info("Connector's spec is not stable. Going to set the counter to zero.");
                stableCounter[0] = 0;
                return false;
            }
            LOGGER.info("Connector's spec gonna be stable in {} polls", Constants.GLOBAL_STABILITY_OFFSET_COUNT - stableCounter[0]);
            return false;
        });
    }

    public static void waitForConnectorWorkerStatus(String namespaceName, String podName, String connectName, String connectorName, String state) {
        LOGGER.info("Wait until KafkaConnector {}'s worker will be in {} state", connectorName, state);
        TestUtils.waitFor(String.format("KafkaConnector %s's worker will be in %s state", connectorName, state), Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.GLOBAL_TIMEOUT,
            () -> {
                JsonObject connectorStatus = new JsonObject(
                        cmdKubeClient().namespace(namespaceName).execInPod(podName,
                        "curl", "GET",
                        "http://" + KafkaConnectResources.serviceName(connectName) + ":8083/connectors/" + connectorName + "/status").out().trim()
                );
                return connectorStatus.getJsonObject("connector").getString("state").equals(state);
            }
        );
    }

    public static void loggerStabilityWait(String namespaceName, String connectClusterName, String podName, String desiredLogger, String connectorName) {
        int[] counter = {0};
        TestUtils.waitFor("Connector logger will be stable", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () ->  {
                String logger = cmdKubeClient().namespace(namespaceName).execInPod(podName, "curl",
                    "http://" + KafkaConnectResources.serviceName(connectClusterName) + ":8083/admin/loggers/" + connectorName).out();
                if (logger.contains(desiredLogger)) {
                    counter[0]++;
                    LOGGER.info("Logger level is {}. Remaining seconds for logger to be stable: {}", desiredLogger, GLOBAL_RECONCILIATION_COUNT - counter[0]);
                } else {
                    LOGGER.warn("Logger level has changed: {}. Reseting counter from {} to 0", logger, counter[0]);
                    counter[0] = 0;
                }

                if (counter[0] == GLOBAL_RECONCILIATION_COUNT) {
                    LOGGER.info("Logger for connector {} is stable", connectorName);
                    return true;
                }
                return false;
            }
        );
    }
}
