/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.systemtest.Constants;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;


public class TracingUtils {

    private static final Logger LOGGER = LogManager.getLogger(TracingUtils.class);

    private static final String JAEGER_QUERY_SERVICE_ENDPOINT = "/jaeger/api/services";
    private static final String JAEGER_QUERY_SERVICE_TRACES_ENDPOINT = "/jaeger/api/traces";
    private static final String JAEGER_QUERY_SERVICE_PARAM_SERVICE = "?service=";
    private static final String JAEGER_QUERY_SERVICE_PARAM_OPERATION = "&operation=";
    private static final int JAEGER_QUERY_PORT = 16686;

    private TracingUtils() {}

    public static void verify(String namespaceName, String componentJaegerServiceName, String clientPodName, String jaegerServiceName) {
        verify(namespaceName, componentJaegerServiceName, clientPodName, null, jaegerServiceName);
    }

    public static void verify(String namespaceName, String componentJaegerServiceName, String clientPodName, String operation, String jaegerServiceName) {
        verifyThatServiceIsPresent(namespaceName, componentJaegerServiceName, clientPodName, jaegerServiceName);
        verifyThatServiceTracesArePresent(namespaceName, componentJaegerServiceName, clientPodName, operation, jaegerServiceName);
    }

    private static void verifyThatServiceIsPresent(String namespaceName, String componentJaegerServiceName, String clientPodName, String jaegerServiceName) {
        TestUtils.waitFor("Jaeger service " + componentJaegerServiceName + " to be present", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            JsonObject jaegerServices = new JsonObject(cmdKubeClient(namespaceName).execInPod(clientPodName, "/bin/bash", "-c", "curl " + jaegerServiceName + ":" + JAEGER_QUERY_PORT + JAEGER_QUERY_SERVICE_ENDPOINT).out());

            if (jaegerServices.getJsonArray("data").contains(componentJaegerServiceName)) {
                LOGGER.info("Jaeger service {} is present", componentJaegerServiceName);
                return true;
            } else {
                LOGGER.info("Jaeger service {} is not present. Present services are {}.", componentJaegerServiceName, jaegerServices.getJsonArray("data"));
                return false;
            }
        });
    }

    private static void verifyThatServiceTracesArePresent(String namespaceName, String componentJaegerServiceName, String clientPodName, String operation, String jaegerServiceName) {
        TestUtils.waitFor("Jaeger service " + componentJaegerServiceName + " has some traces", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            String query;
            if (operation == null)  {
                query = jaegerServiceName + ":" + JAEGER_QUERY_PORT + JAEGER_QUERY_SERVICE_TRACES_ENDPOINT + JAEGER_QUERY_SERVICE_PARAM_SERVICE + componentJaegerServiceName;
            } else {
                query = jaegerServiceName + ":" + JAEGER_QUERY_PORT + JAEGER_QUERY_SERVICE_TRACES_ENDPOINT + JAEGER_QUERY_SERVICE_PARAM_SERVICE + componentJaegerServiceName + JAEGER_QUERY_SERVICE_PARAM_OPERATION + operation;
            }

            JsonObject jaegerServicesTraces = new JsonObject(cmdKubeClient(namespaceName).execInPod(clientPodName,
                "/bin/bash", "-c", "curl " + query).out());
            JsonArray traces = jaegerServicesTraces.getJsonArray("data");

            if (!(jaegerServicesTraces.getJsonArray("data").size() > 0)) {
                LOGGER.error("Jaeger service {} does not contain data object", componentJaegerServiceName);
                return false;
            }

            for (Object trace : traces) {
                String traceId = ((JsonObject) trace).getString("traceID");

                if (!(traceId.matches("^[a-z0-9]+"))) {
                    LOGGER.error("Jaeger trace does not have correct trace Id {}", traceId);
                    return false;
                }

                JsonArray spans = ((JsonObject) trace).getJsonArray("spans");

                if (!(spans.size() > 0)) {
                    LOGGER.error("Jaeger trace {} does not have more than 0 spans inside trace specifically {} spans", traceId, spans.size());
                    return false;
                }

                JsonObject processes = ((JsonObject) trace).getJsonObject("processes");

                if (!(processes.size() > 0)) {
                    LOGGER.error("Jaeger trace {} does not have more that 0 processes specifically {} processes", traceId, processes);
                    return false;
                }

                JsonObject warnings = ((JsonObject) trace).getJsonObject("warnings");

                if (warnings != null) {
                    LOGGER.error("Jaeger trace {} contain some warnings {}", traceId, warnings.toString());
                    return false;
                }
            }
            return true;
        });
    }
}
