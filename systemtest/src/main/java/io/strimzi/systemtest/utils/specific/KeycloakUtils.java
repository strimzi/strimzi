/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.systemtest.Constants;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.executor.ExecResult;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

public class KeycloakUtils {

    private static final Logger LOGGER = LogManager.getLogger(KeycloakUtils.class);

    public final static String PATH_TO_KEYCLOAK_PREPARE_SCRIPT = "../systemtest/src/test/resources/oauth2/prepare_keycloak_operator.sh";
    public final static String PATH_TO_KEYCLOAK_TEARDOWN_SCRIPT = "../systemtest/src/test/resources/oauth2/teardown_keycloak_operator.sh";


    private KeycloakUtils() {}

    public static void deployKeycloak(String namespace) {
        LOGGER.info("Prepare Keycloak in namespace: {}", namespace);

        // This is needed because from time to time the first try fails on Azure
        TestUtils.waitFor("Keycloak instance readiness", Constants.KEYCLOAK_DEPLOYMENT_POLL, Constants.KEYCLOAK_DEPLOYMENT_TIMEOUT, () -> {
            ExecResult result = Exec.exec(true, "/bin/bash", PATH_TO_KEYCLOAK_PREPARE_SCRIPT, namespace);

            if (!result.out().contains("All realms were successfully imported")) {
                LOGGER.info("Errors occurred during Keycloak install: {}", result.err());
                return false;
            }
            return  true;
        });

        LOGGER.info("Keycloak in namespace {} is ready", namespace);
    }

    public static void deleteKeycloak(String namespace) {
        LOGGER.info("Teardown Keycloak in namespace: {}", namespace);
        Exec.exec(true, "/bin/bash", PATH_TO_KEYCLOAK_TEARDOWN_SCRIPT, namespace);
    }

    /**
     * Returns token from Keycloak API
     * @param baseURI base uri for accessing Keycloak API
     * @param userName name of user
     * @param password password of user
     * @return user token
     */
    public static String getToken(String baseURI, String userName, String password) {
        String coPodName = kubeClient().getClusterOperatorPodName();
        return new JsonObject(
            cmdKubeClient().execInPod(
                coPodName,
                "curl",
                "-v",
                "--insecure",
                "-X",
                "POST",
                "-d", "client_id=admin-cli&client_secret=aGVsbG8td29ybGQtcHJvZHVjZXItc2VjcmV0&grant_type=password&username=" + userName + "&password=" + password,
                baseURI + "/auth/realms/master/protocol/openid-connect/token"
            ).out()).getString("access_token");
    }

    /**
     * Returns specific realm from Keycloak API
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param desiredRealm realm we want to get
     * @return JsonObject with whole desired realm from Keycloak
     */
    public static JsonObject getKeycloakRealm(String baseURI, String token, String desiredRealm) {
        String coPodName = kubeClient().getClusterOperatorPodName();
        return new JsonObject(cmdKubeClient().execInPod(
            coPodName,
            "curl",
            "-v",
            "--insecure",
            "-X",
            "GET",
            baseURI + "/auth/admin/realms/" + desiredRealm,
            "-H", "Authorization: Bearer " + token
        ).out());
    }

    /**
     * Returns all clients for specific realm
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param desiredRealm realm we want to get clients from
     * @return JsonArray with all clients set for the specific realm
     */
    public static JsonArray getKeycloakRealmClients(String baseURI, String token, String desiredRealm) {
        String coPodName = kubeClient().getClusterOperatorPodName();
        return new JsonArray(cmdKubeClient().execInPod(
            coPodName,
            "curl",
            "-v",
            "--insecure",
            "-X",
            "GET",
            baseURI + "/auth/admin/realms/" + desiredRealm + "/clients",
            "-H", "Authorization: Bearer " + token
        ).out());
    }

    /**
     * Returns all policies from client of specific realm
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param desiredRealm realm we want to get clients from
     * @param clientId id of desired client
     * @return JsonArray with all policies for clients in specific realm
     */
    public static JsonArray getPoliciesFromRealmClient(String baseURI, String token, String desiredRealm, String clientId) {
        return getConfigFromResourceServerOfRealm(baseURI, token, desiredRealm, clientId, "policy");
    }

    /**
     * Returns "resources" for desired endpoint -> policies, resources ...
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param desiredRealm realm we want to get clients from
     * @param clientId id of desired client
     * @param endpoint endpoint for "resource" - resource, policy etc.
     * @return JsonArray with results from endpoint
     */
    private static JsonArray getConfigFromResourceServerOfRealm(String baseURI, String token, String desiredRealm, String clientId, String endpoint) {
        String coPodName = kubeClient().getClusterOperatorPodName();
        return new JsonArray(cmdKubeClient().execInPod(
            coPodName,
            "curl",
            "-v",
            "--insecure",
            "-X",
            "GET",
            baseURI + "/auth/admin/realms/" + desiredRealm + "/clients/" + clientId + "/authz/resource-server/" + endpoint,
            "-H", "Authorization: Bearer " + token
        ).out());
    }

    /**
     * Puts new configuration to the specific realm
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param desiredRealm realm where the config should be put
     * @param config configuration we want to put into the realm
     * @return response from server
     */
    public static String putConfigurationToRealm(String baseURI, String token, JsonObject config, String desiredRealm) {
        String coPodName = kubeClient().getClusterOperatorPodName();
        return cmdKubeClient().execInPod(
            coPodName,
            "curl",
            "-v",
            "--insecure",
            "-X",
            "PUT",
            baseURI + "/auth/admin/realms/" + desiredRealm,
            "-H", "Authorization: Bearer " + token,
            "-d", config.toString(),
            "-H", "Content-Type: application/json"
        ).out();
    }

    /**
     * Updates policies of specific client in realm
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param desiredRealm realm where the client policies should be updated
     * @param policy new updated policies
     * @param clientId id of client where we want to update policies
     * @return response from server
     */
    public static String updatePolicyOfRealmClient(String baseURI, String token, JsonObject policy, String desiredRealm, String clientId) {
        String coPodName = kubeClient().getClusterOperatorPodName();
        return cmdKubeClient().execInPod(
            coPodName,
            "curl",
            "-v",
            "--insecure",
            "-X",
            "PUT",
            baseURI + "/auth/admin/realms/" + desiredRealm + "/clients/" + clientId + "/authz/resource-server/policy/" + policy.getValue("id"),
            "-H", "Authorization: Bearer " + token,
            "-d", policy.toString(),
            "-H", "Content-Type: application/json"
        ).out();
    }
}
