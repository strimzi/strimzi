/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.SetupClusterOperator;
import io.strimzi.systemtest.enums.DefaultNetworkPolicy;
import io.strimzi.systemtest.keycloak.KeycloakInstance;
import io.strimzi.systemtest.templates.kubernetes.NetworkPolicyTemplates;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.systemtest.utils.specific.KeycloakUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(OAUTH)
@Tag(REGRESSION)
public class OauthAbstractST extends AbstractST {

    protected static final Logger LOGGER = LogManager.getLogger(OauthAbstractST.class);
    protected static final String OAUTH_CLIENT_NAME = "hello-world-producer";
    protected static final String OAUTH_CLIENT_SECRET = "hello-world-producer-secret";
    protected static final String OAUTH_CLIENT_AUDIENCE_PRODUCER = "kafka-audience-producer";
    protected static final String OAUTH_CLIENT_AUDIENCE_CONSUMER = "kafka-audience-consumer";
    protected static final String OAUTH_CLIENT_AUDIENCE_SECRET = "kafka-audience-secret";

    protected static final String OAUTH_KAFKA_BROKER_NAME = "kafka-broker";
    protected static final String OAUTH_KAFKA_CLIENT_NAME = "kafka-client";
    protected static final String OAUTH_PRODUCER_NAME = "oauth-producer";
    protected static final String OAUTH_CONSUMER_NAME = "oauth-consumer";

    protected static final String CONNECT_OAUTH_SECRET = "my-connect-oauth";
    protected static final String MIRROR_MAKER_OAUTH_SECRET = "my-mirror-maker-oauth";
    protected static final String MIRROR_MAKER_2_OAUTH_SECRET = "my-mirror-maker-2-oauth";
    protected static final String BRIDGE_OAUTH_SECRET = "my-bridge-oauth";
    protected static final String OAUTH_KAFKA_BROKER_SECRET = "kafka-broker-secret";
    protected static final String OAUTH_KAFKA_PRODUCER_SECRET = "hello-world-producer-secret";
    protected static final String OAUTH_KAFKA_CONSUMER_SECRET = "hello-world-consumer-secret";
    protected static final String OAUTH_TEAM_A_SECRET = "team-a-client-secret";
    protected static final String OAUTH_TEAM_B_SECRET = "team-b-client-secret";
    protected static final String OAUTH_KAFKA_CLIENT_SECRET = "kafka-client-secret";
    protected static final String OAUTH_KEY = "clientSecret";

    protected final String audienceListenerPort = "9098";

    protected KeycloakInstance keycloakInstance;

    public static Map<String, Object> connectorConfig;
    static {
        connectorConfig = new HashMap<>();
        connectorConfig.put("config.storage.replication.factor", 1);
        connectorConfig.put("offset.storage.replication.factor", 1);
        connectorConfig.put("status.storage.replication.factor", 1);
    }

    protected void setupCoAndKeycloak(ExtensionContext extensionContext, String namespace) {

        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(namespace)
            .createInstallation()
            .runInstallation();

        resourceManager.createResource(extensionContext, NetworkPolicyTemplates.applyDefaultNetworkPolicy(namespace, DefaultNetworkPolicy.DEFAULT_TO_ALLOW));

        LOGGER.info("Deploying keycloak...");

        KeycloakUtils.deployKeycloak(namespace);

        SecretUtils.waitForSecretReady("credential-example-keycloak");
        String passwordEncoded = kubeClient().getSecret("credential-example-keycloak").getData().get("ADMIN_PASSWORD");
        String password = new String(Base64.getDecoder().decode(passwordEncoded.getBytes()));
        keycloakInstance = new KeycloakInstance("admin", password, namespace);

        createSecretsForDeployments();
    }

    @AfterEach
    void tearDownEach(ExtensionContext extensionContext) throws Exception {
        List<Job> clusterJobList = kubeClient().getJobList().getItems()
            .stream()
            .filter(
                job -> job.getMetadata().getName().contains(mapWithClusterNames.get(extensionContext.getDisplayName())))
            .collect(Collectors.toList());

        for (Job job : clusterJobList) {
            LOGGER.info("Deleting {} job", job.getMetadata().getName());
            JobUtils.deleteJobWithWait(job.getMetadata().getNamespace(), job.getMetadata().getName());
        }
    }

    /**
     * Auxiliary method, which creating kubernetes secrets
     * f.e name kafka-broker-secret -> will be encoded to base64 format and use as a key.
     */
    private void createSecretsForDeployments() {
        SecretUtils.createSecret(OAUTH_KAFKA_PRODUCER_SECRET, OAUTH_KEY, "aGVsbG8td29ybGQtcHJvZHVjZXItc2VjcmV0");
        SecretUtils.createSecret(OAUTH_KAFKA_CONSUMER_SECRET, OAUTH_KEY, "aGVsbG8td29ybGQtc3RyZWFtcy1zZWNyZXQ=");
        SecretUtils.createSecret(OAUTH_TEAM_A_SECRET, OAUTH_KEY, "dGVhbS1hLWNsaWVudC1zZWNyZXQ=");
        SecretUtils.createSecret(OAUTH_TEAM_B_SECRET, OAUTH_KEY, "dGVhbS1iLWNsaWVudC1zZWNyZXQ=");
        SecretUtils.createSecret(OAUTH_KAFKA_BROKER_SECRET, OAUTH_KEY, "a2Fma2EtYnJva2VyLXNlY3JldA==");
        SecretUtils.createSecret(CONNECT_OAUTH_SECRET, OAUTH_KEY, "a2Fma2EtY29ubmVjdC1zZWNyZXQ=");
        SecretUtils.createSecret(MIRROR_MAKER_OAUTH_SECRET, OAUTH_KEY, "a2Fma2EtbWlycm9yLW1ha2VyLXNlY3JldA==");
        SecretUtils.createSecret(MIRROR_MAKER_2_OAUTH_SECRET, OAUTH_KEY, "a2Fma2EtbWlycm9yLW1ha2VyLTItc2VjcmV0");
        SecretUtils.createSecret(BRIDGE_OAUTH_SECRET, OAUTH_KEY, "a2Fma2EtYnJpZGdlLXNlY3JldA==");
        SecretUtils.createSecret(OAUTH_CLIENT_AUDIENCE_SECRET, OAUTH_KEY, "a2Fma2EtYXVkaWVuY2Utc2VjcmV0");
        SecretUtils.createSecret(OAUTH_KAFKA_CLIENT_SECRET, OAUTH_KEY, "a2Fma2EtY2xpZW50LXNlY3JldA==");
    }
}


