/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.security.CertAndKeyFiles;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class SecretUtils {

    private static final Logger LOGGER = LogManager.getLogger(SecretUtils.class);
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(Constants.SECRET);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private SecretUtils() { }

    public static void waitForSecretReady(String secretName) {
        waitForSecretReady(kubeClient().getNamespace(),  secretName, () -> { });
    }

    public static void waitForSecretReady(String namespaceName, String secretName, Runnable onTimeout) {
        LOGGER.info("Waiting for Secret {}", secretName);
        TestUtils.waitFor("Expected secret " + secretName + " exists", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> kubeClient(namespaceName).getSecret(namespaceName, secretName) != null,
            onTimeout);
        LOGGER.info("Secret {} created", secretName);
    }

    public static void waitForSecretDeletion(String secretName) {
        waitForSecretDeletion(secretName, () -> { });
    }

    public static void waitForSecretDeletion(String secretName, Runnable onTimeout) {
        LOGGER.info("Waiting for Secret deletion {}", secretName);
        TestUtils.waitFor("Expected secret " + secretName + " deleted", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> kubeClient().getSecret(secretName) == null,
            onTimeout);
        LOGGER.info("Secret {} deleted", secretName);
    }

    public static void createSecret(String namespaceName, String secretName, String dataKey, String dataValue) {
        LOGGER.info("Creating secret {}", secretName);
        kubeClient(namespaceName).createSecret(new SecretBuilder()
            .withApiVersion("v1")
            .withKind("Secret")
            .withNewMetadata()
                .withName(secretName)
                .withNamespace(namespaceName)
            .endMetadata()
            .withType("Opaque")
                .withData(Collections.singletonMap(dataKey, dataValue))
            .build());
    }

    public static void createSecret(String secretName, String dataKey, String dataValue) {
        createSecret(kubeClient().getNamespace(), secretName, dataKey, dataValue);
    }

    public static void createSecretFromFile(String pathToOrigin, String key, String name, String namespace, Map<String, String> labels) {
        createSecretFromFile(Collections.singletonMap(key, pathToOrigin), name, namespace, labels);
    }

    public static void createSecretFromFile(Map<String, String> certFilesPath, String name, String namespace, Map<String, String> labels) {
        byte[] encoded;
        try {
            Map<String, String> data = new HashMap<>();
            for (Map.Entry<String, String> entry : certFilesPath.entrySet()) {
                encoded = Files.readAllBytes(Paths.get(entry.getValue()));

                Base64.Encoder encoder = Base64.getEncoder();
                data.put(entry.getKey(), encoder.encodeToString(encoded));
            }

            Secret secret = new SecretBuilder()
                .withData(data)
                    .withNewMetadata()
                        .withName(name)
                        .withNamespace(namespace)
                        .addToLabels(labels)
                    .endMetadata()
                .build();
            kubeClient().namespace(namespace).createSecret(secret);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createCustomSecret(String name, String clusterName, String namespace, CertAndKeyFiles certAndKeyFiles) {
        Map<String, String> secretLabels = new HashMap<>();
        secretLabels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        secretLabels.put(Labels.STRIMZI_KIND_LABEL, "Kafka");

        Map<String, String> certsPaths = new HashMap<>();
        certsPaths.put("ca.crt", certAndKeyFiles.getCertPath());
        certsPaths.put("ca.key", certAndKeyFiles.getKeyPath());

        SecretUtils.createSecretFromFile(certsPaths, name, namespace, secretLabels);
        waitForSecretReady(namespace, name, () -> { });
    }

    public static void waitForCertToChange(String namespaceName, String originalCert, String secretName) {
        LOGGER.info("Waiting for Secret {} certificate change", secretName);
        TestUtils.waitFor("Cert to be replaced", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_CLUSTER_STABLE, () -> {
            Secret secret = kubeClient(namespaceName).getSecret(namespaceName, secretName);
            if (secret != null && secret.getData() != null && secret.getData().containsKey("ca.crt")) {
                String currentCert = new String(Base64.getDecoder().decode(secret.getData().get("ca.crt")), StandardCharsets.US_ASCII);
                boolean changed = !originalCert.equals(currentCert);
                if (changed) {
                    LOGGER.info("Certificate in Secret {} has changed, was {}, is now {}", secretName, originalCert, currentCert);
                }
                return changed;
            } else {
                return false;
            }
        });
    }

    public static void deleteSecretWithWait(String secretName, String namespace) {
        kubeClient().getClient().secrets().inNamespace(namespace).withName(secretName).delete();

        LOGGER.info("Waiting for Secret: {} to be deleted", secretName);
        TestUtils.waitFor(String.format("Deletion of secret: %s", secretName), Constants.GLOBAL_POLL_INTERVAL, DELETION_TIMEOUT,
            () -> kubeClient().getSecret(secretName) == null);

        LOGGER.info("Secret: {} successfully deleted", secretName);
    }

    public static X509Certificate getCertificateFromSecret(Secret secret, String dataKey) {
        String caCert = secret.getData().get(dataKey);
        byte[] decoded = Base64.getDecoder().decode(caCert);
        X509Certificate cacert = null;
        try {
            cacert = (X509Certificate)
                    CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(decoded));
        } catch (CertificateException e) {
            e.printStackTrace();
        }
        return cacert;
    }

    public static void waitForUserPasswordChange(String namespaceName, String secretName, String expectedEncodedPassword) {
        LOGGER.info("Waiting for user password will be changed to {} in secret: {}", expectedEncodedPassword, secretName);
        TestUtils.waitFor(String.format("user password will be changed to: %s in secret: %s", expectedEncodedPassword, secretName),
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> kubeClient().namespace(namespaceName).getSecret(secretName).getData().get("password").equals(expectedEncodedPassword));
    }
}
