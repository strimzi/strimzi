/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaUserUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaUserUtils.class);
    private static final String KAFKA_USER_NAME_PREFIX = "my-user-";
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private KafkaUserUtils() {}

    /**
     * Generated random name for the KafkaUser resource
     * @return random name with additional salt
     */
    public static String generateRandomNameOfKafkaUser() {
        String salt = new Random().nextInt(Integer.MAX_VALUE) + "-" + new Random().nextInt(Integer.MAX_VALUE);

        return  KAFKA_USER_NAME_PREFIX + salt;
    }

    public static void waitForKafkaUserCreation(String namespaceName, String userName) {
        KafkaUser kafkaUser = KafkaUserResource.kafkaUserClient().inNamespace(namespaceName).withName(userName).get();

        SecretUtils.waitForSecretReady(namespaceName, userName,
            () -> LOGGER.info(KafkaUserResource.kafkaUserClient().inNamespace(namespaceName).withName(userName).get()));

        ResourceManager.waitForResourceStatus(KafkaUserResource.kafkaUserClient(), kafkaUser, Ready);
    }

    public static void waitForKafkaUserCreation(String userName) {
        waitForKafkaUserCreation(kubeClient().getNamespace(), userName);
    }

    public static void waitForKafkaUserDeletion(String userName) {
        LOGGER.info("Waiting for KafkaUser deletion {}", userName);
        TestUtils.waitFor("KafkaUser deletion " + userName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, DELETION_TIMEOUT,
            () -> {
                if (KafkaUserResource.kafkaUserClient().inNamespace(kubeClient().getNamespace()).withName(userName).get() == null) {
                    return true;
                } else {
                    LOGGER.warn("KafkaUser {} is not deleted yet! Triggering force delete by cmd client!", userName);
                    cmdKubeClient().deleteByName(KafkaUser.RESOURCE_KIND, userName);
                    return false;
                }
            },
            () -> LOGGER.info(KafkaUserResource.kafkaUserClient().inNamespace(kubeClient().getNamespace()).withName(userName).get())
        );
        LOGGER.info("KafkaUser {} deleted", userName);
    }

    public static void waitForKafkaUserIncreaseObserverGeneration(String namespaceName, long observation, String userName) {
        TestUtils.waitFor("increase observation generation from " + observation + " for user " + userName,
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> observation < KafkaUserResource.kafkaUserClient()
                .inNamespace(namespaceName).withName(userName).get().getStatus().getObservedGeneration());
    }

    public static void waitUntilKafkaUserStatusConditionIsPresent(String namespaceName, String userName) {
        LOGGER.info("Wait until KafkaUser {} status is available", userName);
        TestUtils.waitFor("KafkaUser " + userName + " status is available", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> KafkaUserResource.kafkaUserClient().inNamespace(namespaceName).withName(userName).get().getStatus().getConditions() != null,
            () -> LOGGER.info(KafkaUserResource.kafkaUserClient().inNamespace(namespaceName).withName(userName).get())
        );
        LOGGER.info("KafkaUser {} status is available", userName);
    }

    /**
     * Wait until KafkaUser is in desired state
     * @param userName name of KafkaUser
     * @param state desired state
     */
    public static boolean waitForKafkaUserStatus(String userName, Enum<?> state) {
        KafkaUser kafkaUser = KafkaUserResource.kafkaUserClient().inNamespace(kubeClient().getNamespace()).withName(userName).get();
        return ResourceManager.waitForResourceStatus(KafkaUserResource.kafkaUserClient(), kafkaUser, state);
    }

    public static boolean waitForKafkaUserNotReady(String userName) {
        return waitForKafkaUserStatus(userName, NotReady);
    }

    public static String removeKafkaUserPart(File kafkaUserFile, String partName) {
        YAMLMapper mapper = new YAMLMapper();
        try {
            JsonNode node = mapper.readTree(kafkaUserFile);
            ObjectNode kafkaUserSpec = (ObjectNode) node.at("/spec");
            kafkaUserSpec.remove(partName);
            return mapper.writeValueAsString(node);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
