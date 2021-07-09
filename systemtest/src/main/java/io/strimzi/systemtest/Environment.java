/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.systemtest.enums.ClusterOperatorInstallType;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.cluster.OpenShift;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Class which holds environment variables for system tests.
 */
public class Environment {

    private static final Logger LOGGER = LogManager.getLogger(Environment.class);
    private static final Map<String, String> VALUES = new HashMap<>();
    private static final JsonNode JSON_DATA = loadConfigurationFile();

    /**
     * Specify the system test configuration file path from an environmental variable
     */
    private static final String CONFIG_FILE_PATH_ENVAR = "ST_CONFIG_PATH";
    /**
     * Specify organization which owns image used in system tests.
     */
    private static final String STRIMZI_ORG_ENV = "DOCKER_ORG";
    /**
     * Specify registry for images used in system tests.
     */
    private static final String STRIMZI_REGISTRY_ENV = "DOCKER_REGISTRY";
    /**
     * Specify image tags used in system tests.
     */
    private static final String STRIMZI_TAG_ENV = "DOCKER_TAG";
    /**
     * Specify test-client image used in system tests.
     */
    private static final String TEST_CLIENT_IMAGE_ENV = "TEST_CLIENT_IMAGE";
    /**
     * Specify kafka bridge image used in system tests.
     */
    private static final String BRIDGE_IMAGE_ENV = "BRIDGE_IMAGE";
    /**
     * Directory for store logs collected during the tests.
     */
    private static final String TEST_LOG_DIR_ENV = "TEST_LOG_DIR";
    /**
     * Kafka version used in images during the system tests.
     */
    private static final String ST_KAFKA_VERSION_ENV = "ST_KAFKA_VERSION";
    /**
     * Log level for cluster operator.
     */
    private static final String STRIMZI_LOG_LEVEL_ENV = "STRIMZI_LOG_LEVEL";
    /**
     * Image pull policy env var for Components images (Kafka, Bridge, ...)
     */
    private static final String COMPONENTS_IMAGE_PULL_POLICY_ENV = "COMPONENTS_IMAGE_PULL_POLICY";
    /**
     * Image pull policy env var for Operator images
     */
    private static final String OPERATOR_IMAGE_PULL_POLICY_ENV = "OPERATOR_IMAGE_PULL_POLICY";
    /**
     * CO Roles only mode.
     */
    private static final String STRIMZI_RBAC_SCOPE_ENV = "STRIMZI_RBAC_SCOPE";
    private static final String STRIMZI_RBAC_SCOPE_CLUSTER = "CLUSTER";
    private static final String STRIMZI_RBAC_SCOPE_NAMESPACE = "NAMESPACE";
    private static final String STRIMZI_RBAC_SCOPE_DEFAULT = STRIMZI_RBAC_SCOPE_CLUSTER;

    /**
     * OLM env variables
     */
    private static final String OLM_OPERATOR_NAME_ENV = "OLM_OPERATOR_NAME";
    private static final String OLM_OPERATOR_DEPLOYMENT_NAME_ENV = "OLM_OPERATOR_DEPLOYMENT_NAME";
    private static final String OLM_SOURCE_NAME_ENV = "OLM_SOURCE_NAME";
    private static final String OLM_SOURCE_NAMESPACE_ENV = "OLM_SOURCE_NAMESPACE";
    private static final String OLM_APP_BUNDLE_PREFIX_ENV = "OLM_APP_BUNDLE_PREFIX";
    private static final String OLM_OPERATOR_VERSION_ENV = "OLM_OPERATOR_VERSION";
    /**
     * Allows network policies
     */
    private static final String DEFAULT_TO_DENY_NETWORK_POLICIES_ENV = "DEFAULT_TO_DENY_NETWORK_POLICIES";
    /**
     * ClusterOperator installation type
     */
    private static final String CLUSTER_OPERATOR_INSTALL_TYPE_ENV = "CLUSTER_OPERATOR_INSTALL_TYPE";

    private static final String SKIP_TEARDOWN_ENV = "SKIP_TEARDOWN";

    /**
     * Use finalizers for loadbalancers
     */
    private static final String LB_FINALIZERS_ENV = "LB_FINALIZERS";

    /**
     * CO Features gates variable
     */
    public static final String STRIMZI_FEATURE_GATES_ENV = "STRIMZI_FEATURE_GATES";

    /**
     * Defaults
     */
    private static final String ST_KAFKA_VERSION_DEFAULT = "2.8.0";
    public static final String STRIMZI_ORG_DEFAULT = "strimzi";
    public static final String STRIMZI_TAG_DEFAULT = "latest";
    public static final String STRIMZI_REGISTRY_DEFAULT = "quay.io";
    public static final String STRIMZI_CLIENTS_ORG_DEFAULT = "strimzi-examples";
    private static final String TEST_LOG_DIR_DEFAULT = TestUtils.USER_PATH + "/../systemtest/target/logs/";
    private static final String STRIMZI_LOG_LEVEL_DEFAULT = "DEBUG";
    public static final String COMPONENTS_IMAGE_PULL_POLICY_ENV_DEFAULT = Constants.IF_NOT_PRESENT_IMAGE_PULL_POLICY;
    public static final String OPERATOR_IMAGE_PULL_POLICY_ENV_DEFAULT = Constants.ALWAYS_IMAGE_PULL_POLICY;
    public static final String OLM_OPERATOR_NAME_DEFAULT = "strimzi-kafka-operator";
    public static final String OLM_OPERATOR_DEPLOYMENT_NAME_DEFAULT = Constants.STRIMZI_DEPLOYMENT_NAME;
    public static final String OLM_SOURCE_NAME_DEFAULT = "community-operators";
    public static final String OLM_APP_BUNDLE_PREFIX_DEFAULT = "strimzi-cluster-operator";
    public static final String OLM_OPERATOR_VERSION_DEFAULT = "0.23.0";
    private static final boolean DEFAULT_TO_DENY_NETWORK_POLICIES_DEFAULT = true;
    private static final ClusterOperatorInstallType CLUSTER_OPERATOR_INSTALL_TYPE_DEFAULT = ClusterOperatorInstallType.BUNDLE;
    private static final boolean LB_FINALIZERS_DEFAULT = false;
    private static final String STRIMZI_FEATURE_GATES_DEFAULT = "";

    /**
     * Set values
     */
    private static String config;
    public static final String STRIMZI_ORG = getOrDefault(STRIMZI_ORG_ENV, STRIMZI_ORG_DEFAULT);
    public static final String STRIMZI_TAG = getOrDefault(STRIMZI_TAG_ENV, STRIMZI_TAG_DEFAULT);
    public static final String STRIMZI_REGISTRY = getOrDefault(STRIMZI_REGISTRY_ENV, STRIMZI_REGISTRY_DEFAULT);
    public static final String TEST_LOG_DIR = getOrDefault(TEST_LOG_DIR_ENV, TEST_LOG_DIR_DEFAULT);
    public static final String ST_KAFKA_VERSION = getOrDefault(ST_KAFKA_VERSION_ENV, ST_KAFKA_VERSION_DEFAULT);
    public static final String STRIMZI_LOG_LEVEL = getOrDefault(STRIMZI_LOG_LEVEL_ENV, STRIMZI_LOG_LEVEL_DEFAULT);
    public static final boolean SKIP_TEARDOWN = getOrDefault(SKIP_TEARDOWN_ENV, Boolean::parseBoolean, false);
    public static final String STRIMZI_RBAC_SCOPE = getOrDefault(STRIMZI_RBAC_SCOPE_ENV, STRIMZI_RBAC_SCOPE_DEFAULT);
    public static final String STRIMZI_FEATURE_GATES = getOrDefault(STRIMZI_FEATURE_GATES_ENV, STRIMZI_FEATURE_GATES_DEFAULT);
    // variables for test-client image
    private static final String TEST_CLIENT_IMAGE_DEFAULT = STRIMZI_REGISTRY + "/" + STRIMZI_ORG + "/test-client:" + STRIMZI_TAG + "-kafka-" + ST_KAFKA_VERSION;
    public static final String TEST_CLIENT_IMAGE = getOrDefault(TEST_CLIENT_IMAGE_ENV, TEST_CLIENT_IMAGE_DEFAULT);
    // variables for kafka bridge image
    private static final String BRIDGE_IMAGE_DEFAULT = "latest-released";
    public static final String BRIDGE_IMAGE = getOrDefault(BRIDGE_IMAGE_ENV, BRIDGE_IMAGE_DEFAULT);
    // Image pull policy variables
    public static final String COMPONENTS_IMAGE_PULL_POLICY = getOrDefault(COMPONENTS_IMAGE_PULL_POLICY_ENV, COMPONENTS_IMAGE_PULL_POLICY_ENV_DEFAULT);
    public static final String OPERATOR_IMAGE_PULL_POLICY = getOrDefault(OPERATOR_IMAGE_PULL_POLICY_ENV, OPERATOR_IMAGE_PULL_POLICY_ENV_DEFAULT);
    // OLM env variables
    public static final String OLM_OPERATOR_NAME = getOrDefault(OLM_OPERATOR_NAME_ENV, OLM_OPERATOR_NAME_DEFAULT);
    public static final String OLM_OPERATOR_DEPLOYMENT_NAME = getOrDefault(OLM_OPERATOR_DEPLOYMENT_NAME_ENV, OLM_OPERATOR_DEPLOYMENT_NAME_DEFAULT);
    public static final String OLM_SOURCE_NAME = getOrDefault(OLM_SOURCE_NAME_ENV, OLM_SOURCE_NAME_DEFAULT);
    public static final String OLM_SOURCE_NAMESPACE = getOrDefault(OLM_SOURCE_NAMESPACE_ENV, OpenShift.OLM_SOURCE_NAMESPACE);
    public static final String OLM_APP_BUNDLE_PREFIX = getOrDefault(OLM_APP_BUNDLE_PREFIX_ENV, OLM_APP_BUNDLE_PREFIX_DEFAULT);
    public static final String OLM_OPERATOR_LATEST_RELEASE_VERSION = getOrDefault(OLM_OPERATOR_VERSION_ENV, OLM_OPERATOR_VERSION_DEFAULT);
    // NetworkPolicy variable
    public static final boolean DEFAULT_TO_DENY_NETWORK_POLICIES = getOrDefault(DEFAULT_TO_DENY_NETWORK_POLICIES_ENV, Boolean::parseBoolean, DEFAULT_TO_DENY_NETWORK_POLICIES_DEFAULT);
    // ClusterOperator installation type variable
    public static final ClusterOperatorInstallType CLUSTER_OPERATOR_INSTALL_TYPE = getOrDefault(CLUSTER_OPERATOR_INSTALL_TYPE_ENV, value -> ClusterOperatorInstallType.valueOf(value.toUpperCase(Locale.ENGLISH)), CLUSTER_OPERATOR_INSTALL_TYPE_DEFAULT);
    public static final boolean LB_FINALIZERS = getOrDefault(LB_FINALIZERS_ENV, Boolean::parseBoolean, LB_FINALIZERS_DEFAULT);

    private Environment() { }

    static {
        String debugFormat = "{}: {}";
        LOGGER.info("Used environment variables:");
        LOGGER.info(debugFormat, "CONFIG", config);
        VALUES.forEach((key, value) -> LOGGER.info(debugFormat, key, value));
    }

    public static boolean isOlmInstall() {
        return CLUSTER_OPERATOR_INSTALL_TYPE.equals(ClusterOperatorInstallType.OLM);
    }

    public static boolean isHelmInstall() {
        return CLUSTER_OPERATOR_INSTALL_TYPE.equals(ClusterOperatorInstallType.HELM);
    }

    public static boolean isNamespaceRbacScope() {
        return STRIMZI_RBAC_SCOPE_NAMESPACE.equals(STRIMZI_RBAC_SCOPE);
    }

    public static boolean useLatestReleasedBridge() {
        return Environment.BRIDGE_IMAGE.equals(Environment.BRIDGE_IMAGE_DEFAULT);
    }

    private static String getOrDefault(String varName, String defaultValue) {
        return getOrDefault(varName, String::toString, defaultValue);
    }

    private static <T> T getOrDefault(String var, Function<String, T> converter, T defaultValue) {
        String value = System.getenv(var) != null ?
                System.getenv(var) :
                (Objects.requireNonNull(JSON_DATA).get(var) != null ?
                        JSON_DATA.get(var).asText() :
                        null);
        T returnValue = defaultValue;
        if (value != null) {
            returnValue = converter.apply(value);
        }
        VALUES.put(var, String.valueOf(returnValue));
        return returnValue;
    }

    private static JsonNode loadConfigurationFile() {
        config = System.getenv().getOrDefault(CONFIG_FILE_PATH_ENVAR,
                Paths.get(System.getProperty("user.dir"), "config.json").toAbsolutePath().toString());
        ObjectMapper mapper = new ObjectMapper();
        try {
            File jsonFile = new File(config).getAbsoluteFile();
            return mapper.readTree(jsonFile);
        } catch (IOException ex) {
            LOGGER.info("Json configuration is not provided or cannot be processed");
            return mapper.createObjectNode();
        }
    }
}
