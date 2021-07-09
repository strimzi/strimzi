/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.specific;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.enums.OlmInstallationStrategy;
import io.strimzi.systemtest.resources.ResourceItem;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.OlmUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.k8s.KubeClusterResource;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.resources.ResourceManager.CR_CREATION_TIMEOUT;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class OlmResource implements SpecificResourceType {
    private static final Logger LOGGER = LogManager.getLogger(OlmResource.class);

    public static final String NO_MORE_NON_USED_INSTALL_PLANS = "NoMoreNonUsedInstallPlans";

    // only three versions
    private static final Map<String, Boolean> CLOSED_MAP_INSTALL_PLAN = new HashMap<>(3);

    private static Map<String, JsonObject> exampleResources = new HashMap<>();

    private String deploymentName;
    private String namespace;
    private String csvName;

    @Override
    public void create(ExtensionContext extensionContext) {
        this.create(extensionContext, Constants.CO_OPERATION_TIMEOUT_DEFAULT, Constants.RECONCILIATION_INTERVAL);
    }

    public void create(ExtensionContext extensionContext, long operationTimeout, long reconciliationInterval) {
        ResourceManager.STORED_RESOURCES.computeIfAbsent(extensionContext.getDisplayName(), k -> new Stack<>());
        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(new ResourceItem(this::delete));
        this.clusterOperator(this.namespace, operationTimeout, reconciliationInterval);
    }

    public void create(OlmInstallationStrategy olmInstallationStrategy, String fromVersion) {
        this.clusterOperator(this.namespace, olmInstallationStrategy, fromVersion);
    }

    @Override
    public void delete() {
        deleteOlm(deploymentName, namespace, csvName);
    }

    public OlmResource(String namespace) {
        this.namespace = namespace;
    }

    private void clusterOperator(String namespace, OlmInstallationStrategy olmInstallationStrategy, String fromVersion) {
        clusterOperator(namespace, Constants.CO_OPERATION_TIMEOUT_DEFAULT, Constants.RECONCILIATION_INTERVAL,
            olmInstallationStrategy, fromVersion);
    }

    private void clusterOperator(String namespace, long operationTimeout, long reconciliationInterval) {
        clusterOperator(namespace, operationTimeout, reconciliationInterval, OlmInstallationStrategy.Automatic, null);
    }

    private void clusterOperator(String namespace, long operationTimeout, long reconciliationInterval,
                                             OlmInstallationStrategy olmInstallationStrategy, String fromVersion) {

        // if on cluster is not defaultOlmNamespace apply 'operator group' in current namespace
        if (!KubeClusterResource.getInstance().getDefaultOlmNamespace().equals(namespace)) {
            createOperatorGroup(namespace);
        }

        if (fromVersion != null) {
            createAndModifySubscription(namespace, operationTimeout, reconciliationInterval, olmInstallationStrategy, fromVersion);
            // must be strimzi-cluster-operator.v0.18.0
            csvName = Environment.OLM_APP_BUNDLE_PREFIX + ".v" + fromVersion;
        } else {
            createAndModifySubscriptionLatestRelease(namespace, operationTimeout, reconciliationInterval, olmInstallationStrategy);
            csvName = Environment.OLM_APP_BUNDLE_PREFIX + ".v" + Environment.OLM_OPERATOR_LATEST_RELEASE_VERSION;
        }

        // manual installation needs approval with patch
        if (olmInstallationStrategy == OlmInstallationStrategy.Manual) {
            OlmUtils.waitUntilNonUsedInstallPlanIsPresent(fromVersion);
            obtainInstallPlanName();
            approveNonUsedInstallPlan();
        }

        // Make sure that operator will be deleted
        TestUtils.waitFor("Cluster Operator deployment creation", Constants.GLOBAL_POLL_INTERVAL, CR_CREATION_TIMEOUT,
            () -> kubeClient(namespace).getDeploymentNameByPrefix(Environment.OLM_OPERATOR_DEPLOYMENT_NAME) != null);

        deploymentName = kubeClient(namespace).getDeploymentNameByPrefix(Environment.OLM_OPERATOR_DEPLOYMENT_NAME);
        ResourceManager.setCoDeploymentName(deploymentName);

        // Wait for operator creation
        waitFor(deploymentName, namespace, 1);

        exampleResources = parseExamplesFromCsv(csvName, namespace);
    }

    /**
     * Get install plan name and store it to closedMapInstallPlan
     */
    public static void obtainInstallPlanName() {
        String installPlansPureString = cmdKubeClient().execInCurrentNamespace("get", "installplan").out();
        String[] installPlansLines = installPlansPureString.split("\n");

        for (String line : installPlansLines) {
            // line NAME  CSV  APPROVAL   APPROVED
            String[] wholeLine = line.split(" ");

            // name
            if (wholeLine[0].startsWith("install-")) {

                // if is not already applied add to closed map
                if (!CLOSED_MAP_INSTALL_PLAN.containsKey(wholeLine[0])) {
                    LOGGER.info("CLOSED_MAP_INSTALL_PLAN does not contain {} install plan so this is not used and will " +
                        "be in the following upgrade.", wholeLine[0]);
                    CLOSED_MAP_INSTALL_PLAN.put(wholeLine[0], Boolean.FALSE);
                }
            }
        }
        if (!(CLOSED_MAP_INSTALL_PLAN.keySet().size() > 0)) {
            throw new RuntimeException("No install plans located in namespace:" + cmdKubeClient().namespace());
        }
    }

    /**
     * Get specific version of cluster operator with prefix name in format: 'strimzi-cluster-operator.v0.18.0'
     * @return version with prefix name
     */
    public static String getClusterOperatorVersion() {
        String installPlansPureString = cmdKubeClient().execInCurrentNamespace("get", "installplan").out();
        String[] installPlansLines = installPlansPureString.split("\n");

        for (String line : installPlansLines) {
            // line = NAME   CSV   APPROVAL   APPROVED
            String[] wholeLine = line.split("   ");

            // non-used install plan
            if (wholeLine[0].equals(getNonUsedInstallPlan())) {
                return wholeLine[1];
            }
        }
        throw new RuntimeException("Version was not found in the install plan.");
    }

    public static String getNonUsedInstallPlan() {
        String[] nonUsedInstallPlan = new String[1];

        for (Map.Entry<String, Boolean> entry : CLOSED_MAP_INSTALL_PLAN.entrySet()) {
            // if value is FALSE we are gonna use it = non-used install plan
            if (!entry.getValue()) {
                nonUsedInstallPlan[0] = entry.getKey();
                break;
            }
            nonUsedInstallPlan[0] = NO_MORE_NON_USED_INSTALL_PLANS;
        }

        LOGGER.info("Non-used install plan is {}", nonUsedInstallPlan[0]);
        return nonUsedInstallPlan[0];
    }

    /**
     * Patches specific non used install plan, which will approve installation. Only for manual installation strategy.
     * Also updates closedMapInstallPlan map and set specific install plan to true.
     */
    private static void approveNonUsedInstallPlan() {
        String nonUsedInstallPlan = getNonUsedInstallPlan();

        try {
            LOGGER.info("Approving {} install plan", nonUsedInstallPlan);
            String dynamicScriptContent =
                "#!/bin/bash\n" +
                    cmdKubeClient().cmd() +
                    " patch installplan " + nonUsedInstallPlan + " --type json  --patch '[{\"op\": \"add\", \"path\": \"/spec/approved\", \"value\": true}]' -n " + KubeClusterResource.getInstance().getNamespace();

            InputStream inputStream = new ByteArrayInputStream(dynamicScriptContent.getBytes(Charset.defaultCharset()));
            File patchScript = File.createTempFile("installplan_patch",  ".sh");
            Files.copy(inputStream, patchScript.toPath(), StandardCopyOption.REPLACE_EXISTING);

            Exec.exec("bash", patchScript.getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Upgrade cluster operator by obtaining new install plan, which was not used and also approves installation by
     * changing the install plan YAML
     */
    public static void upgradeClusterOperator() {
        if (kubeClient().listPodsByPrefixInName(ResourceManager.getCoDeploymentName()).size() == 0) {
            throw new RuntimeException("We can not perform upgrade! Cluster operator pod is not present.");
        }

        obtainInstallPlanName();
        approveNonUsedInstallPlan();
    }

    /**
     * Creates OperatorGroup from `olm/operator-group.yaml` and modify "${OPERATOR_NAMESPACE}" attribute in YAML
     * @param namespace namespace where you want to apply OperatorGroup  kind
     */
    private static void createOperatorGroup(String namespace) {
        try {
            File operatorGroupFile = File.createTempFile("operatorgroup", ".yaml");
            InputStream groupInputStream = OlmResource.class.getClassLoader().getResourceAsStream("olm/operator-group.yaml");
            String operatorGroup = TestUtils.readResource(groupInputStream);
            TestUtils.writeFile(operatorGroupFile.getAbsolutePath(), operatorGroup.replace("${OPERATOR_NAMESPACE}", namespace));
            ResourceManager.cmdKubeClient().apply(operatorGroupFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates Subscription from "olm/subscription.yaml" and modify "${OPERATOR_NAMESPACE}", "${OLM_OPERATOR_NAME}...
     * attributes.
     * @param namespace namespace where you want to apply Subscription kind
     * @param reconciliationInterval reconciliation interval of cluster operator
     * @param operationTimeout operation timeout  of cluster operator
     * @param installationStrategy type of installation
     */
    private static void createAndModifySubscription(String namespace, long reconciliationInterval, long operationTimeout,
                                                    OlmInstallationStrategy installationStrategy, String version) {
        try {
            File subscriptionFile = File.createTempFile("subscription", ".yaml");
            InputStream subscriptionInputStream = OlmResource.class.getClassLoader().getResourceAsStream("olm/subscription.yaml");
            String subscription = TestUtils.readResource(subscriptionInputStream);
            TestUtils.writeFile(subscriptionFile.getAbsolutePath(),
                subscription.replace("${OPERATOR_NAMESPACE}", namespace)
                    .replace("${OLM_OPERATOR_NAME}", Environment.OLM_OPERATOR_NAME)
                    .replace("${OLM_SOURCE_NAME}", Environment.OLM_SOURCE_NAME)
                    .replace("${OLM_SOURCE_NAMESPACE}", Environment.OLM_SOURCE_NAMESPACE)
                    .replace("${OLM_APP_BUNDLE_PREFIX}", Environment.OLM_APP_BUNDLE_PREFIX)
                    .replace("${OLM_OPERATOR_VERSION}", version)
                    .replace("${OLM_INSTALL_PLAN_APPROVAL}", installationStrategy.toString())
                    .replace("${STRIMZI_FULL_RECONCILIATION_INTERVAL_MS}", Long.toString(reconciliationInterval))
                    .replace("${STRIMZI_OPERATION_TIMEOUT_MS}", Long.toString(operationTimeout))
                    .replace("${STRIMZI_RBAC_SCOPE}", Environment.STRIMZI_RBAC_SCOPE)
                    .replace("${STRIMZI_FEATURE_GATES}", Environment.STRIMZI_FEATURE_GATES));

            cmdKubeClient(namespace).apply(subscriptionFile);
        }  catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void createAndModifySubscriptionLatestRelease(String namespace, long reconciliationInterval,
                                                                 long operationTimeout, OlmInstallationStrategy installationStrategy) {
        createAndModifySubscription(namespace, reconciliationInterval, operationTimeout, installationStrategy,
            Environment.OLM_OPERATOR_LATEST_RELEASE_VERSION);
    }

    private static void deleteOlm(String deploymentName, String namespace, String csvName) {
        ResourceManager.cmdKubeClient().exec("delete", "subscriptions", "-l", "app=strimzi", "-n", namespace);
        ResourceManager.cmdKubeClient().exec("delete", "operatorgroups", "-l", "app=strimzi", "-n", namespace);
        ResourceManager.cmdKubeClient().exec(false, "delete", "csv", csvName, "-n", namespace);
        DeploymentUtils.waitForDeploymentDeletion(deploymentName);
    }

    private static void waitFor(String deploymentName, String namespace, int replicas) {
        LOGGER.info("Waiting for deployment {} in namespace {}", deploymentName, namespace);
        DeploymentUtils.waitForDeploymentAndPodsReady(namespace, deploymentName, replicas);
        LOGGER.info("Deployment {} in namespace {} is ready", deploymentName, namespace);
    }

    private static Map<String, JsonObject> parseExamplesFromCsv(String csvName, String namespace) {
        String csvString = ResourceManager.cmdKubeClient().exec(true, false, "get", "csv", csvName, "-o", "json", "-n", namespace).out();
        JsonObject csv = new JsonObject(csvString);
        String almExamples = csv.getJsonObject("metadata").getJsonObject("annotations").getString("alm-examples");
        JsonArray examples = new JsonArray(almExamples);
        return examples.stream().map(o -> (JsonObject) o).collect(Collectors.toMap(object -> object.getString("kind"), object -> object));
    }

    public static Map<String, JsonObject> getExampleResources() {
        return exampleResources;
    }

    public static void setExampleResources(Map<String, JsonObject> exampleResources) {
        OlmResource.exampleResources = exampleResources;
    }

    public static Map<String, Boolean> getClosedMapInstallPlan() {
        return CLOSED_MAP_INSTALL_PLAN;
    }
}
