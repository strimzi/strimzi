/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.SetupClusterOperator;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.UPGRADE;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * This test class contains tests for Strimzi upgrade from version X to version X + 1.
 * Metadata for upgrade procedure are available in resource file StrimziUpgrade.json
 * Kafka upgrade is done as part of those tests as well, but the tests for Kafka upgrade/downgrade are in {@link KafkaUpgradeDowngradeST}.
 */
@Tag(UPGRADE)
public class StrimziUpgradeST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(StrimziUpgradeST.class);

    public static final String NAMESPACE = "strimzi-upgrade-test";

    // TODO: make testUpgradeKafkaWithoutVersion to run upgrade with config from StrimziUpgradeST.json
    // main idea of the test and usage of latestReleasedVersion: upgrade CO from version X, kafka Y, to CO version Z and kafka Y + 1 at the end
    private final String strimziReleaseWithOlderKafkaVersion = "0.22.1";
    private final String strimziReleaseWithOlderKafka = String.format("https://github.com/strimzi/strimzi-kafka-operator/releases/download/%s/strimzi-%s.zip",
            strimziReleaseWithOlderKafkaVersion, strimziReleaseWithOlderKafkaVersion);

    @ParameterizedTest(name = "testUpgradeStrimziVersion->{0}->{1}")
    @MethodSource("loadJsonUpgradeData")
    @Tag(INTERNAL_CLIENTS_USED)
    void testUpgradeStrimziVersion(String fromVersion, String toVersion, JsonObject parameters, ExtensionContext extensionContext) throws Exception {
        assumeTrue(StUtils.isAllowOnCurrentEnvironment(parameters.getJsonObject("environmentInfo").getString("flakyEnvVariable")));
        assumeTrue(StUtils.isAllowedOnCurrentK8sVersion(parameters.getJsonObject("environmentInfo").getString("maxK8sVersion")));

        LOGGER.debug("Running upgrade test from version {} to {}", fromVersion, toVersion);
        performUpgrade(parameters, extensionContext);
    }

    @Test
    void testUpgradeKafkaWithoutVersion(ExtensionContext extensionContext) throws IOException {
        JsonObject conversionTool = getConversionToolDataFromUpgradeJSON();

        File dir = FileUtils.downloadAndUnzip(strimziReleaseWithOlderKafka);
        File startKafkaPersistent = new File(dir, "strimzi-" + strimziReleaseWithOlderKafkaVersion + "/examples/kafka/kafka-persistent.yaml");
        File startKafkaVersionsYaml = FileUtils.downloadYaml("https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/" + strimziReleaseWithOlderKafkaVersion + "/kafka-versions.yaml");
        File latestKafkaVersionsYaml = new File(TestUtils.USER_PATH + "//../kafka-versions.yaml");

        coDir = new File(dir, "strimzi-" + strimziReleaseWithOlderKafkaVersion + "/install/cluster-operator/");

        String latestKafkaVersion = getValueForLastKafkaVersionInFile(latestKafkaVersionsYaml, "version");
        String startKafkaVersion = getValueForLastKafkaVersionInFile(startKafkaVersionsYaml, "version");
        String startLogMessageFormat = getValueForLastKafkaVersionInFile(startKafkaVersionsYaml, "format");
        String startInterBrokerProtocol = getValueForLastKafkaVersionInFile(startKafkaVersionsYaml, "protocol");

        // Modify + apply installation files
        copyModifyApply(coDir, NAMESPACE);
        convertCRDs(conversionTool, NAMESPACE);
        // Apply Kafka Persistent without version
        LOGGER.info("Going to deploy Kafka from: {}", startKafkaPersistent.getPath());
        // Change kafka version of it's empty (null is for remove the version)
        cmdKubeClient().applyContent(KafkaUtils.changeOrRemoveKafkaConfiguration(startKafkaPersistent, null, startLogMessageFormat, startInterBrokerProtocol));
        // Wait for readiness
        waitForReadinessOfKafkaCluster();

        assertThat(KafkaUtils.getVersionFromKafkaPodLibs(KafkaResources.kafkaPodName(clusterName, 0)), containsString(startKafkaVersion));

        Map<String, String> operatorSnapshot = DeploymentUtils.depSnapshot(ResourceManager.getCoDeploymentName());
        Map<String, String> zooSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(clusterName));
        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(clusterName));
        Map<String, String> eoSnapshot = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(clusterName));

        // Update CRDs, CRB, etc.
        install.applyClusterOperatorInstallFiles(NAMESPACE);
        SetupClusterOperator.applyBindings(extensionContext, NAMESPACE);

        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).delete();
        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).create(
            new BundleResource.BundleResourceBuilder()
                .withName(Constants.STRIMZI_DEPLOYMENT_NAME)
                .withNamespace(NAMESPACE)
                .buildBundleInstance()
                .buildBundleDeployment()
                .build());

        DeploymentUtils.waitTillDepHasRolled(ResourceManager.getCoDeploymentName(), 1, operatorSnapshot);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(clusterName), 3, zooSnapshot);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoSnapshot);

        assertThat(KafkaUtils.getVersionFromKafkaPodLibs(KafkaResources.kafkaPodName(clusterName, 0)), containsString(latestKafkaVersion));
    }

    @Test
    void testUpgradeAcrossVersionsWithUnsupportedKafkaVersion(ExtensionContext extensionContext) throws IOException {
        JsonObject acrossUpgradeData = buildDataForUpgradeAcrossVersions();
        JsonObject conversionTool = getConversionToolDataFromUpgradeJSON();

        String continuousTopicName = "continuous-topic";
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";
        String continuousConsumerGroup = "continuous-consumer-group";

        // Setup env
        setupEnvAndUpgradeClusterOperator(extensionContext, acrossUpgradeData, producerName, consumerName, continuousTopicName, continuousConsumerGroup, acrossUpgradeData.getString("startingKafkaVersion"), NAMESPACE);
        convertCRDs(conversionTool, NAMESPACE);
        // Make snapshots of all pods
        makeSnapshots(clusterName);

        // Upgrade CO
        changeClusterOperator(acrossUpgradeData, NAMESPACE);
        logPodImages(clusterName);
        //  Upgrade kafka
        changeKafkaAndLogFormatVersion(acrossUpgradeData.getJsonObject("proceduresAfterOperatorUpgrade"), acrossUpgradeData, clusterName, extensionContext);
        logPodImages(clusterName);
        checkAllImages(acrossUpgradeData.getJsonObject("imagesAfterKafkaUpgrade"));
        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(clusterName);
        // Verify upgrade
        verifyProcedure(acrossUpgradeData, producerName, consumerName, NAMESPACE);
        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }

    @Test
    void testUpgradeAcrossVersionsWithNoKafkaVersion(ExtensionContext extensionContext) throws IOException {
        JsonObject acrossUpgradeData = buildDataForUpgradeAcrossVersions();
        JsonObject conversionTool = getConversionToolDataFromUpgradeJSON();

        String continuousTopicName = "continuous-topic";
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";
        String continuousConsumerGroup = "continuous-consumer-group";

        // Setup env
        setupEnvAndUpgradeClusterOperator(extensionContext, acrossUpgradeData, producerName, consumerName, continuousTopicName, continuousConsumerGroup, null, NAMESPACE);
        convertCRDs(conversionTool, NAMESPACE);

        // Upgrade CO
        changeClusterOperator(acrossUpgradeData, NAMESPACE);
        // Wait till first upgrade finished
        zkPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(clusterName), 3, zkPods);
        kafkaPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaPods);
        eoPods = DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);

        LOGGER.info("Rolling to new images has finished!");
        logPodImages(clusterName);
        //  Upgrade kafka
        changeKafkaAndLogFormatVersion(acrossUpgradeData.getJsonObject("proceduresAfterOperatorUpgrade"), acrossUpgradeData, clusterName, extensionContext);
        logPodImages(clusterName);
        checkAllImages(acrossUpgradeData.getJsonObject("imagesAfterKafkaUpgrade"));
        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(clusterName);
        // Verify upgrade
        verifyProcedure(acrossUpgradeData, producerName, consumerName, NAMESPACE);

        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }

    private JsonObject buildDataForUpgradeAcrossVersions() throws IOException {
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getSupportedKafkaVersions();
        TestKafkaVersion latestKafkaSupported = sortedVersions.get(sortedVersions.size() - 1);

        JsonArray upgradeJson = readUpgradeJson(UPGRADE_JSON_FILE);

        JsonObject acrossUpgradeData = upgradeJson.getJsonObject(upgradeJson.size() - 1);
        JsonObject startingVersion = getDataForStartUpgrade(upgradeJson);

        acrossUpgradeData.put("fromVersion", startingVersion.getValue("fromVersion"));
        acrossUpgradeData.put("fromExamples", startingVersion.getValue("fromExamples"));
        acrossUpgradeData.put("urlFrom", startingVersion.getValue("urlFrom"));

        acrossUpgradeData.put("urlTo", "HEAD");
        acrossUpgradeData.put("toVersion", "HEAD");
        acrossUpgradeData.put("toExamples", "HEAD");

        acrossUpgradeData.put("startingKafkaVersion", startingVersion.getString("oldestKafka"));
        acrossUpgradeData.put("defaultKafka", startingVersion.getString("defaultKafka"));
        acrossUpgradeData.put("oldestKafka", startingVersion.getString("oldestKafka"));

        // Generate procedures for upgrade
        JsonObject procedures = new JsonObject();
        procedures.put("kafkaVersion", latestKafkaSupported.version());
        procedures.put("logMessageVersion", latestKafkaSupported.messageVersion());
        procedures.put("interBrokerProtocolVersion", latestKafkaSupported.protocolVersion());
        acrossUpgradeData.put("proceduresAfterOperatorUpgrade", procedures);

        LOGGER.info("Upgrade Json for the test: {}", acrossUpgradeData.encodePrettily());

        return acrossUpgradeData;
    }

    private JsonObject getDataForStartUpgrade(JsonArray upgradeJson) throws IOException {
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getSupportedKafkaVersions();
        Collections.reverse(upgradeJson.getList());

        JsonObject startingVersion = null;

        for (Object item : upgradeJson) {
            TestKafkaVersion defaultVersion = getDefaultKafkaVersionPerStrimzi(((JsonObject) item).getValue("fromVersion").toString());

            ((JsonObject) item).put("defaultKafka", defaultVersion.version());

            startingVersion = (JsonObject) item;
            break;
        }
        return startingVersion;
    }

    String getValueForLastKafkaVersionInFile(File kafkaVersions, String field) throws IOException {
        YAMLMapper mapper = new YAMLMapper();
        JsonNode node = mapper.readTree(kafkaVersions);
        ObjectNode kafkaVersionNode = (ObjectNode) node.get(node.size() - 1);

        return kafkaVersionNode.get(field).asText();
    }

    private void performUpgrade(JsonObject testParameters, ExtensionContext extensionContext) throws IOException {
        String continuousTopicName = "continuous-topic";
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";
        String continuousConsumerGroup = "continuous-consumer-group";

        // Setup env
        setupEnvAndUpgradeClusterOperator(extensionContext, testParameters, producerName, consumerName, continuousTopicName, continuousConsumerGroup, "", NAMESPACE);

        logPodImages(clusterName);

        // Upgrade CRDs and upgrade CO to 0.24
        if (testParameters.getBoolean("convertCRDs")) {
            convertCRDs(testParameters.getJsonObject("conversionTool"), NAMESPACE);
        }

        // Upgrade CO to HEAD
        logPodImages(clusterName);
        changeClusterOperator(testParameters, NAMESPACE);

        if (TestKafkaVersion.containsVersion(getDefaultKafkaVersionPerStrimzi(testParameters.getString("fromVersion")).version())) {
            waitForKafkaClusterRollingUpdate();
        }

        logPodImages(clusterName);
        // Upgrade kafka
        changeKafkaAndLogFormatVersion(testParameters.getJsonObject("proceduresAfterOperatorUpgrade"), testParameters, clusterName, extensionContext);
        logPodImages(clusterName);
        checkAllImages(testParameters.getJsonObject("imagesAfterKafkaUpgrade"));

        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(clusterName);
        // Verify upgrade
        verifyProcedure(testParameters, producerName, consumerName, NAMESPACE);

        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }

    private TestKafkaVersion getDefaultKafkaVersionPerStrimzi(String strimziVersion) throws IOException {
        List<TestKafkaVersion> testKafkaVersions = TestKafkaVersion.parseKafkaVersionsFromUrl("https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/" + strimziVersion + "/kafka-versions.yaml");
        return testKafkaVersions.stream().filter(TestKafkaVersion::isDefault).collect(Collectors.toList()).get(0);
    }

    @BeforeEach
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
    }

    @AfterEach
    protected void tearDownEnvironmentAfterEach() {
        deleteInstalledYamls(coDir, NAMESPACE);
        cluster.deleteNamespaces();
    }

    // There is no value of having teardown logic for class resources due to the fact that
    // CO was deployed by method StrimziUpgradeST.copyModifyApply() and removed by method StrimziUpgradeST.deleteInstalledYamls()
    @AfterAll
    protected void tearDownEnvironmentAfterAll() { }
}
