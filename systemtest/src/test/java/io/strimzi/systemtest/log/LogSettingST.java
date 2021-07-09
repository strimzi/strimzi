/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.log;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.JvmOptions;
import io.strimzi.api.kafka.model.JvmOptionsBuilder;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.SetupClusterOperator;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CC_LOG_CONFIG_RELOAD;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.CO_OPERATION_TIMEOUT_MEDIUM;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.TIMEOUT_FOR_LOG;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(CONNECT)
@Tag(MIRROR_MAKER)
@Tag(MIRROR_MAKER2)
@Tag(BRIDGE)
@Tag(CONNECT_COMPONENTS)
@TestMethodOrder(OrderAnnotation.class)
class LogSettingST extends AbstractST {
    static final String NAMESPACE = "log-setting-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(LogSettingST.class);

    private static final String INFO = "INFO";
    private static final String ERROR = "ERROR";
    private static final String WARN = "WARN";
    private static final String TRACE = "TRACE";
    private static final String DEBUG = "DEBUG";
    private static final String FATAL = "FATAL";
    private static final String OFF = "OFF";

    private static final String LOG_SETTING_CLUSTER_NAME = "log-setting-cluster-name";
    private static final String GC_LOGGING_SET_NAME = "gc-set-logging";

    private static final JvmOptions JVM_OPTIONS = new JvmOptionsBuilder()
        .withGcLoggingEnabled(false)
        .build();

    private static final Map<String, String> KAFKA_LOGGERS = new HashMap<String, String>() {
        {
            put("kafka.root.logger.level", INFO);
            put("test.kafka.logger.level", INFO);
            put("log4j.logger.org.I0Itec.zkclient.ZkClient", ERROR);
            put("log4j.logger.org.apache.zookeeper", WARN);
            put("log4j.logger.kafka", TRACE);
            put("log4j.logger.org.apache.kafka", DEBUG);
            put("log4j.logger.kafka.request.logger", FATAL);
            put("log4j.logger.kafka.network.Processor", OFF);

            put("log4j.logger.kafka.server.KafkaApis", INFO);
            put("log4j.logger.kafka.network.RequestChannel$", ERROR);
            put("log4j.logger.kafka.controller", WARN);
            put("log4j.logger.kafka.log.LogCleaner", TRACE);
            put("log4j.logger.state.change.logger", DEBUG);
            put("log4j.logger.kafka.authorizer.logger", FATAL);
        }
    };

    private static final Map<String, String> ZOOKEEPER_LOGGERS = new HashMap<String, String>() {
        {
            put("zookeeper.root.logger", OFF);
            put("test.zookeeper.logger.level", DEBUG);
        }
    };

    private static final Map<String, String> CONNECT_LOGGERS = new HashMap<String, String>() {
        {
            put("connect.root.logger.level", INFO);
            put("test.connect.logger.level", DEBUG);
            put("log4j.logger.org.I0Itec.zkclient", ERROR);
            put("log4j.logger.org.reflections", WARN);
        }
    };

    private static final Map<String, String> OPERATORS_LOGGERS = new HashMap<String, String>() {
        {
            put("rootLogger.level", DEBUG);
            put("test.operator.logger.level", DEBUG);
        }
    };

    private static final Map<String, String> MIRROR_MAKER_LOGGERS = new HashMap<String, String>() {
        {
            put("mirrormaker.root.logger", TRACE);
            put("test.mirrormaker.logger.level", TRACE);
        }
    };

    private static final Map<String, String> BRIDGE_LOGGERS = new HashMap<String, String>() {
        {
            put("logger.createConsumer.name", "http.openapi.operation.createConsumer");
            put("logger.createConsumer.level", INFO);
            put("logger.deleteConsumer.name", "http.openapi.operation.deleteConsumer");
            put("logger.deleteConsumer.level", DEBUG);
            put("logger.subscribe.name", "http.openapi.operation.subscribe");
            put("logger.subscribe.level", TRACE);
            put("logger.unsubscribe.name", "http.openapi.operation.unsubscribe");
            put("logger.unsubscribe.level", DEBUG);
            put("logger.poll.name", "http.openapi.operation.poll");
            put("logger.poll.level", INFO);
            put("logger.assign.name", "http.openapi.operation.assign");
            put("logger.assign.level", TRACE);
            put("logger.commit.name", "http.openapi.operation.commit");
            put("logger.commit.level", DEBUG);
            put("logger.send.name", "http.openapi.operation.send");
            put("logger.send.level", ERROR);
            put("logger.sendToPartition.name", "http.openapi.operation.sendToPartition");
            put("logger.sendToPartition.level", TRACE);
            put("logger.seekToBeginning.name", "http.openapi.operation.seekToBeginning");
            put("logger.seekToBeginning.level", DEBUG);
            put("logger.seekToEnd.name", "http.openapi.operation.seekToEnd");
            put("logger.seekToEnd.level", WARN);
            put("logger.seek.name", "http.openapi.operation.seek");
            put("logger.seek.level", INFO);
            put("logger.healthy.name", "http.openapi.operation.healthy");
            put("logger.healthy.level", ERROR);
            put("logger.ready.name", "http.openapi.operation.ready");
            put("logger.ready.level", WARN);
            put("logger.openapi.name", "http.openapi.operation.openapi");
            put("logger.openapi.level", TRACE);
            put("test.logger.bridge.level", ERROR);
        }
    };

    @IsolatedTest("Using shared Kafka")
    void testKafkaLogSetting(ExtensionContext extensionContext) {
        String kafkaMap = KafkaResources.kafkaMetricsAndLogConfigMapName(LOG_SETTING_CLUSTER_NAME);
        String zookeeperMap = KafkaResources.zookeeperMetricsAndLogConfigMapName(LOG_SETTING_CLUSTER_NAME);
        String topicOperatorMap = String.format("%s-%s", LOG_SETTING_CLUSTER_NAME, "entity-topic-operator-config");
        String userOperatorMap = String.format("%s-%s", LOG_SETTING_CLUSTER_NAME, "entity-user-operator-config");

        String eoDepName = KafkaResources.entityOperatorDeploymentName(LOG_SETTING_CLUSTER_NAME);
        String kafkaSsName = KafkaResources.kafkaStatefulSetName(LOG_SETTING_CLUSTER_NAME);
        String zkSsName = KafkaResources.zookeeperStatefulSetName(LOG_SETTING_CLUSTER_NAME);

        Map<String, String> eoPods = DeploymentUtils.depSnapshot(NAMESPACE, eoDepName);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(NAMESPACE, kafkaSsName);
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(NAMESPACE, zkSsName);

        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(LOG_SETTING_CLUSTER_NAME, topicName).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(LOG_SETTING_CLUSTER_NAME, userName).build());

        LOGGER.info("Checking if Kafka, Zookeeper, TO and UO of cluster:{} has log level set properly", LOG_SETTING_CLUSTER_NAME);
        assertThat("Kafka's log level is set properly", checkLoggersLevel(NAMESPACE, KAFKA_LOGGERS, kafkaMap), is(true));
        assertThat("Zookeeper's log level is set properly", checkLoggersLevel(NAMESPACE, ZOOKEEPER_LOGGERS, zookeeperMap), is(true));
        assertThat("Topic operator's log level is set properly", checkLoggersLevel(NAMESPACE, OPERATORS_LOGGERS, topicOperatorMap), is(true));
        assertThat("User operator's log level is set properly", checkLoggersLevel(NAMESPACE, OPERATORS_LOGGERS, userOperatorMap), is(true));

        LOGGER.info("Checking if Kafka, Zookeeper, TO and UO of cluster:{} has GC logging enabled in stateful sets/deployments", LOG_SETTING_CLUSTER_NAME);
        assertThat("Kafka GC logging is not enabled", checkGcLoggingStatefulSets(NAMESPACE, kafkaSsName), is(true));
        assertThat("Zookeeper GC logging is enabled", checkGcLoggingStatefulSets(NAMESPACE, zkSsName), is(true));
        assertThat("TO GC logging is enabled", checkGcLoggingDeployments(NAMESPACE, eoDepName, "topic-operator"), is(true));
        assertThat("UO GC logging is enabled", checkGcLoggingDeployments(NAMESPACE, eoDepName, "user-operator"), is(true));

        LOGGER.info("Changing JVM options - setting GC logging to false");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(LOG_SETTING_CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setJvmOptions(JVM_OPTIONS);
            kafka.getSpec().getZookeeper().setJvmOptions(JVM_OPTIONS);
            kafka.getSpec().getEntityOperator().getTopicOperator().setJvmOptions(JVM_OPTIONS);
            kafka.getSpec().getEntityOperator().getUserOperator().setJvmOptions(JVM_OPTIONS);
        }, NAMESPACE);

        StatefulSetUtils.waitTillSsHasRolled(NAMESPACE, zkSsName, 1, zkPods);
        StatefulSetUtils.waitTillSsHasRolled(NAMESPACE, kafkaSsName, 3, kafkaPods);
        DeploymentUtils.waitTillDepHasRolled(NAMESPACE, eoDepName, 1, eoPods);

        LOGGER.info("Checking if Kafka, Zookeeper, TO and UO of cluster:{} has GC logging disabled in stateful sets/deployments", LOG_SETTING_CLUSTER_NAME);
        assertThat("Kafka GC logging is disabled", checkGcLoggingStatefulSets(NAMESPACE, kafkaSsName), is(false));
        assertThat("Zookeeper GC logging is disabled", checkGcLoggingStatefulSets(NAMESPACE, zkSsName), is(false));
        assertThat("TO GC logging is disabled", checkGcLoggingDeployments(NAMESPACE, eoDepName, "topic-operator"), is(false));
        assertThat("UO GC logging is disabled", checkGcLoggingDeployments(NAMESPACE, eoDepName, "user-operator"), is(false));

        LOGGER.info("Checking if Kafka, Zookeeper, TO and UO of cluster:{} has GC logging disabled in stateful sets/deployments", GC_LOGGING_SET_NAME);
        assertThat("Kafka GC logging is enabled", checkGcLoggingStatefulSets(NAMESPACE, kafkaSsName), is(false));
        assertThat("Zookeeper GC logging is enabled", checkGcLoggingStatefulSets(NAMESPACE, zkSsName), is(false));
        assertThat("TO GC logging is enabled", checkGcLoggingDeployments(NAMESPACE, eoDepName, "topic-operator"), is(false));
        assertThat("UO GC logging is enabled", checkGcLoggingDeployments(NAMESPACE, eoDepName, "user-operator"), is(false));

        kubectlGetStrimziUntilOperationIsSuccessful(NAMESPACE, LOG_SETTING_CLUSTER_NAME);
        kubectlGetStrimziUntilOperationIsSuccessful(NAMESPACE, GC_LOGGING_SET_NAME);

        checkContainersHaveProcessOneAsTini(NAMESPACE, LOG_SETTING_CLUSTER_NAME);
        checkContainersHaveProcessOneAsTini(NAMESPACE, GC_LOGGING_SET_NAME);
    }

    @ParallelTest
    void testConnectLogSetting(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String connectClusterName = clusterName + "-connect";

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, connectClusterName, LOG_SETTING_CLUSTER_NAME, 1)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(CONNECT_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .build());

        String connectDepName = KafkaConnectResources.deploymentName(connectClusterName);
        Map<String, String> connectPods = DeploymentUtils.depSnapshot(NAMESPACE, connectDepName);
        String connectMap = KafkaConnectResources.metricsAndLogConfigMapName(connectClusterName);

        LOGGER.info("Checking if Connect has log level set properly");
        assertThat("KafkaConnect's log level is set properly", checkLoggersLevel(NAMESPACE, CONNECT_LOGGERS, connectMap), is(true));
        assertThat("Connect GC logging is enabled", checkGcLoggingDeployments(NAMESPACE, connectDepName), is(true));

        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(connectClusterName, kc -> kc.getSpec().setJvmOptions(JVM_OPTIONS), NAMESPACE);
        DeploymentUtils.waitTillDepHasRolled(NAMESPACE, connectDepName, 1, connectPods);
        assertThat("Connect GC logging is disabled", checkGcLoggingDeployments(NAMESPACE, connectDepName), is(false));

        kubectlGetStrimziUntilOperationIsSuccessful(NAMESPACE, connectClusterName);
        checkContainersHaveProcessOneAsTini(NAMESPACE, connectClusterName);
    }

    @ParallelTest
    void testMirrorMakerLogSetting(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String mirrorMakerName = clusterName + "-mirror-maker";

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(mirrorMakerName, LOG_SETTING_CLUSTER_NAME, GC_LOGGING_SET_NAME, "my-group", 1, false)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(MIRROR_MAKER_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .build());

        String mmDepName = KafkaMirrorMakerResources.deploymentName(mirrorMakerName);
        Map<String, String> mmPods = DeploymentUtils.depSnapshot(NAMESPACE, mmDepName);
        String mirrorMakerMap = KafkaMirrorMakerResources.metricsAndLogConfigMapName(mirrorMakerName);

        LOGGER.info("Checking if MirrorMaker has log level set properly");
        assertThat("KafkaMirrorMaker's log level is set properly", checkLoggersLevel(NAMESPACE, MIRROR_MAKER_LOGGERS, mirrorMakerMap), is(true));
        assertThat("Mirror-maker GC logging is enabled", checkGcLoggingDeployments(NAMESPACE, mmDepName), is(true));

        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(mirrorMakerName, mm -> mm.getSpec().setJvmOptions(JVM_OPTIONS), NAMESPACE);
        DeploymentUtils.waitTillDepHasRolled(NAMESPACE, mmDepName, 1, mmPods);
        assertThat("Mirror-maker GC logging is disabled", checkGcLoggingDeployments(NAMESPACE, mmDepName), is(false));

        kubectlGetStrimziUntilOperationIsSuccessful(NAMESPACE, mirrorMakerName);
        checkContainersHaveProcessOneAsTini(NAMESPACE, mirrorMakerName);
    }

    @ParallelTest
    void testMirrorMaker2LogSetting(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String mirrorMaker2Name = clusterName + "-mirror-maker-2";

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(mirrorMaker2Name, LOG_SETTING_CLUSTER_NAME, GC_LOGGING_SET_NAME, 1, false)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(MIRROR_MAKER_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .build());

        String mm2DepName = KafkaMirrorMaker2Resources.deploymentName(mirrorMaker2Name);
        Map<String, String> mm2Pods = DeploymentUtils.depSnapshot(NAMESPACE, mm2DepName);
        String mirrorMakerMap = KafkaMirrorMaker2Resources.metricsAndLogConfigMapName(mirrorMaker2Name);

        LOGGER.info("Checking if MirrorMaker2 has log level set properly");
        assertThat("KafkaMirrorMaker2's log level is set properly", checkLoggersLevel(NAMESPACE, MIRROR_MAKER_LOGGERS, mirrorMakerMap), is(true));
        assertThat("Mirror-maker-2 GC logging is enabled", checkGcLoggingDeployments(NAMESPACE, mm2DepName), is(true));

        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(mirrorMaker2Name, mm2 -> mm2.getSpec().setJvmOptions(JVM_OPTIONS), NAMESPACE);
        DeploymentUtils.waitTillDepHasRolled(NAMESPACE, mm2DepName, 1, mm2Pods);
        assertThat("Mirror-maker2 GC logging is disabled", checkGcLoggingDeployments(NAMESPACE, mm2DepName), is(false));

        kubectlGetStrimziUntilOperationIsSuccessful(NAMESPACE, mirrorMaker2Name);
        checkContainersHaveProcessOneAsTini(NAMESPACE, mirrorMaker2Name);
    }

    @ParallelTest
    void testBridgeLogSetting(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String bridgeName = clusterName + "-bridge";

        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(bridgeName, LOG_SETTING_CLUSTER_NAME, KafkaResources.plainBootstrapAddress(LOG_SETTING_CLUSTER_NAME), 1)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(BRIDGE_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .build());

        String bridgeDepName = KafkaBridgeResources.deploymentName(bridgeName);
        Map<String, String> bridgePods = DeploymentUtils.depSnapshot(NAMESPACE, bridgeDepName);
        String bridgeMap = KafkaBridgeResources.metricsAndLogConfigMapName(bridgeName);

        LOGGER.info("Checking if Bridge has log level set properly");
        assertThat("Bridge's log level is set properly", checkLoggersLevel(NAMESPACE, BRIDGE_LOGGERS, bridgeMap), is(true));
        assertThat("Bridge's GC logging is enabled", checkGcLoggingDeployments(NAMESPACE, bridgeDepName), is(true));

        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(bridgeName, bridge -> bridge.getSpec().setJvmOptions(JVM_OPTIONS), NAMESPACE);
        DeploymentUtils.waitTillDepHasRolled(NAMESPACE, bridgeDepName, 1, bridgePods);
        assertThat("Bridge GC logging is disabled", checkGcLoggingDeployments(NAMESPACE, bridgeDepName), is(false));

        kubectlGetStrimziUntilOperationIsSuccessful(NAMESPACE, bridgeName);
        checkContainersHaveProcessOneAsTiniWithoutPs(NAMESPACE, bridgeName);
    }

    @IsolatedTest("Updating shared Kafka")
    // This test might be flaky, as it gets real logs from CruiseControl pod
    void testCruiseControlLogChange(ExtensionContext extensionContext) {
        final String debugText = " DEBUG ";
        String cruiseControlPodName = PodUtils.getPodNameByPrefix(NAMESPACE, LOG_SETTING_CLUSTER_NAME + "-" + Constants.CRUISE_CONTROL_CONTAINER_NAME);
        LOGGER.info("Check that default/actual root logging level is info");
        String containerLogLevel = cmdKubeClient().namespace(NAMESPACE).execInPod(cruiseControlPodName, "grep", "-i", "rootlogger.level",
                Constants.CRUISE_CONTROL_LOG_FILE_PATH).out().trim().split("=")[1];
        assertThat(containerLogLevel.toUpperCase(Locale.ENGLISH), is(not(debugText.strip())));

        LOGGER.info("Check logs in CruiseControl - make sure no DEBUG is found there.");
        String logOut = StUtils.getLogFromPodByTime(NAMESPACE, cruiseControlPodName, Constants.CRUISE_CONTROL_CONTAINER_NAME, "20s");
        assertThat(logOut.toUpperCase(Locale.ENGLISH), not(containsString(debugText)));

        InlineLogging logging = new InlineLogging();
        logging.setLoggers(Collections.singletonMap("rootLogger.level", debugText.strip()));
        KafkaResource.replaceKafkaResourceInSpecificNamespace(LOG_SETTING_CLUSTER_NAME, kafka -> kafka.getSpec().getCruiseControl().setLogging(logging), NAMESPACE);

        LOGGER.info("Wait for change of root logger in {}.", cruiseControlPodName);
        TestUtils.waitFor("Waiting for log to be changed", CC_LOG_CONFIG_RELOAD, CO_OPERATION_TIMEOUT_MEDIUM, () -> {
            String line = StUtils.getLineFromPodContainer(NAMESPACE, cruiseControlPodName, null, Constants.CRUISE_CONTROL_LOG_FILE_PATH, "rootlogger.level");
            return line.toUpperCase(Locale.ENGLISH).contains(debugText.strip());
        });

        LOGGER.info("Check cruise control logs in pod {} and it's container {} .", cruiseControlPodName, Constants.CRUISE_CONTROL_CONTAINER_NAME);
        TestUtils.waitFor("Wait for debug log line to show in logs", CC_LOG_CONFIG_RELOAD, TIMEOUT_FOR_LOG, () -> {
            String log = StUtils.getLogFromPodByTime(NAMESPACE, cruiseControlPodName, Constants.CRUISE_CONTROL_CONTAINER_NAME, "20s");
            return log.toUpperCase(Locale.ENGLISH).contains(debugText);
        });
    }

    // only one thread can access (eliminate data-race)
    private synchronized void kubectlGetStrimziUntilOperationIsSuccessful(String namespaceName, String resourceName) {
        TestUtils.waitFor("Checking if kubectl get strimzi contains:" + resourceName, Duration.ofSeconds(10).toMillis(),
            Constants.GLOBAL_TIMEOUT, () -> cmdKubeClient().namespace(namespaceName).execInCurrentNamespace("get", "strimzi").out().contains(resourceName));
    }

    // only one thread can access (eliminate data-race)
    private synchronized void checkContainersHaveProcessOneAsTini(String namespaceName, String resourceClusterName) {
        //Used [/] in the grep command so that grep process does not return itself
        String command = "ps -ef | grep '[/]usr/bin/tini' | awk '{ print $2}'";

        for (Pod pod : kubeClient(namespaceName).listPods(Labels.STRIMZI_CLUSTER_LABEL, resourceClusterName)) {
            String podName = pod.getMetadata().getName();
            if (!podName.contains("build") && !podName.contains("deploy") && !podName.contains("kafka-clients")) {
                for (Container container : pod.getSpec().getContainers()) {
                    String containerName = container.getName();

                    PodUtils.waitForPodContainerReady(namespaceName, podName, containerName);
                    LOGGER.info("Checking tini process for pod {} with container {}", podName, containerName);
                    boolean isPresent = cmdKubeClient().namespace(namespaceName).execInPodContainer(false, podName, containerName, "/bin/bash", "-c", command).out().trim().equals("1");
                    assertThat(isPresent, is(true));
                }
            }
        }
    }

    // only one thread can access (eliminate data-race)
    private synchronized void checkContainersHaveProcessOneAsTiniWithoutPs(String namespaceName, String resourceClusterName) {
        //Used [/] in the grep command so that grep process does not return itself
        String command = "cat /proc/1/cmdline";

        for (Pod pod : kubeClient(namespaceName).listPods(Labels.STRIMZI_CLUSTER_LABEL, resourceClusterName)) {
            String podName = pod.getMetadata().getName();
            if (!podName.contains("build") && !podName.contains("deploy") && !podName.contains("kafka-clients")) {
                for (Container container : pod.getSpec().getContainers()) {
                    String containerName = container.getName();

                    PodUtils.waitForPodContainerReady(namespaceName, podName, containerName);
                    LOGGER.info("Checking tini process for pod {} with container {}", podName, containerName);
                    String processOne = cmdKubeClient().namespace(namespaceName).execInPodContainer(false, podName, containerName, "/bin/bash", "-c", command).out().trim();
                    assertThat(processOne, startsWith("/usr/bin/tini"));
                }
            }
        }
    }

    private synchronized String configMap(String namespaceName, String configMapName) {
        Map<String, String> configMapData = kubeClient(namespaceName).getConfigMap(configMapName).getData();
        // tries to get a log4j2 configuration file first (operator, bridge, ...) otherwise log4j one (kafka, zookeeper, ...)
        String configMapKey = configMapData.keySet()
                .stream()
                .filter(key -> key.equals("log4j2.properties") || key.equals("log4j.properties"))
                .findAny()
                .orElseThrow();
        return configMapData.get(configMapKey);
    }

    private synchronized boolean checkLoggersLevel(String namespaceName, Map<String, String> loggers, String configMapName) {
        boolean result = false;
        String configMap = configMap(namespaceName, configMapName);
        for (Map.Entry<String, String> entry : loggers.entrySet()) {
            LOGGER.info("Check log level setting for logger: {} Expected: {}", entry.getKey(), entry.getValue());
            String loggerConfig = String.format("%s=%s", entry.getKey(), entry.getValue());
            result = configMap.contains(loggerConfig);

            // Validation failed
            if (!result) {
                break;
            }
        }

        return result;
    }

    private synchronized Boolean checkGcLoggingDeployments(String namespaceName, String deploymentName, String containerName) {
        LOGGER.info("Checking deployment: {}", deploymentName);
        List<Container> containers = kubeClient(namespaceName).getDeployment(deploymentName).getSpec().getTemplate().getSpec().getContainers();
        Container container = getContainerByName(containerName, containers);
        LOGGER.info("Checking container with name: {}", container.getName());
        return checkEnvVarValue(container);
    }

    private synchronized Boolean checkGcLoggingDeployments(String namespaceName, String deploymentName) {
        LOGGER.info("Checking deployment: {}", deploymentName);
        Container container = kubeClient(namespaceName).getDeployment(namespaceName, deploymentName).getSpec().getTemplate().getSpec().getContainers().get(0);
        LOGGER.info("Checking container with name: {}", container.getName());
        return checkEnvVarValue(container);
    }

    private synchronized Boolean checkGcLoggingDeploymentConfig(String namespaceName, String depConfName) {
        LOGGER.info("Checking deployment config: {}", depConfName);
        Container container = kubeClient(namespaceName).getDeploymentConfig(namespaceName, depConfName).getSpec().getTemplate().getSpec().getContainers().get(0);
        LOGGER.info("Checking container with name: {}", container.getName());
        return checkEnvVarValue(container);
    }

    private synchronized Boolean checkGcLoggingStatefulSets(String namespaceName, String statefulSetName) {
        LOGGER.info("Checking stateful set: {}", statefulSetName);
        Container container = kubeClient(namespaceName).getStatefulSet(statefulSetName).getSpec().getTemplate().getSpec().getContainers().get(0);
        LOGGER.info("Checking container with name: {}", container.getName());
        return checkEnvVarValue(container);
    }

    private synchronized Container getContainerByName(String containerName, List<Container> containers) {
        return containers.stream().filter(c -> c.getName().equals(containerName)).findFirst().orElse(null);
    }

    private synchronized Boolean checkEnvVarValue(Container container) {
        assertThat("Container is null!", container, is(notNullValue()));

        List<EnvVar> loggingEnvVar = container.getEnv().stream().filter(envVar -> envVar.getName().contains("GC_LOG_ENABLED")).collect(Collectors.toList());
        LOGGER.info("{}={}", loggingEnvVar.get(0).getName(), loggingEnvVar.get(0).getValue());
        return loggingEnvVar.get(0).getValue().contains("true");
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(NAMESPACE)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .createInstallation()
            .runInstallation();

        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaPersistent(LOG_SETTING_CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .withNewInlineLogging()
                        .withLoggers(KAFKA_LOGGERS)
                    .endInlineLogging()
                    .withNewJvmOptions()
                        .withGcLoggingEnabled(true)
                    .endJvmOptions()
                .endKafka()
                .editZookeeper()
                    .withNewInlineLogging()
                        .withLoggers(ZOOKEEPER_LOGGERS)
                    .endInlineLogging()
                    .withNewJvmOptions()
                        .withGcLoggingEnabled(true)
                    .endJvmOptions()
                .endZookeeper()
                .editEntityOperator()
                    .editOrNewUserOperator()
                        .withNewInlineLogging()
                            .withLoggers(OPERATORS_LOGGERS)
                        .endInlineLogging()
                        .withNewJvmOptions()
                            .withGcLoggingEnabled(true)
                        .endJvmOptions()
                    .endUserOperator()
                    .editOrNewTopicOperator()
                        .withNewInlineLogging()
                            .withLoggers(OPERATORS_LOGGERS)
                        .endInlineLogging()
                        .withNewJvmOptions()
                            .withGcLoggingEnabled(true)
                        .endJvmOptions()
                    .endTopicOperator()
                .endEntityOperator()
                .withNewCruiseControl()
                .endCruiseControl()
                .withNewKafkaExporter()
                .endKafkaExporter()
            .endSpec()
            .build());

//         deploying second Kafka here because of MM and MM2 tests
        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaPersistent(GC_LOGGING_SET_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewJvmOptions()
                    .endJvmOptions()
                .endKafka()
                .editZookeeper()
                    .withNewJvmOptions()
                    .endJvmOptions()
                .endZookeeper()
                .editEntityOperator()
                    .editTopicOperator()
                        .withNewJvmOptions()
                        .endJvmOptions()
                    .endTopicOperator()
                    .editUserOperator()
                        .withNewJvmOptions()
                        .endJvmOptions()
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .build());

        String kafkaClientsName = "shared" + "-" + Constants.KAFKA_CLIENTS;
        resourceManager.createResource(extensionContext, false, KafkaClientsTemplates.kafkaClients(NAMESPACE, false, kafkaClientsName).build());

        // sync point wait for all resources
        KafkaUtils.waitForKafkaReady(NAMESPACE, LOG_SETTING_CLUSTER_NAME);
        KafkaUtils.waitForKafkaReady(NAMESPACE, GC_LOGGING_SET_NAME);
        DeploymentUtils.waitForDeploymentReady(NAMESPACE, kafkaClientsName);
    }

    @AfterAll
    protected void tearDownEnvironmentAfterAll() {
        teardownEnvForOperator();
    }
}
