/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.connect;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.OtherArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.api.kafka.model.connect.build.TgzArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.ZipArtifactBuilder;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.SetupClusterOperator;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
@Tag(CONNECT_COMPONENTS)
@Tag(CONNECT)
class ConnectBuilderST extends AbstractST {

    private static final String NAMESPACE = "connect-builder";
    private static final Logger LOGGER = LogManager.getLogger(ConnectBuilderST.class);
    private static final String SHARED_KAFKA_CLUSTER_NAME = NAMESPACE;

    private static final String ECHO_SINK_CLASS_NAME = "cz.scholz.kafka.connect.echosink.EchoSinkConnector";
    private static final String CAMEL_CONNECTOR_CLASS_NAME = "org.apache.camel.kafkaconnector.http.CamelHttpSinkConnector";

    private static final String ECHO_SINK_JAR_URL = "https://github.com/scholzj/echo-sink/releases/download/1.1.0/echo-sink-1.1.0.jar";
    private static final String ECHO_SINK_JAR_CHECKSUM = "b7da48d5ecd1e4199886d169ced1bf702ffbdfd704d69e0da97e78ff63c1bcece2f59c2c6c751f9c20be73472b8cb6a31b6fd4f75558c1cb9d96daa9e9e603d2";
    private static final String ECHO_SINK_FILE_NAME = "echo-sink-test.jar";

    private static final String ECHO_SINK_JAR_WRONG_CHECKSUM = "f1f167902325062efc8c755647bc1b782b2b067a87a6e507ff7a3f6205803220";

    private static final String ECHO_SINK_TGZ_URL = "https://github.com/scholzj/echo-sink/archive/1.1.0.tar.gz";
    private static final String ECHO_SINK_TGZ_CHECKSUM = "5318b1f031d4e5eeab6f8b774c76de297237574fc51d1e81b03a10e0b5d5435a46a108b85fdb604c644529f38830ae83239c17b6ec91c90a60ac790119bb2950";

    private static final String CAMEL_CONNECTOR_TGZ_URL = "https://repo.maven.apache.org/maven2/org/apache/camel/kafkaconnector/camel-http-kafka-connector/0.7.0/camel-http-kafka-connector-0.7.0-package.tar.gz";
    private static final String CAMEL_CONNECTOR_TGZ_CHECKSUM = "d0bb8c6a9e50b68eee3e4d70b6b7e5ae361373883ed3156bc11771330095b66195ac1c12480a0669712da4e5f38e64f004ffecabca4bf70d312f3f7ae0ad51b5";

    private static final String CAMEL_CONNECTOR_ZIP_URL = "https://repo.maven.apache.org/maven2/org/apache/camel/kafkaconnector/camel-http-kafka-connector/0.7.0/camel-http-kafka-connector-0.7.0-package.zip";
    private static final String CAMEL_CONNECTOR_ZIP_CHECKSUM = "bc15135b8ef7faccd073508da0510c023c0f6fa3ec7e48c98ad880dd112b53bf106ad0a47bcb353eed3ec03bb3d273da7de356f3f7f1766a13a234a6bc28d602";

    private static final String PLUGIN_WITH_OTHER_TYPE_NAME = "plugin-with-other-type";

    private String outputRegistry = "";

    private static final Plugin PLUGIN_WITH_TAR_AND_JAR = new PluginBuilder()
        .withName("connector-with-tar-and-jar")
        .withArtifacts(
            new JarArtifactBuilder()
                .withUrl(ECHO_SINK_JAR_URL)
                .withSha512sum(ECHO_SINK_JAR_CHECKSUM)
                .build(),
            new TgzArtifactBuilder()
                .withUrl(ECHO_SINK_TGZ_URL)
                .withSha512sum(ECHO_SINK_TGZ_CHECKSUM)
                .build())
        .build();

    private static final Plugin PLUGIN_WITH_ZIP = new PluginBuilder()
        .withName("connector-from-zip")
        .withArtifacts(
            new ZipArtifactBuilder()
                .withUrl(CAMEL_CONNECTOR_ZIP_URL)
                .withSha512sum(CAMEL_CONNECTOR_ZIP_CHECKSUM)
                .build())
        .build();

    private static final Plugin PLUGIN_WITH_OTHER_TYPE = new PluginBuilder()
        .withName(PLUGIN_WITH_OTHER_TYPE_NAME)
        .withArtifacts(
            new OtherArtifactBuilder()
                .withUrl(ECHO_SINK_JAR_URL)
                .withFileName(ECHO_SINK_FILE_NAME)
                .withSha512sum(ECHO_SINK_JAR_CHECKSUM)
                .build()
        )
        .build();

    @ParallelTest
    void testBuildFailsWithWrongChecksumOfArtifact(ExtensionContext extensionContext) {
        String connectClusterName = mapWithClusterNames.get(extensionContext.getDisplayName()) + "-connect";
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        final String imageName = getImageNameForTestCase();

        Plugin pluginWithWrongChecksum = new PluginBuilder()
            .withName("connector-with-wrong-checksum")
            .withArtifacts(new JarArtifactBuilder()
                .withUrl(ECHO_SINK_JAR_URL)
                .withSha512sum(ECHO_SINK_JAR_WRONG_CHECKSUM)
                .build())
            .build();

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());
        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        resourceManager.createResource(extensionContext, false, KafkaConnectTemplates.kafkaConnect(extensionContext, connectClusterName, SHARED_KAFKA_CLUSTER_NAME, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editOrNewSpec()
                .withNewBuild()
                    .withPlugins(pluginWithWrongChecksum)
                    .withNewDockerOutput()
                        .withImage(imageName)
                    .endDockerOutput()
                .endBuild()
            .endSpec()
            .build());

        KafkaConnectUtils.waitForConnectNotReady(connectClusterName);
        KafkaConnectUtils.waitUntilKafkaConnectStatusConditionContainsMessage(connectClusterName, NAMESPACE, "The Kafka Connect build failed(.*)?");

        LOGGER.info("Checking if KafkaConnect status condition contains message about build failure");
        KafkaConnect kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(connectClusterName).get();

        LOGGER.info("Deploying network policies for KafkaConnect");
        NetworkPolicyResource.deployNetworkPolicyForResource(extensionContext, kafkaConnect, KafkaConnectResources.deploymentName(connectClusterName));

        Condition connectCondition = kafkaConnect.getStatus().getConditions().stream().findFirst().orElseThrow();

        assertTrue(connectCondition.getMessage().matches("The Kafka Connect build failed(.*)?"));
        assertThat(connectCondition.getType(), is(NotReady.toString()));

        LOGGER.info("Replacing plugin's checksum with right one");
        KafkaConnectResource.replaceKafkaConnectResource(connectClusterName, kC -> {
            Plugin pluginWithRightChecksum = new PluginBuilder()
                .withName("connector-with-right-checksum")
                .withArtifacts(new JarArtifactBuilder()
                    .withUrl(ECHO_SINK_JAR_URL)
                    .withSha512sum(ECHO_SINK_JAR_CHECKSUM)
                    .build())
                .build();

            kC.getSpec().getBuild().getPlugins().remove(0);
            kC.getSpec().getBuild().getPlugins().add(pluginWithRightChecksum);
        });

        KafkaConnectUtils.waitForConnectReady(connectClusterName);

        LOGGER.info("Checking if KafkaConnect API contains EchoSink connector");
        String plugins = cmdKubeClient().execInPod(kafkaClientsPodName, "curl", "-X", "GET", "http://" + KafkaConnectResources.serviceName(connectClusterName) + ":8083/connector-plugins").out();

        assertTrue(plugins.contains(ECHO_SINK_CLASS_NAME));

        LOGGER.info("Checking if KafkaConnect resource contains EchoSink connector in status");
        kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(connectClusterName).get();
        assertTrue(kafkaConnect.getStatus().getConnectorPlugins().stream().anyMatch(connectorPlugin -> connectorPlugin.getConnectorClass().contains(ECHO_SINK_CLASS_NAME)));
    }

    @ParallelTest
    void testBuildWithJarTgzAndZip(ExtensionContext extensionContext) {
        // this test also testing push into Docker output
        String connectClusterName = mapWithClusterNames.get(extensionContext.getDisplayName()) + "-connect";
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        final String imageName = getImageNameForTestCase();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(SHARED_KAFKA_CLUSTER_NAME, topicName).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, connectClusterName, SHARED_KAFKA_CLUSTER_NAME, 1, false)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editOrNewSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewBuild()
                    .withPlugins(PLUGIN_WITH_TAR_AND_JAR, PLUGIN_WITH_ZIP)
                    .withNewDockerOutput()
                        .withImage(imageName)
                    .endDockerOutput()
                .endBuild()
                .withNewInlineLogging()
                    .addToLoggers("connect.root.logger.level", "INFO")
                .endInlineLogging()
            .endSpec()
            .build());

        Map<String, Object> connectorConfig = new HashMap<>();
        connectorConfig.put("topics", topicName);
        connectorConfig.put("level", "INFO");

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());

        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(connectClusterName)
            .editOrNewSpec()
                .withClassName(ECHO_SINK_CLASS_NAME)
                .withConfig(connectorConfig)
            .endSpec()
            .build());

        KafkaConnector kafkaConnector = KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(connectClusterName).get();

        assertThat(kafkaConnector.getSpec().getClassName(), is(ECHO_SINK_CLASS_NAME));

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(SHARED_KAFKA_CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.sendMessagesPlain();

        String connectPodName = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).stream()
                .filter(it -> it.contains(connectClusterName)).collect(Collectors.toList()).get(0);
        PodUtils.waitUntilMessageIsInPodLogs(connectPodName, "Received message with key 'null' and value '99'");
    }

    @OpenShiftOnly
    @ParallelTest
    void testPushIntoImageStream(ExtensionContext extensionContext) {
        String connectClusterTest = mapWithClusterNames.get(extensionContext.getDisplayName()) + "-connect";

        String imageStreamName = "custom-image-stream";
        ImageStream imageStream = new ImageStreamBuilder()
            .editOrNewMetadata()
                .withName(imageStreamName)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build();

        kubeClient().getClient().adapt(OpenShiftClient.class).imageStreams().inNamespace(NAMESPACE).create(imageStream);

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, connectClusterTest, SHARED_KAFKA_CLUSTER_NAME, 1, false)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editOrNewSpec()
                .withNewBuild()
                    .withPlugins(PLUGIN_WITH_TAR_AND_JAR)
                    .withNewImageStreamOutput()
                        .withImage(imageStreamName + ":latest")
                    .endImageStreamOutput()
                .endBuild()
            .endSpec()
            .build());

        KafkaConnect kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(connectClusterTest).get();

        LOGGER.info("Checking, if KafkaConnect has all artifacts and if is successfully created");
        assertThat(kafkaConnect.getSpec().getBuild().getPlugins().get(0).getArtifacts().size(), is(2));
        assertThat(kafkaConnect.getSpec().getBuild().getOutput().getType(), is("imagestream"));
        assertThat(kafkaConnect.getSpec().getBuild().getOutput().getImage(), is(imageStreamName + ":latest"));
        assertThat(kafkaConnect.getStatus().getConditions().get(0).getType(), is(Ready.toString()));

        assertTrue(kafkaConnect.getStatus().getConnectorPlugins().size() > 0);
        assertTrue(kafkaConnect.getStatus().getConnectorPlugins().stream().anyMatch(connectorPlugin -> connectorPlugin.getConnectorClass().contains(ECHO_SINK_CLASS_NAME)));
    }

    @ParallelTest
    void testUpdateConnectWithAnotherPlugin(ExtensionContext extensionContext) {
        String connectClusterName = mapWithClusterNames.get(extensionContext.getDisplayName()) + "-connect";
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        String echoConnector = "echo-sink-connector";
        String camelConnector = "camel-http-connector";
        final String imageName = getImageNameForTestCase();

        Plugin secondPlugin =  new PluginBuilder()
            .withName("camel-connector")
            .withArtifacts(
                new TgzArtifactBuilder()
                    .withUrl(CAMEL_CONNECTOR_TGZ_URL)
                    .withSha512sum(CAMEL_CONNECTOR_TGZ_CHECKSUM)
                    .build())
            .build();

        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(SHARED_KAFKA_CLUSTER_NAME, topicName).build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());

        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, connectClusterName, SHARED_KAFKA_CLUSTER_NAME, 1, true)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editOrNewSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewBuild()
                    .withPlugins(PLUGIN_WITH_TAR_AND_JAR)
                    .withNewDockerOutput()
                        .withImage(imageName)
                    .endDockerOutput()
                .endBuild()
                .withNewInlineLogging()
                    .addToLoggers("connect.root.logger.level", "INFO")
                .endInlineLogging()
            .endSpec()
            .build());

        Map<String, Object> echoSinkConfig = new HashMap<>();
        echoSinkConfig.put("topics", topicName);
        echoSinkConfig.put("level", "INFO");

        LOGGER.info("Creating EchoSink connector");
        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(echoConnector, connectClusterName)
            .editOrNewSpec()
                .withClassName(ECHO_SINK_CLASS_NAME)
                .withConfig(echoSinkConfig)
            .endSpec()
            .build());

        String deploymentName = KafkaConnectResources.deploymentName(connectClusterName);
        Map<String, String> connectSnapshot = DeploymentUtils.depSnapshot(deploymentName);

        LOGGER.info("Checking that KafkaConnect API contains EchoSink connector and not Camel-Telegram Connector class name");
        String plugins = cmdKubeClient().execInPod(kafkaClientsPodName, "curl", "-X", "GET", "http://" + KafkaConnectResources.serviceName(connectClusterName) + ":8083/connector-plugins").out();

        assertFalse(plugins.contains(CAMEL_CONNECTOR_CLASS_NAME));
        assertTrue(plugins.contains(ECHO_SINK_CLASS_NAME));

        LOGGER.info("Adding one more connector to the KafkaConnect");
        KafkaConnectResource.replaceKafkaConnectResource(connectClusterName, kafkaConnect -> {
            kafkaConnect.getSpec().getBuild().getPlugins().add(secondPlugin);
        });

        DeploymentUtils.waitTillDepHasRolled(deploymentName, 1, connectSnapshot);

        Map<String, Object> camelHttpConfig = new HashMap<>();
        camelHttpConfig.put("camel.sink.path.httpUri", "http://" + KafkaConnectResources.serviceName(connectClusterName) + ":8083");
        camelHttpConfig.put("topics", topicName);

        LOGGER.info("Creating Camel-HTTP-Sink connector");
        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(camelConnector, connectClusterName)
            .editOrNewSpec()
                .withClassName(CAMEL_CONNECTOR_CLASS_NAME)
                .withConfig(camelHttpConfig)
            .endSpec()
            .build());

        KafkaConnect kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(connectClusterName).get();

        LOGGER.info("Checking if both Connectors were created and Connect contains both plugins");
        assertThat(kafkaConnect.getSpec().getBuild().getPlugins().size(), is(2));

        assertTrue(kafkaConnect.getStatus().getConnectorPlugins().stream().anyMatch(connectorPlugin -> connectorPlugin.getConnectorClass().contains(ECHO_SINK_CLASS_NAME)));
        assertTrue(kafkaConnect.getStatus().getConnectorPlugins().stream().anyMatch(connectorPlugin -> connectorPlugin.getConnectorClass().contains(CAMEL_CONNECTOR_CLASS_NAME)));
    }

    @ParallelTest
    void testBuildOtherPluginTypeWithAndWithoutFileName(ExtensionContext extensionContext) {
        final String connectClusterName = mapWithClusterNames.get(extensionContext.getDisplayName()) + "-connect";
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        final String imageName = getImageNameForTestCase();

        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(SHARED_KAFKA_CLUSTER_NAME, topicName).build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, connectClusterName, SHARED_KAFKA_CLUSTER_NAME, 1, false)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editOrNewSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .withNewBuild()
                        .withPlugins(PLUGIN_WITH_OTHER_TYPE)
                        .withNewDockerOutput()
                            .withImage(imageName)
                        .endDockerOutput()
                .endBuild()
            .endSpec()
            .build());

        String deploymentName = KafkaConnectResources.deploymentName(connectClusterName);
        Map<String, String> connectSnapshot = DeploymentUtils.depSnapshot(deploymentName);
        String connectPodName = kubeClient().listPods(connectClusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();

        LOGGER.info("Checking that plugin has correct file name: {}", ECHO_SINK_FILE_NAME);
        assertEquals(getPluginFileNameFromConnectPod(connectPodName), ECHO_SINK_FILE_NAME);

        final Plugin pluginWithoutFileName = new PluginBuilder()
            .withName(PLUGIN_WITH_OTHER_TYPE_NAME)
            .withArtifacts(
                new OtherArtifactBuilder()
                    .withUrl(ECHO_SINK_JAR_URL)
                    .withSha512sum(ECHO_SINK_JAR_CHECKSUM)
                    .build()
            )
            .build();

        LOGGER.info("Removing file name from the plugin, hash should be used");
        KafkaConnectResource.replaceKafkaConnectResource(connectClusterName, connect -> {
            connect.getSpec().getBuild().setPlugins(Collections.singletonList(pluginWithoutFileName));
        });

        DeploymentUtils.waitTillDepHasRolled(deploymentName, 1, connectSnapshot);

        LOGGER.info("Checking that plugin has different name than before");
        connectPodName = kubeClient().listPods(connectClusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();
        String fileName = getPluginFileNameFromConnectPod(connectPodName);
        assertNotEquals(fileName, ECHO_SINK_FILE_NAME);
        assertEquals(fileName, Util.sha1Prefix(ECHO_SINK_JAR_URL));
    }

    private String getPluginFileNameFromConnectPod(String connectPodName) {
        return cmdKubeClient().execInPod(connectPodName,
            "/bin/bash", "-c", "ls plugins/plugin-with-other-type/*").out().trim();
    }

    private String getImageNameForTestCase() {
        int randomNum = new Random().nextInt(Integer.MAX_VALUE);
        return outputRegistry + "/connect-build-" + randomNum + ":latest";
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(NAMESPACE)
            .withOperationTimeout(Constants.CO_OPERATION_TIMEOUT_SHORT)
            .createInstallation()
            .runInstallation();

        if (cluster.isNotKubernetes()) {
            outputRegistry = "image-registry.openshift-image-registry.svc:5000/" + NAMESPACE;
        } else {
            LOGGER.warn("For running these tests on K8s you have to have internal registry deployed using `minikube start --insecure-registry '10.0.0.0/24'` and `minikube addons enable registry`");
            Service service = kubeClient("kube-system").getService("registry");
            outputRegistry = service.getSpec().getClusterIP() + ":" + service.getSpec().getPorts().stream().filter(servicePort -> servicePort.getName().equals("http")).findFirst().orElseThrow().getPort();
        }
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(SHARED_KAFKA_CLUSTER_NAME, 3).build());
    }
}
