/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.olm;

import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceState;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.resources.specific.OlmResource;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMaker2Utils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMakerUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.extension.ExtensionContext;


import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

public class OlmAbstractST extends AbstractST {

    void doTestDeployExampleKafka() {
        JsonObject kafkaResource = OlmResource.getExampleResources().get(Kafka.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaResource.toString());
        KafkaUtils.waitForKafkaReady(kafkaResource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaUser(ExtensionContext extensionContext) {
        String userKafkaName = "user-kafka";
        // KafkaUser example needs Kafka with authorization
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(userKafkaName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationSimple()
                    .endKafkaAuthorizationSimple()
                .endKafka()
            .endSpec()
            .build());
        JsonObject kafkaUserResource = OlmResource.getExampleResources().get(KafkaUser.RESOURCE_KIND);
        kafkaUserResource.getJsonObject("metadata").getJsonObject("labels").put(Labels.STRIMZI_CLUSTER_LABEL, userKafkaName);
        cmdKubeClient().applyContent(kafkaUserResource.toString());
        KafkaUserUtils.waitForKafkaUserCreation(kafkaUserResource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaTopic() {
        JsonObject kafkaTopicResource = OlmResource.getExampleResources().get(KafkaTopic.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaTopicResource.toString());
        KafkaTopicUtils.waitForKafkaTopicCreation(kafkaTopicResource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaConnect() {
        JsonObject kafkaConnectResource = OlmResource.getExampleResources().get(KafkaConnect.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaConnectResource.toString());
        KafkaConnectUtils.waitForConnectReady(kafkaConnectResource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaBridge() {
        JsonObject kafkaBridgeResource = OlmResource.getExampleResources().get(KafkaBridge.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaBridgeResource.toString());
        KafkaBridgeUtils.waitForKafkaBridgeReady(kafkaBridgeResource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaMirrorMaker() {
        JsonObject kafkaMirrorMakerResource = OlmResource.getExampleResources().get(KafkaMirrorMaker.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaMirrorMakerResource.toString()
                .replace("my-source-cluster-kafka-bootstrap", "my-cluster-kafka-bootstrap")
                .replace("my-target-cluster-kafka-bootstrap", "my-cluster-kafka-bootstrap"));
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(kafkaMirrorMakerResource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaMirrorMaker2() {
        JsonObject kafkaMirrorMaker2Resource = OlmResource.getExampleResources().get(KafkaMirrorMaker2.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaMirrorMaker2Resource.toString()
                .replace("my-cluster-source-kafka-bootstrap", "my-cluster-kafka-bootstrap")
                .replace("my-cluster-target-kafka-bootstrap", "my-cluster-kafka-bootstrap"));
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(kafkaMirrorMaker2Resource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaRebalance(ExtensionContext extensionContext) {
        String cruiseControlClusterName = "cruise-control";
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(cruiseControlClusterName, 3, 3).build());
        JsonObject kafkaRebalanceResource = OlmResource.getExampleResources().get(KafkaRebalance.RESOURCE_KIND);
        kafkaRebalanceResource.getJsonObject("metadata").getJsonObject("labels").put(Labels.STRIMZI_CLUSTER_LABEL, cruiseControlClusterName);
        cmdKubeClient().applyContent(kafkaRebalanceResource.toString());
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState("my-rebalance", KafkaRebalanceState.PendingProposal);
    }

    @AfterAll
    void teardown(ExtensionContext extensionContext) {
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaRebalance.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaMirrorMaker2.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaMirrorMaker.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaBridge.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaConnect.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaTopic.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaUser.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(Kafka.RESOURCE_KIND).toString());
    }
}
