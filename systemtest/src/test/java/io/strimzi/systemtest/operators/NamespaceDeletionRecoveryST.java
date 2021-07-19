/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.PersistentVolume;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.storage.StorageClass;
import io.fabric8.kubernetes.api.model.storage.StorageClassBuilder;
import io.strimzi.api.kafka.model.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.SetupClusterOperator;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NamespaceUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.List;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.RECOVERY;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

/**
 * Suite for testing topic recovery in case of namespace deletion.
 * Procedure described in documentation  https://strimzi.io/docs/master/#namespace-deletion_str
 */
@Tag(RECOVERY)
class NamespaceDeletionRecoveryST extends AbstractST {

    static final String NAMESPACE = "namespace-recovery-cluster-test";

    private static final Logger LOGGER = LogManager.getLogger(NamespaceDeletionRecoveryST.class);

    private String storageClassName = "retain";
    private static final String CLUSTER_NAME = "my-cluster";

    /**
     * In case that we have all KafkaTopic resources that existed before cluster loss, including internal topics,
     * we can simply recreate all KafkaTopic resources and then deploy the Kafka cluster.
     * At the end we verify that we can receive messages from topic (so data are present).
     */
    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(INTERNAL_CLIENTS_USED)
    void testTopicAvailable(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        prepareEnvironmentForRecovery(extensionContext, topicName, MESSAGE_COUNT);

        // Wait till consumer offset topic is created
        KafkaTopicUtils.waitForKafkaTopicCreationByNamePrefix("consumer-offsets");
        // Get list of topics and list of PVC needed for recovery
        List<KafkaTopic> kafkaTopicList = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).list().getItems();
        List<PersistentVolumeClaim> persistentVolumeClaimList = kubeClient().getClient().persistentVolumeClaims().list().getItems();
        deleteAndRecreateNamespace();

        recreatePvcAndUpdatePv(persistentVolumeClaimList);
        recreateClusterOperator(extensionContext);

        // Recreate all KafkaTopic resources
        for (KafkaTopic kafkaTopic : kafkaTopicList) {
            kafkaTopic.getMetadata().setResourceVersion(null);
            KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).createOrReplace(kafkaTopic);
        }

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withSize("100")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endKafka()
                .editZookeeper()
                    .withNewPersistentClaimStorage()
                        .withSize("100")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, clusterName + "-" + Constants.KAFKA_CLIENTS).build());

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Checking produced and consumed messages to pod:{}", internalKafkaClient.getPodName());
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );
    }

    /**
     * In case we don't have KafkaTopic resources from before the cluster loss, we do these steps:
     *  1. deploy the Kafka cluster without Topic Operator - otherwise topics will be deleted
     *  2. delete KafkaTopic Store topics - `__strimzi-topic-operator-kstreams-topic-store-changelog` and `__strimzi_store_topic`
     *  3. enable Topic Operator by redeploying Kafka cluster
     * @throws InterruptedException - sleep
     */
    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(INTERNAL_CLIENTS_USED)
    void testTopicNotAvailable(ExtensionContext extensionContext) throws InterruptedException {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        prepareEnvironmentForRecovery(extensionContext, topicName, MESSAGE_COUNT);

        // Wait till consumer offset topic is created
        KafkaTopicUtils.waitForKafkaTopicCreationByNamePrefix("consumer-offsets");
        // Get list of topics and list of PVC needed for recovery
        List<PersistentVolumeClaim> persistentVolumeClaimList = kubeClient().getClient().persistentVolumeClaims().list().getItems();
        deleteAndRecreateNamespace();
        recreatePvcAndUpdatePv(persistentVolumeClaimList);
        recreateClusterOperator(extensionContext);

        // Recreate Kafka Cluster
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withSize("100")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endKafka()
                .editZookeeper()
                    .withNewPersistentClaimStorage()
                        .withSize("100")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endZookeeper()
                .withNewEntityOperator()
                .endEntityOperator()
            .endSpec()
            .build());

        // Wait some time after kafka is ready before delete topics files
        Thread.sleep(60000);
        // Remove all topic data from topic store

        String deleteTopicStoreTopics = "./bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic __strimzi-topic-operator-kstreams-topic-store-changelog --delete " +
            "&& ./bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic __strimzi_store_topic --delete";

        cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", deleteTopicStoreTopics);
        // Wait till exec result will be finish
        Thread.sleep(30000);
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().setEntityOperator(new EntityOperatorSpecBuilder()
                .withNewTopicOperator()
                .endTopicOperator()
                .withNewUserOperator()
                .endUserOperator().build());
        });

        DeploymentUtils.waitForDeploymentAndPodsReady(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1);

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, clusterName + "-" + Constants.KAFKA_CLIENTS).build());

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Checking produced and consumed messages to pod:{}", internalKafkaClient.getPodName());
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );
    }

    private void prepareEnvironmentForRecovery(ExtensionContext extensionContext, String topicName, int messageCount) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(NAMESPACE)
            .createInstallation()
            .runInstallation();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withSize("100")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endKafka()
                .editZookeeper()
                    .withNewPersistentClaimStorage()
                        .withSize("100")
                        .withStorageClass(storageClassName)
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, clusterName + "-" + Constants.KAFKA_CLIENTS).build());

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Checking produced and consumed messages to pod:{}", internalKafkaClient.getPodName());
        internalKafkaClient.checkProducedAndConsumedMessages(
                internalKafkaClient.sendMessagesPlain(),
                internalKafkaClient.receiveMessagesPlain()
        );
    }

    private void recreatePvcAndUpdatePv(List<PersistentVolumeClaim> persistentVolumeClaimList) {
        for (PersistentVolumeClaim pvc : persistentVolumeClaimList) {
            pvc.getMetadata().setResourceVersion(null);
            kubeClient().getClient().persistentVolumeClaims().inNamespace(NAMESPACE).create(pvc);

            PersistentVolume pv = kubeClient().getClient().persistentVolumes().withName(pvc.getSpec().getVolumeName()).get();
            pv.getSpec().setClaimRef(null);
            kubeClient().getClient().persistentVolumes().createOrReplace(pv);
        }
    }

    private void recreateClusterOperator(ExtensionContext extensionContext) {
        // Recreate CO
        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(NAMESPACE)
            .createInstallation()
            .runInstallation();
    }

    private void deleteAndRecreateNamespace() {
        // Delete namespace with all resources
        kubeClient().deleteNamespace(NAMESPACE);
        NamespaceUtils.waitForNamespaceDeletion(NAMESPACE);

        // Recreate namespace
        cluster.createNamespace(NAMESPACE);
    }

    @BeforeAll
    void createStorageClass() {
        kubeClient().getClient().storage().storageClasses().withName(storageClassName).delete();
        StorageClass storageClass = new StorageClassBuilder()
            .withNewMetadata()
                .withName(storageClassName)
            .endMetadata()
            .withProvisioner("kubernetes.io/cinder")
            .withReclaimPolicy("Retain")
            .build();

        kubeClient().getClient().storage().storageClasses().createOrReplace(storageClass);
    }

    @AfterAll
    void teardown() {
        kubeClient().getClient().storage().storageClasses().withName(storageClassName).delete();

        kubeClient().getClient().persistentVolumes().list().getItems().stream()
            .filter(pv -> pv.getSpec().getClaimRef().getName().contains("kafka") || pv.getSpec().getClaimRef().getName().contains("zookeeper"))
            .forEach(pv -> kubeClient().getClient().persistentVolumes().delete(pv));
    }
}
