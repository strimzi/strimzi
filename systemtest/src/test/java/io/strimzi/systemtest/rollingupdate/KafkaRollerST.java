/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.rollingupdate;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirementBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorTerm;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.template.KafkaClusterTemplate;
import io.strimzi.api.kafka.model.template.KafkaClusterTemplateBuilder;
import io.strimzi.api.kafka.model.template.PodTemplate;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.SetupClusterOperator;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.timemeasuring.Operation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.ROLLING_UPDATE;
import static io.strimzi.systemtest.k8s.Events.Created;
import static io.strimzi.systemtest.k8s.Events.Pulled;
import static io.strimzi.systemtest.k8s.Events.Scheduled;
import static io.strimzi.systemtest.k8s.Events.Started;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
@Tag(ROLLING_UPDATE)
public class KafkaRollerST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(KafkaRollerST.class);
    static final String NAMESPACE = "kafka-roller-cluster-test";

    @ParallelNamespaceTest
    void testKafkaRollsWhenTopicIsUnderReplicated(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        String operationId = timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());

        // We need to start with 3 replicas / brokers,
        // so that KafkaStreamsTopicStore topic gets set/distributed on this first 3 [0, 1, 2],
        // since this topic has replication-factor 3 and minISR 2.
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3)
            .editSpec()
                .editKafka()
                    .addToConfig("auto.create.topics.enable", "false")
                .endKafka()
            .endSpec()
            .build());

        String kafkaStsName = KafkaResources.kafkaStatefulSetName(clusterName);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(clusterName));

        LOGGER.info("Running kafkaScaleUpScaleDown {}", clusterName);
        final int initialReplicas = kubeClient(namespaceName).getStatefulSet(KafkaResources.kafkaStatefulSetName(clusterName)).getStatus().getReplicas();
        assertEquals(3, initialReplicas);

        // Now that KafkaStreamsTopicStore topic is set on the first 3 brokers, lets spin-up another one.
        int scaledUpReplicas = 4;
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getKafka().setReplicas(scaledUpReplicas), namespaceName);

        kafkaPods = StatefulSetUtils.waitTillSsHasRolled(namespaceName, kafkaStsName, scaledUpReplicas, kafkaPods);

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName, 4, 4, 4).build());

        //Test that the new pod does not have errors or failures in events
        String uid = kubeClient(namespaceName).getPodUid(KafkaResources.kafkaPodName(clusterName,  3));
        List<Event> events = kubeClient(namespaceName).listEventsByResourceUid(uid);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));

        //Test that CO doesn't have any exceptions in log
        timeMeasuringSystem.stopOperation(operationId, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
        assertNoCoErrorsLogged(timeMeasuringSystem.getDurationInSeconds(extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName(), operationId));

        // scale down
        final int scaledDownReplicas = 3;
        LOGGER.info("Scaling down to {}", scaledDownReplicas);
        operationId = timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getKafka().setReplicas(scaledDownReplicas), namespaceName);

        kafkaPods = StatefulSetUtils.waitTillSsHasRolled(namespaceName, kafkaStsName, scaledDownReplicas, kafkaPods);

        PodUtils.verifyThatRunningPodsAreStable(namespaceName, clusterName);

        // set annotation to trigger Kafka rolling update
        kubeClient(namespaceName).statefulSet(KafkaResources.kafkaStatefulSetName(clusterName)).withPropagationPolicy(DeletionPropagation.ORPHAN).edit(sts -> new StatefulSetBuilder(sts)
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
            .endMetadata()
            .build());

        StatefulSetUtils.waitTillSsHasRolled(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), kafkaPods);
    }

    @ParallelNamespaceTest
    void testKafkaTopicRFLowerThanMinInSyncReplicas(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName, 1, 1).build());

        String kafkaName = KafkaResources.kafkaStatefulSetName(clusterName);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(namespaceName, kafkaName);

        LOGGER.info("Setting KafkaTopic's min.insync.replicas to be higher than replication factor");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> kafkaTopic.getSpec().getConfig().replace("min.insync.replicas", 2), namespaceName);

        // rolling update for kafka
        LOGGER.info("Annotate Kafka StatefulSet {} with manual rolling update annotation", kafkaName);

        // set annotation to trigger Kafka rolling update
        kubeClient(namespaceName).statefulSet(kafkaName).withPropagationPolicy(DeletionPropagation.ORPHAN).edit(sts -> new StatefulSetBuilder(sts)
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
            .endMetadata()
            .build());

        StatefulSetUtils.waitTillSsHasRolled(namespaceName, kafkaName, 3, kafkaPods);
        assertThat(StatefulSetUtils.ssSnapshot(namespaceName, kafkaName), is(not(kafkaPods)));
    }

    @ParallelNamespaceTest
    void testKafkaPodCrashLooping(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withNewJvmOptions()
                        .withXx(Collections.emptyMap())
                    .endJvmOptions()
                .endKafka()
            .endSpec()
            .build());

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka ->
                kafka.getSpec().getKafka().getJvmOptions().setXx(Collections.singletonMap("UseParNewGC", "true")), namespaceName);

        KafkaUtils.waitForKafkaNotReady(namespaceName, clusterName);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka ->
                kafka.getSpec().getKafka().getJvmOptions().setXx(Collections.emptyMap()), namespaceName);

        // kafka should get back ready in some reasonable time frame.
        // Current timeout for wait is set to 14 minutes, which should be enough.
        // No additional checks are needed, because in case of wait failure, the test will not continue.
        KafkaUtils.waitForKafkaReady(namespaceName, clusterName);
    }

    @ParallelNamespaceTest
    void testKafkaPodImagePullBackOff(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3).build());

        String kafkaImage = kubeClient(namespaceName).getStatefulSet(KafkaResources.kafkaStatefulSetName(clusterName))
            .getSpec().getTemplate().getSpec().getContainers().get(0).getImage();

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            kafka.getSpec().getKafka().setImage("quay.io/strimzi/kafka:not-existent-tag");
            kafka.getSpec().getZookeeper().setImage(kafkaImage);
        }, namespaceName);

        KafkaUtils.waitForKafkaNotReady(namespaceName, clusterName);

        assertTrue(checkIfExactlyOneKafkaPodIsNotReady(namespaceName, clusterName));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> kafka.getSpec().getKafka().setImage(kafkaImage), namespaceName);

        // kafka should get back ready in some reasonable time frame.
        // Current timeout for wait is set to 14 minutes, which should be enough.
        // No additional checks are needed, because in case of wait failure, the test will not continue.
        KafkaUtils.waitForKafkaReady(namespaceName, clusterName);
    }

    @ParallelNamespaceTest
    public void testKafkaPodPending(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        ResourceRequirements rr = new ResourceRequirementsBuilder()
            .withRequests(Collections.emptyMap())
            .build();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withResources(rr)
                .endKafka()
            .endSpec()
            .build());

        Map<String, Quantity> requests = new HashMap<>(2);
        requests.put("cpu", new Quantity("123456"));
        requests.put("memory", new Quantity("128Mi"));
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka ->
                kafka.getSpec().getKafka().getResources().setRequests(requests), namespaceName);

        KafkaUtils.waitForKafkaNotReady(namespaceName, clusterName);

        assertTrue(checkIfExactlyOneKafkaPodIsNotReady(namespaceName, clusterName));

        requests.put("cpu", new Quantity("100m"));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka ->
                kafka.getSpec().getKafka().getResources().setRequests(requests), namespaceName);

        // kafka should get back ready in some reasonable time frame.
        // Current timeout for wait is set to 14 minutes, which should be enough.
        // No additional checks are needed, because in case of wait failure, the test will not continue.
        KafkaUtils.waitForKafkaReady(namespaceName, clusterName);
    }

    @ParallelNamespaceTest
    void testKafkaPodPendingDueToRack(ExtensionContext extensionContext) {
        // Testing this scenario
        // 1. deploy Kafka with wrong pod template (looking for nonexistent node) kafka pods should not exist
        // 2. wait for Kafka not ready, kafka pods should be in the pending state
        // 3. fix the Kafka CR, kafka pods should be in the pending state
        // 4. wait for Kafka ready, kafka pods should NOT be in the pending state
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        NodeSelectorRequirement nsr = new NodeSelectorRequirementBuilder()
                .withKey("dedicated_test")
                .withOperator("In")
                .withValues("Kafka")
                .build();

        NodeSelectorTerm nst = new NodeSelectorTermBuilder()
                .withMatchExpressions(nsr)
                .build();

        Affinity affinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(nst)
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .build();

        PodTemplate pt = new PodTemplate();
        pt.setAffinity(affinity);

        KafkaClusterTemplate kct = new KafkaClusterTemplateBuilder()
                .withPod(pt)
                .build();

        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaEphemeral(clusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withTemplate(kct)
                .endKafka()
            .endSpec()
            .build());

        // pods are stable in the Pending state
        PodUtils.waitUntilPodStabilityReplicasCount(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3);

        LOGGER.info("Removing requirement for the affinity");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka ->
                kafka.getSpec().getKafka().getTemplate().getPod().setAffinity(null), namespaceName);

        // kafka should get back ready in some reasonable time frame
        KafkaUtils.waitForKafkaReady(namespaceName, clusterName);
        KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        KafkaUtils.waitForKafkaDeletion(namespaceName, clusterName);
    }

    boolean checkIfExactlyOneKafkaPodIsNotReady(String namespaceName, String clusterName) {
        List<Pod> kafkaPods = kubeClient(namespaceName).listPodsByPrefixInName(KafkaResources.kafkaStatefulSetName(clusterName));
        int runningKafkaPods = (int) kafkaPods.stream().filter(pod -> pod.getStatus().getPhase().equals("Running")).count();

        return runningKafkaPods == (kafkaPods.size() - 1);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(NAMESPACE)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .withOperationTimeout(Constants.CO_OPERATION_TIMEOUT_MEDIUM)
            .createInstallation()
            .runInstallation();
    }
}
