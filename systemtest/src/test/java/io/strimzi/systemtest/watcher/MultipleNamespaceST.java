/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.watcher;

import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.templates.KafkaTemplates;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Arrays;
import java.util.List;

import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
class MultipleNamespaceST extends AbstractNamespaceST {

    private static final Logger LOGGER = LogManager.getLogger(MultipleNamespaceST.class);

    /**
     * Test the case where the TO is configured to watch a different namespace that it is deployed in
     */
    @ParallelTest
    void testTopicOperatorWatchingOtherNamespace(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        String topicName = mapTestWithTestTopics.get(extensionContext.getDisplayName());

        LOGGER.info("Deploying TO to watch a different namespace that it is deployed in");
        cluster.setNamespace(SECOND_NAMESPACE);
        List<String> topics = KafkaCmdClient.listTopicsUsingPodCli(MAIN_NAMESPACE_CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems(topicName)));

        deployNewTopic(CO_NAMESPACE, SECOND_NAMESPACE, topicName);
        deleteNewTopic(CO_NAMESPACE, topicName);
        cluster.setNamespace(CO_NAMESPACE);
    }

    /**
     * Test the case when Kafka will be deployed in different namespace than CO
     */
    @ParallelTest
    void testKafkaInDifferentNsThanClusterOperator(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        LOGGER.info("Deploying Kafka in different namespace than CO when CO watches multiple namespaces");
        checkKafkaInDiffNamespaceThanCO(MAIN_NAMESPACE_CLUSTER_NAME, SECOND_NAMESPACE);
    }

    /**
     * Test the case when MirrorMaker will be deployed in different namespace across multiple namespaces
     */
    @ParallelTest
    @Tag(MIRROR_MAKER)
    void testDeployMirrorMakerAcrossMultipleNamespace(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        LOGGER.info("Deploying KafkaMirrorMaker in different namespace than CO when CO watches multiple namespaces");
        checkMirrorMakerForKafkaInDifNamespaceThanCO(extensionContext, MAIN_NAMESPACE_CLUSTER_NAME);
    }

    @BeforeAll
    void setupEnvironment(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        deployTestSpecificResources(extensionContext);
    }

    private void deployTestSpecificResources(ExtensionContext extensionContext) {
        prepareEnvForOperator(extensionContext, CO_NAMESPACE, Arrays.asList(CO_NAMESPACE, SECOND_NAMESPACE));

        applyBindings(extensionContext, CO_NAMESPACE);
        applyBindings(extensionContext, CO_NAMESPACE, SECOND_NAMESPACE);
        // 060-Deployment
        resourceManager.createResource(extensionContext, BundleResource.clusterOperator(String.join(",", CO_NAMESPACE, SECOND_NAMESPACE)).build());

        cluster.setNamespace(SECOND_NAMESPACE);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(MAIN_NAMESPACE_CLUSTER_NAME, 3)
            .editSpec()
                .editEntityOperator()
                    .editTopicOperator()
                        .withWatchedNamespace(CO_NAMESPACE)
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec()
            .build());

        cluster.setNamespace(CO_NAMESPACE);
    }
}
