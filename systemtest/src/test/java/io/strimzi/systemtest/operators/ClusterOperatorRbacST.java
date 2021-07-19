/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.kubernetes.RoleBindingResource;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
public class ClusterOperatorRbacST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(ClusterOperatorRbacST.class);
    public static final String NAMESPACE = "cluster-operator-test";

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(CONNECT)
    void testCRBDeletionErrorIsIgnoredWhenRackAwarenessIsNotEnabled(ExtensionContext extensionContext) {
        assumeFalse(Environment.isNamespaceRbacScope());

        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        applyRoleBindingsWithoutCRBs(extensionContext);
        // 060-Deployment
        resourceManager.createResource(extensionContext,
            new BundleResource.BundleResourceBuilder()
                .withName(Constants.STRIMZI_DEPLOYMENT_NAME)
                .withNamespace(NAMESPACE)
                .buildBundleInstance()
                .buildBundleDeployment()
                .build());

        String coPodName = kubeClient().getClusterOperatorPodName();
        LOGGER.info("Deploying Kafka: {}, which should be deployed even the CRBs are not present", clusterName);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        LOGGER.info("CO log should contain some information about ignoring forbidden access to CRB for Kafka");
        String log = cmdKubeClient().execInCurrentNamespace(false, "logs", coPodName).out();
        assertTrue(log.contains("Ignoring forbidden access to ClusterRoleBindings resource which does not seem to be required."));

        LOGGER.info("Deploying KafkaConnect: {} without rack awareness, the CR should be deployed without error", clusterName);
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1, false).build());

        LOGGER.info("CO log should contain some information about ignoring forbidden access to CRB for KafkaConnect");
        log = cmdKubeClient().execInCurrentNamespace(false, "logs", coPodName, "--tail", "50").out();
        assertTrue(log.contains("Ignoring forbidden access to ClusterRoleBindings resource which does not seem to be required."));
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(CONNECT)
    void testCRBDeletionErrorsWhenRackAwarenessIsEnabled(ExtensionContext extensionContext) {
        assumeFalse(Environment.isNamespaceRbacScope());

        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        applyRoleBindingsWithoutCRBs(extensionContext);
        // 060-Deployment
        resourceManager.createResource(extensionContext,
            new BundleResource.BundleResourceBuilder()
                .withNamespace(NAMESPACE)
                .buildBundleInstance()
                .buildBundleDeployment()
                .build());

        String rackKey = "rack-key";

        LOGGER.info("Deploying Kafka: {}, which should not be deployed and error should be present in CR status message", clusterName);
        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaEphemeral(clusterName, 3, 3)
            .editOrNewSpec()
                .editOrNewKafka()
                    .withNewRack()
                        .withTopologyKey(rackKey)
                    .endRack()
                .endKafka()
            .endSpec()
            .build());

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, NAMESPACE, ".*Forbidden!.*");
        Condition kafkaStatusCondition = KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).get().getStatus().getConditions().get(0);
        assertTrue(kafkaStatusCondition.getMessage().contains("Configured service account doesn't have access."));
        assertThat(kafkaStatusCondition.getType(), is(NotReady.toString()));

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(kafkaClientsName).build());
        resourceManager.createResource(extensionContext, false, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, clusterName, 1)
            .editSpec()
                .withNewRack(rackKey)
            .endSpec()
            .build());

        KafkaConnectUtils.waitUntilKafkaConnectStatusConditionContainsMessage(clusterName, NAMESPACE, ".*Forbidden!.*");
        Condition kafkaConnectStatusCondition = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(clusterName).get().getStatus().getConditions().get(0);
        assertTrue(kafkaConnectStatusCondition.getMessage().contains("Configured service account doesn't have access."));
        assertThat(kafkaConnectStatusCondition.getType(), is(NotReady.toString()));
    }

    private void applyRoleBindingsWithoutCRBs(ExtensionContext extensionContext) {
        // 020-RoleBinding
        RoleBindingResource.roleBinding(TestUtils.USER_PATH + "/../packaging/install/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml", NAMESPACE, NAMESPACE);
        // 031-RoleBinding
        RoleBindingResource.roleBinding(TestUtils.USER_PATH + "/../packaging/install/cluster-operator/031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml", NAMESPACE, NAMESPACE);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        install.prepareEnvForOperator(extensionContext, NAMESPACE);
    }
}
