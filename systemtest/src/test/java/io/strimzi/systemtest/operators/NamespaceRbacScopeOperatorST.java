/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.SetupClusterOperator;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(REGRESSION)
class NamespaceRbacScopeOperatorST extends AbstractST {

    static final String NAMESPACE = "roles-only-cluster-test";

    @IsolatedTest("This test case needs own Cluster Operator")
    void testNamespacedRbacScopeDeploysRoles(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        assumeTrue(Environment.isNamespaceRbacScope());
        prepareEnvironment(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 3)
            .editMetadata()
                .addToLabels("app", "strimzi")
            .endMetadata()
            .build());

        // Wait for Kafka to be Ready to ensure all potentially erroneous ClusterRole applications have happened
        KafkaUtils.waitForKafkaReady(clusterName);

        // Assert that no ClusterRoles are present on the server that have app strimzi
        // Naturally returns false positives if another Strimzi operator has been installed
        List<ClusterRole> strimziClusterRoles = kubeClient().listClusterRoles().stream()
            .filter(cr -> {
                Map<String, String> labels = cr.getMetadata().getLabels() != null ? cr.getMetadata().getLabels() : Collections.emptyMap();
                return "strimzi".equals(labels.get("app"));
            })
            .collect(Collectors.toList());
        assertThat(strimziClusterRoles, is(Collections.emptyList()));
    }

    private void prepareEnvironment(ExtensionContext extensionContext) {
        install.prepareEnvForOperator(extensionContext, NAMESPACE);
        SetupClusterOperator.applyBindings(extensionContext, NAMESPACE);
        // 060-Deployment
        resourceManager.createResource(extensionContext,
            new BundleResource.BundleResourceBuilder()
                .withName(Constants.STRIMZI_DEPLOYMENT_NAME)
                .withNamespace(NAMESPACE)
                .buildBundleInstance()
                .buildBundleDeployment()
                .build());
    }
}
