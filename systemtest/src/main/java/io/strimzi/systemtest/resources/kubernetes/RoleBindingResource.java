/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RoleBindingResource implements ResourceType<RoleBinding> {

    private static final Logger LOGGER = LogManager.getLogger(RoleBindingResource.class);

    @Override
    public String getKind() {
        return Constants.ROLE_BINDING;
    }
    @Override
    public RoleBinding get(String namespace, String name) {
        return ResourceManager.kubeClient().namespace(namespace).getRoleBinding(name);
    }
    @Override
    public void create(RoleBinding resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).createOrReplaceRoleBinding(resource);
    }
    @Override
    public void delete(RoleBinding resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).deleteRoleBinding(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
    }
    @Override
    public boolean waitForReadiness(RoleBinding resource) {
        return resource != null;
    }

    public static RoleBinding roleBinding(String yamlPath, String namespace, String clientNamespace) {
        LOGGER.info("Creating RoleBinding from {} in namespace {}", yamlPath, namespace);
        RoleBinding roleBinding = getRoleBindingFromYaml(yamlPath);
        if (Environment.isNamespaceRbacScope()) {
            LOGGER.info("Replacing ClusterRole RoleRef for Role RoleRef");
            roleBinding.getRoleRef().setKind(Constants.ROLE);
        }

        return createRoleBinding(
            new RoleBindingBuilder(roleBinding)
                .editFirstSubject()
                    .withNamespace(namespace)
                .endSubject()
                .build(),
            clientNamespace);
    }

    public static RoleBinding createRoleBinding(RoleBinding roleBinding, String clientNamespace) {
        ResourceManager.kubeClient().namespace(clientNamespace).createOrReplaceRoleBinding(roleBinding);
        return roleBinding;
    }

    private static RoleBinding getRoleBindingFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, RoleBinding.class);
    }
}
