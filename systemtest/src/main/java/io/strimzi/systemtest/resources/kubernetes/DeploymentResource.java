/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.TestUtils;


public class DeploymentResource implements ResourceType<Deployment> {

    @Override
    public String getKind() {
        return Constants.DEPLOYMENT;
    }
    @Override
    public Deployment get(String namespace, String name) {
        return ResourceManager.kubeClient().namespace(namespace).getDeployment(name);
    }
    @Override
    public void create(Deployment resource) {
        ResourceManager.kubeClient().createOrReplaceDeployment(resource);
    }
    @Override
    public void delete(Deployment resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).deleteDeployment(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
    }

    @Override
    public boolean waitForReadiness(Deployment resource) {
        return DeploymentUtils.waitForDeploymentAndPodsReady(resource.getMetadata().getNamespace(), resource.getMetadata().getName(), resource.getSpec().getReplicas());
    }

    public static Deployment getDeploymentFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, Deployment.class);
    }
}
