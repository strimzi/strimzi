/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaRebalanceList;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceState;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;

import java.util.function.Consumer;


public class KafkaRebalanceResource implements ResourceType<KafkaRebalance> {

    public KafkaRebalanceResource() {}

    @Override
    public String getKind() {
        return KafkaRebalance.RESOURCE_KIND;
    }
    @Override
    public KafkaRebalance get(String namespace, String name) {
        return kafkaRebalanceClient().inNamespace(namespace).withName(name).get();
    }
    @Override
    public void create(KafkaRebalance resource) {
        kafkaRebalanceClient().inNamespace(resource.getMetadata().getNamespace()).createOrReplace(resource);
    }
    @Override
    public void delete(KafkaRebalance resource) {
        kafkaRebalanceClient().inNamespace(resource.getMetadata().getNamespace()).withName(
            resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }
    @Override
    public boolean waitForReadiness(KafkaRebalance resource) {
        return KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(resource.getMetadata().getNamespace(), resource.getMetadata().getName(), KafkaRebalanceState.PendingProposal);
    }

    public static MixedOperation<KafkaRebalance, KafkaRebalanceList, Resource<KafkaRebalance>> kafkaRebalanceClient() {
        return Crds.kafkaRebalanceOperation(ResourceManager.kubeClient().getClient());
    }

    public static void replaceKafkaRebalanceResourceInSpecificNamespace(String resourceName, Consumer<KafkaRebalance> editor, String namespaceName) {
        ResourceManager.replaceCrdResource(KafkaRebalance.class, KafkaRebalanceList.class, resourceName, editor, namespaceName);
    }
}
