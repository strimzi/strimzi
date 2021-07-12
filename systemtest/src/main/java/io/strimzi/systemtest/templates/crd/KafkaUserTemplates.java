/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.KafkaUserList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.resources.ResourceManager;

public class KafkaUserTemplates {

    private KafkaUserTemplates() {}


    public static MixedOperation<KafkaUser, KafkaUserList, Resource<KafkaUser>> kafkaUserClient() {
        return Crds.kafkaUserOperation(ResourceManager.kubeClient().getClient());
    }

    public static MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaClient() {
        return Crds.kafkaOperation(ResourceManager.kubeClient().getClient());
    }

    public static KafkaUserBuilder tlsUser(String namespaceName, String clusterName, String name) {
        return defaultUser(namespaceName, clusterName, name)
            .withNewSpec()
                .withNewKafkaUserTlsClientAuthentication()
                .endKafkaUserTlsClientAuthentication()
            .endSpec();
    }

    public static KafkaUserBuilder tlsUser(String clusterName, String name) {
        return tlsUser(ResourceManager.kubeClient().getNamespace(), clusterName, name);
    }

    public static KafkaUserBuilder scramShaUser(String namespaceName, String clusterName, String name) {
        return defaultUser(namespaceName, clusterName, name)
            .withNewSpec()
                .withNewKafkaUserScramSha512ClientAuthentication()
                .endKafkaUserScramSha512ClientAuthentication()
            .endSpec();
    }

    public static KafkaUserBuilder scramShaUser(String clusterName, String name) {
        return scramShaUser(ResourceManager.kubeClient().getNamespace(), clusterName, name);
    }

    public static KafkaUserBuilder defaultUser(String namespaceName, String clusterName, String name) {
        return new KafkaUserBuilder()
            .withNewMetadata()
                .withClusterName(clusterName)
                .withName(name)
                .withNamespace(namespaceName)
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, clusterName)
            .endMetadata();
    }

    public static KafkaUserBuilder defaultUser(String clusterName, String name) {
        return defaultUser(ResourceManager.kubeClient().getNamespace(), clusterName, name);
    }

    public static KafkaUser kafkaUserWithoutWait(KafkaUser user) {
        kafkaUserClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(user);
        return user;
    }

    public static KafkaUserBuilder userWithQuotas(KafkaUser user, Integer prodRate, Integer consRate, Integer requestPerc, Double mutRate) {
        return new KafkaUserBuilder(user)
                .editSpec()
                    .withNewQuotas()
                        .withConsumerByteRate(consRate)
                        .withProducerByteRate(prodRate)
                        .withRequestPercentage(requestPerc)
                        .withControllerMutationRate(mutRate)
                    .endQuotas()
                .endSpec();
    }
}
