/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaMirrorMakerList;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.TestUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaMirrorMakerTemplates {

    private KafkaMirrorMakerTemplates() {}

    public static MixedOperation<KafkaMirrorMaker, KafkaMirrorMakerList, Resource<KafkaMirrorMaker>> kafkaMirrorMakerClient() {
        return Crds.mirrorMakerOperation(ResourceManager.kubeClient().getClient());
    }

    public static KafkaMirrorMakerBuilder kafkaMirrorMaker(String name, String sourceBootstrapServer, String targetBootstrapServer, String groupId, int mirrorMakerReplicas, boolean tlsListener) {
        KafkaMirrorMaker kafkaMirrorMaker = getKafkaMirrorMakerFromYaml(Constants.PATH_TO_KAFKA_MIRROR_MAKER_CONFIG);
        return defaultKafkaMirrorMaker(kafkaMirrorMaker, name, sourceBootstrapServer, targetBootstrapServer, groupId, mirrorMakerReplicas, tlsListener);
    }

    private static KafkaMirrorMakerBuilder defaultKafkaMirrorMaker(KafkaMirrorMaker kafkaMirrorMaker,
                                                                   String name,
                                                                   String sourceBootstrapServer,
                                                                   String targetBootstrapServer,
                                                                   String groupId,
                                                                   int kafkaMirrorMakerReplicas,
                                                                   boolean tlsListener) {
        return new KafkaMirrorMakerBuilder(kafkaMirrorMaker)
            .withNewMetadata()
                .withName(name)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
            .endMetadata()
            .editSpec()
                .withVersion(Environment.ST_KAFKA_VERSION)
                .withNewConsumer()
                    .withBootstrapServers(tlsListener ? KafkaResources.tlsBootstrapAddress(sourceBootstrapServer) : KafkaResources.plainBootstrapAddress(sourceBootstrapServer))
                    .withGroupId(groupId)
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
                .withNewProducer()
                    .withBootstrapServers(tlsListener ? KafkaResources.tlsBootstrapAddress(targetBootstrapServer) : KafkaResources.plainBootstrapAddress(targetBootstrapServer))
                    .addToConfig(ProducerConfig.ACKS_CONFIG, "all")
                .endProducer()
                .withReplicas(kafkaMirrorMakerReplicas)
                .withInclude(".*")
                .withNewInlineLogging()
                    .addToLoggers("mirrormaker.root.logger", "DEBUG")
                .endInlineLogging()
            .endSpec();
    }

    public static void deleteKafkaMirrorMakerWithoutWait(String resourceName) {
        kafkaMirrorMakerClient().inNamespace(ResourceManager.kubeClient().getNamespace()).withName(resourceName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    private static KafkaMirrorMaker getKafkaMirrorMakerFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaMirrorMaker.class);
    }
}
