/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cluster.KubeCluster;
import io.strimzi.test.k8s.exceptions.NoClusterException;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
@ExtendWith(VertxExtension.class)
public class KafkaConnectCrdOperatorIT {
    protected static final Logger log = LogManager.getLogger(KafkaConnectCrdOperatorIT.class);

    public static final String RESOURCE_NAME = "my-test-resource";
    protected static Vertx vertx;
    protected static KubernetesClient client;
    protected static CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList, DoneableKafkaConnect> kafkaConnectOperator;
    protected static String namespace = "connect-crd-it-namespace";

    private static KubeClusterResource cluster;

    @BeforeAll
    public static void before() {
        cluster = KubeClusterResource.getInstance();
        cluster.setTestNamespace(namespace);

        try {
            KubeCluster.bootstrap();
        } catch (NoClusterException e) {
            assumeTrue(false, e.getMessage());
        }
        vertx = Vertx.vertx();
        client = new DefaultKubernetesClient();
        kafkaConnectOperator = new CrdOperator(vertx, client, KafkaConnect.class, KafkaConnectList.class, DoneableKafkaConnect.class);

        log.info("Preparing namespace");
        if (cluster.getTestNamespace() != null && System.getenv("SKIP_TEARDOWN") == null) {
            log.warn("Namespace {} is already created, going to delete it", namespace);
            kubeClient().deleteNamespace(namespace);
            cmdKubeClient().waitForResourceDeletion("Namespace", namespace);
        }

        log.info("Creating namespace: {}", namespace);
        kubeClient().createNamespace(namespace);
        cmdKubeClient().waitForResourceCreation("Namespace", namespace);

        log.info("Creating CRD");
        client.customResourceDefinitions().create(Crds.kafkaConnect());
        log.info("Created CRD");
    }

    @AfterAll
    public static void after() {
        if (client != null) {
            log.info("Deleting CRD");
            client.customResourceDefinitions().delete(Crds.kafkaConnect());
        }
        if (kubeClient().getNamespace(namespace) != null && System.getenv("SKIP_TEARDOWN") == null) {
            log.warn("Deleting namespace {} after tests run", namespace);
            kubeClient().deleteNamespace(namespace);
            cmdKubeClient().waitForResourceDeletion("Namespace", namespace);
        }

        if (vertx != null) {
            vertx.close();
        }
    }

    protected KafkaConnect getResource() {
        return new KafkaConnectBuilder()
                .withApiVersion(KafkaConnect.RESOURCE_GROUP + "/" + KafkaConnect.V1BETA1)
                .withNewMetadata()
                .withName(RESOURCE_NAME)
                .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                .endStatus()
                .build();
    }

    @Test
    public void testUpdateStatus(VertxTestContext context) {
        Checkpoint async = context.checkpoint();

        log.info("Getting Kubernetes version");
        PlatformFeaturesAvailability.create(vertx, client)
            .setHandler(context.succeeding(pfa -> context.verify(() -> {
                assertThat("Kubernetes version : " + pfa.getKubernetesVersion() + " is too old",
                        pfa.getKubernetesVersion().compareTo(KubernetesVersion.V1_11), is(not(lessThan(0))));
            })))

            .compose(pfa -> {
                log.info("Creating resource");
                return kafkaConnectOperator.reconcile(namespace, RESOURCE_NAME, getResource());
            })
            .setHandler(context.succeeding())

            .compose(rrCreated -> {
                KafkaConnect newStatus = new KafkaConnectBuilder(kafkaConnectOperator.get(namespace, RESOURCE_NAME))
                        .withNewStatus()
                        .withConditions(new ConditionBuilder()
                                .withType("Ready")
                                .withStatus("True")
                                .build())
                        .endStatus()
                        .build();

                log.info("Updating resource status");
                return kafkaConnectOperator.updateStatusAsync(newStatus);
            })
            .setHandler(context.succeeding())

            .compose(v -> kafkaConnectOperator.getAsync(namespace, RESOURCE_NAME))
            .setHandler(context.succeeding(updatedKafkaConnect -> context.verify(() -> {

                assertThat(updatedKafkaConnect.getStatus().getConditions().get(0), is(new ConditionBuilder()
                        .withType("Ready")
                        .withStatus("True")
                        .build()));
            })))

            .compose(rr -> {
                log.info("Deleting resource");
                return kafkaConnectOperator.reconcile(namespace, RESOURCE_NAME, null);
            })
            .setHandler(context.succeeding(rr -> async.flag()));
    }

    /**
     * Tests what happens when the resource is deleted while updating the status
     *
     * @param context
     */
    @Test
    public void testUpdateStatusWhileResourceDeletedThrowsNullPointerException(VertxTestContext context) {
        Checkpoint async = context.checkpoint();

        log.info("Getting Kubernetes version");
        PlatformFeaturesAvailability.create(vertx, client)
            .setHandler(context.succeeding(pfa -> context.verify(() -> {
                assertThat("Kubernetes version : " + pfa.getKubernetesVersion() + " is too old",
                        pfa.getKubernetesVersion().compareTo(KubernetesVersion.V1_11), is(not(lessThan(0))));
            })))
            .compose(pfa -> {
                log.info("Creating resource");
                return kafkaConnectOperator.reconcile(namespace, RESOURCE_NAME, getResource());
            })
            .setHandler(context.succeeding())

            .compose(rr -> {
                log.info("Deleting resource");
                return kafkaConnectOperator.reconcile(namespace, RESOURCE_NAME, null);
            })
            .setHandler(context.succeeding())

            .compose(v -> {
                KafkaConnect newStatus = new KafkaConnectBuilder(kafkaConnectOperator.get(namespace, RESOURCE_NAME))
                        .withNewStatus()
                        .withConditions(new ConditionBuilder()
                                .withType("Ready")
                                .withStatus("True")
                                .build())
                        .endStatus()
                        .build();

                log.info("Updating resource status");
                return kafkaConnectOperator.updateStatusAsync(newStatus);
            })
            .setHandler(context.failing(e -> context.verify(() -> {
                assertThat(e, instanceOf(NullPointerException.class));
                async.flag();
            })));
    }

    /**
     * Tests what happens when the resource is modifed while updating the status
     *
     * @param context
     */
    @Test
    public void testUpdateStatusThrowsKubernetesExceptionIfResourceUpdatedPrior(VertxTestContext context) {
        Checkpoint async = context.checkpoint();

        Promise updateFailed = Promise.promise();

        log.info("Getting Kubernetes version");
        PlatformFeaturesAvailability.create(vertx, client)
            .setHandler(context.succeeding(pfa -> context.verify(() -> {
                assertThat("Kubernetes version : " + pfa.getKubernetesVersion() + " is too old",
                        pfa.getKubernetesVersion().compareTo(KubernetesVersion.V1_11), is(not(lessThan(0))));
            })))
            .compose(pfa -> {
                log.info("Creating resource");
                return kafkaConnectOperator.reconcile(namespace, RESOURCE_NAME, getResource());
            })
            .setHandler(context.succeeding())
            .compose(rr -> {

                KafkaConnect currentKafkaConnect = kafkaConnectOperator.get(namespace, RESOURCE_NAME);

                KafkaConnect updated = new KafkaConnectBuilder(currentKafkaConnect)
                        .editSpec()
                            .withNewLivenessProbe()
                                .withInitialDelaySeconds(14)
                            .endLivenessProbe()
                        .endSpec()
                        .build();

                KafkaConnect newStatus = new KafkaConnectBuilder(currentKafkaConnect)
                        .withNewStatus()
                            .withConditions(new ConditionBuilder()
                                    .withType("Ready")
                                    .withStatus("True")
                                    .build())
                        .endStatus()
                        .build();

                log.info("Updating resource (mocking an update due to some other reason)");
                kafkaConnectOperator.operation().inNamespace(namespace).withName(RESOURCE_NAME).patch(updated);

                log.info("Updating resource status after underlying resource has changed");
                return kafkaConnectOperator.updateStatusAsync(newStatus);
            })
            .setHandler(context.failing(e -> context.verify(() -> {
                assertThat("Exception was not KubernetesClientException, it was : " + e.toString(),
                        e, instanceOf(KubernetesClientException.class));
                updateFailed.complete();
            })));

        updateFailed.future().compose(v -> {
            log.info("Deleting resource");
            return kafkaConnectOperator.reconcile(namespace, RESOURCE_NAME, null);
        })
        .setHandler(context.succeeding(v -> async.flag()));
    }
}

