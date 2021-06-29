/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.KafkaRebalanceList;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.KafkaRebalanceSpec;
import io.strimzi.api.kafka.model.KafkaRebalanceSpecBuilder;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceState;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.KafkaRebalanceStatus;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApi;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApiImpl;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.MockCruiseControl;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.RebalanceOptions;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.integration.ClientAndServer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaRebalanceStateMachineTest {

    private static final String HOST = "localhost";
    private static final String RESOURCE_NAME = "my-rebalance";
    private static final String CLUSTER_NAMESPACE = "cruise-control-namespace";
    private static final String CLUSTER_NAME = "kafka-cruise-control-test-cluster";
    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_18;

    private static ClientAndServer ccServer;

    @BeforeAll
    public static void before() throws IOException, URISyntaxException {
        ccServer = MockCruiseControl.server(CruiseControl.REST_API_PORT);
    }

    @AfterAll
    public static void after() {
        ccServer.stop();
    }

    @BeforeEach
    public void resetServer() {
        ccServer.reset();
    }

    /**
     * Creates an example {@link KafkaRebalanceBuilder} instance using the supplied state parameters.
     *
     * @param currentState The current state of the resource before being passed to computeNextStatus.
     * @param currentStatusSessionID The user task ID attached to the current KafkaRebalance resource. Can be null.
     * @param userAnnotation An annotation to be applied after the reconcile has started, for example "approve" or "stop".
     * @param rebalanceSpec A custom rebalance specification. If null a blank spec will be used.
     * @return A KafkaRebalance instance configured with the supplied parameters.
     */
    private KafkaRebalance createKafkaRebalance(KafkaRebalanceState currentState,
                                                String currentStatusSessionID,
                                                String userAnnotation,
                                                KafkaRebalanceSpec rebalanceSpec) {

        KafkaRebalanceBuilder kafkaRebalanceBuilder =
                new KafkaRebalanceBuilder()
                        .editOrNewMetadata()
                            .withName(RESOURCE_NAME)
                            .withNamespace(CLUSTER_NAMESPACE)
                            .withLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                            .withAnnotations(Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_REBALANCE, userAnnotation == null ? "none" : userAnnotation))
                        .endMetadata()
                        .withSpec(rebalanceSpec);

        // there is no actual status and related condition when a KafkaRebalance is just created
        if (currentState != KafkaRebalanceState.New) {
            Condition currentRebalanceCondition = new Condition();
            currentRebalanceCondition.setType(currentState.toString());
            currentRebalanceCondition.setStatus("True");

            KafkaRebalanceStatus currentStatus = new KafkaRebalanceStatus();
            currentStatus.setConditions(Collections.singletonList(currentRebalanceCondition));
            currentStatus.setSessionId(currentStatusSessionID);

            kafkaRebalanceBuilder.withStatus(currentStatus);
        }
        return kafkaRebalanceBuilder.build();
    }

    /**
     *  Checks the expected transition between two states of the Kafka Rebalance operator.
     *
     * @param vertx The vertx test instance.
     * @param context The test context instance.
     * @param currentState The current state of the resource before being passed to computeNextStatus.
     * @param nextState The expected state of the resouce after computeNextStatus has been called.
     * @param initialAnnotation The initial annotation attached to the Kafka Rebalance resource. For example none or refresh.
     * @param kcRebalance The Kafka Rebalance instance that will be returned by the resourceSupplier.
     * @return A future for the {@link KafkaRebalanceStatus} returned by the {@link KafkaRebalanceAssemblyOperator#computeNextStatus} method
     */
    private Future<KafkaRebalanceStatus> checkTransition(Vertx vertx, VertxTestContext context,
                                                         KafkaRebalanceState currentState,
                                                         KafkaRebalanceState nextState,
                                                         KafkaRebalanceAnnotation initialAnnotation,
                                                         KafkaRebalance kcRebalance) {

        CruiseControlApi client = new CruiseControlApiImpl(vertx, MockCruiseControl.CC_SECRET, MockCruiseControl.CO_SECRET, true, true);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaRebalanceAssemblyOperator kcrao = new KafkaRebalanceAssemblyOperator(vertx, pfa, supplier, ResourceUtils.dummyClusterOperatorConfig()) {
            @Override
            public String cruiseControlHost(String clusterName, String clusterNamespace) {
                return HOST;
            }
        };

        Reconciliation recon = new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME);

        RebalanceOptions.RebalanceOptionsBuilder rbOptions = new RebalanceOptions.RebalanceOptionsBuilder();

        if (kcRebalance.getSpec() != null) {
            if (kcRebalance.getSpec().getGoals() != null) {
                rbOptions.withGoals(kcRebalance.getSpec().getGoals());
            }

            if (kcRebalance.getSpec().isSkipHardGoalCheck()) {
                rbOptions.withSkipHardGoalCheck();
            }
        }

        CrdOperator<KubernetesClient,
                KafkaRebalance,
                KafkaRebalanceList> mockRebalanceOps = supplier.kafkaRebalanceOperator;

        when(mockCmOps.getAsync(CLUSTER_NAMESPACE, RESOURCE_NAME)).thenReturn(Future.succeededFuture(new ConfigMap()));
        when(mockRebalanceOps.get(CLUSTER_NAMESPACE, RESOURCE_NAME)).thenReturn(kcRebalance);
        when(mockRebalanceOps.getAsync(CLUSTER_NAMESPACE, RESOURCE_NAME)).thenReturn(Future.succeededFuture(kcRebalance));

        return kcrao.computeNextStatus(recon, HOST, client, kcRebalance, currentState, initialAnnotation, rbOptions)
                .compose(result -> {
                    context.verify(() -> {
                        assertThat(result.getStatus().getConditions(), StateMatchers.hasStateInConditions(nextState));
                    });
                    return Future.succeededFuture(result.getStatus());
                });
    }

    /**
     *  Checks the expected transition between two states of the Kafka Rebalance operator.
     *
     * @param vertx The vertx test instance.
     * @param context The test context instance.
     * @param currentState The current state of the resource before being passed to computeNextStatus.
     * @param nextState The expected state of the resouce after computeNextStatus has been called.
     * @param initialAnnotation The initial annotation attached to the Kafka Rebalance resource. For example none or refresh.
     * @param userAnnotation An annotation to be applied after the reconcile has started, for example "approve" or "stop".
     * @param currentStatusSessionID The user task ID attached to the current KafkaRebalance resource. Can be null.
     * @return A future for the {@link KafkaRebalanceStatus} returned by the {@link KafkaRebalanceAssemblyOperator#computeNextStatus} method
     */
    private Future<KafkaRebalanceStatus> checkTransition(Vertx vertx, VertxTestContext context,
                                                         KafkaRebalanceState currentState,
                                                         KafkaRebalanceState nextState,
                                                         KafkaRebalanceAnnotation initialAnnotation,
                                                         String userAnnotation, String currentStatusSessionID) {

        KafkaRebalance kcRebalance = createKafkaRebalance(currentState, currentStatusSessionID, userAnnotation, null);

        return checkTransition(vertx, context, currentState, nextState, initialAnnotation, kcRebalance);

    }

    private static void defaultStatusHandler(AsyncResult<KafkaRebalanceStatus> result, VertxTestContext context) {
        if (result.succeeded()) {
            context.completeNow();
        } else {
            context.failNow(result.cause());
        }
    }

    private static void checkOptimizationResults(AsyncResult<KafkaRebalanceStatus> result, VertxTestContext context, boolean shouldBeEmpty) {
        if (result.succeeded()) {
            assertEquals(shouldBeEmpty, result.result().getOptimizationResult().isEmpty());
            context.completeNow();
        } else {
            context.failNow(result.cause());
        }
    }

    @Test
    public void testNewToProposalReady(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);
        checkTransition(vertx, context,
                KafkaRebalanceState.New, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.none, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, false));

    }

    @Test
    public void testNewWithNotEnoughData(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        // Test the case where the user asks for a rebalance but there is not enough data, the returned status should
        // not contain an optimisation result
        MockCruiseControl.setupCCRebalanceNotEnoughDataError(ccServer);
        checkTransition(vertx, context,
                KafkaRebalanceState.New, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.none, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, true));

    }

    @Test
    public void testNewToProposalPending(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 1);
        checkTransition(vertx, context,
                KafkaRebalanceState.New, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.none, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, true));

    }

    @Test
    public void testNewBadGoalsError(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        // Test the case where the user asks for a rebalance with custom goals which do not contain all the configured hard goals
        // In this case the computeNextStatus error will return a failed future with a message containing an illegal argument exception
        MockCruiseControl.setupCCRebalanceBadGoalsError(ccServer);

        List<String> customGoals = new ArrayList<>();
        customGoals.add("Goal.one");
        customGoals.add("Goal.two");
        customGoals.add("Goal.three");

        KafkaRebalanceSpec rebalanceSpec = new KafkaRebalanceSpecBuilder().addAllToGoals(customGoals).build();

        KafkaRebalance kcRebalance = createKafkaRebalance(
                KafkaRebalanceState.New, null, null, rebalanceSpec);

        checkTransition(vertx, context,
                KafkaRebalanceState.New, KafkaRebalanceState.NotReady,
                KafkaRebalanceAnnotation.none, kcRebalance)
                .onComplete(result -> {
                    if (result.failed()) {
                        if (result.cause().getMessage().contains("java.lang.IllegalArgumentException: Missing hard goals")) {
                            context.completeNow();
                        } else {
                            context.failNow(new RuntimeException("This operation failed with an unexpected error:" + result.cause().getMessage()));
                        }
                    }
                    context.failNow(new RuntimeException("This operations should have failed"));
                });

    }

    @Test
    public void testNewBadGoalsErrorWithSkipHGCheck(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        // Test the case where the user asks for a rebalance with custom goals which do not contain all the configured hard goals
        // But we have set skip hard goals check to true
        MockCruiseControl.setupCCRebalanceBadGoalsError(ccServer);

        List<String> customGoals = new ArrayList<>();
        customGoals.add("Goal.one");
        customGoals.add("Goal.two");
        customGoals.add("Goal.three");

        KafkaRebalanceSpec rebalanceSpec = new KafkaRebalanceSpecBuilder().addAllToGoals(customGoals).withSkipHardGoalCheck(true).build();

        KafkaRebalance kcRebalance = createKafkaRebalance(
                KafkaRebalanceState.New, null, null, rebalanceSpec);

        checkTransition(vertx, context,
                KafkaRebalanceState.New, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.none, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));

    }

    @Test
    public void testProposalPendingToProposalReady(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);
        checkTransition(vertx, context,
                KafkaRebalanceState.PendingProposal, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.none, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, false));

    }

    @Test
    public void testProposalPendingToProposalReadyWithDelay(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 3);
        checkTransition(vertx, context,
                KafkaRebalanceState.PendingProposal, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.none, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, false));

    }

    @Test
    public void testProposalPendingToStopped(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 3);
        checkTransition(vertx, context,
                KafkaRebalanceState.PendingProposal, KafkaRebalanceState.Stopped,
                KafkaRebalanceAnnotation.none, "stop", null)
                .onComplete(result -> checkOptimizationResults(result, context, true));

    }

    @Test
    public void testProposalReadyNoChange(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);
        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.none, null, null)
                .onComplete(result -> defaultStatusHandler(result, context));

    }

    @Test
    public void testProposalReadyToRebalancingWithNotEnoughData(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceNotEnoughDataError(ccServer);
        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.approve, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, true));

    }

    @Test
    public void testProposalReadyToRebalancingWithPendingSummary(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 1);
        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.Rebalancing,
                KafkaRebalanceAnnotation.approve, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, true));

    }

    @Test
    public void testProposalReadyToRebalancing(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);
        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.Rebalancing,
                KafkaRebalanceAnnotation.approve, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, false));

    }

    @Test
    public void testProposalReadyRefreshNoChange(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);
        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.refresh, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, false));

    }

    @Test
    public void testProposalReadyRefreshToPendingProposal(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 1);
        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.refresh, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, true));

    }

    @Test
    public void testProposalReadyRefreshToPendingProposalNotEnoughData(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceNotEnoughDataError(ccServer);
        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.refresh, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, true));

    }

    @Test
    public void testRebalancingCompleted(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, 0, 0);
        checkTransition(vertx, context,
                KafkaRebalanceState.Rebalancing, KafkaRebalanceState.Ready,
                KafkaRebalanceAnnotation.none, null,
                MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID)
                .onComplete(result -> checkOptimizationResults(result, context, false));

    }

    @Test
    public void testRebalancingPendingThenExecution(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        // This tests that the optimization proposal is added correctly if it was not ready when the rebalance(dryrun=false) was called.
        // The first poll should see active and then the second should see in execution and add the optimization and cancel the timer
        // so that the status is updated.
        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, 1, 1);
        checkTransition(vertx, context,
                KafkaRebalanceState.Rebalancing, KafkaRebalanceState.Rebalancing,
                KafkaRebalanceAnnotation.none, null,
                MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID)
                .onComplete(result -> checkOptimizationResults(result, context, false));

    }

    @Test
    public void testRebalancingToStopped(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, 0, 0);
        MockCruiseControl.setupCCStopResponse(ccServer);
        checkTransition(vertx, context,
                KafkaRebalanceState.Rebalancing, KafkaRebalanceState.Stopped,
                KafkaRebalanceAnnotation.none, "stop",
                MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID)
                .onComplete(result -> checkOptimizationResults(result, context, true));

    }

    @Test
    public void testRebalancingCompletedWithError(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCUserTasksCompletedWithError(ccServer);
        checkTransition(vertx, context,
                KafkaRebalanceState.Rebalancing, KafkaRebalanceState.NotReady,
                KafkaRebalanceAnnotation.none, null,
                MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID)
                .onComplete(result -> checkOptimizationResults(result, context, true));

    }

    @Test
    public void testStoppedRefreshToPendingProposal(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 1);
        checkTransition(vertx, context,
                KafkaRebalanceState.Stopped, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.refresh, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, true));

    }


    @Test
    public void testStoppedRefreshToProposalReady(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);
        checkTransition(vertx, context,
                KafkaRebalanceState.Stopped, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.refresh, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, false));

    }

    @Test
    public void testStoppedRefreshToPendingProposalNotEnoughData(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceNotEnoughDataError(ccServer);
        checkTransition(vertx, context,
                KafkaRebalanceState.Stopped, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.refresh, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, true));

    }

    @Test
    public void testReadyRefreshToPendingProposal(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 1);
        checkTransition(vertx, context,
                KafkaRebalanceState.Ready, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.refresh, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, true));

    }

    @Test
    public void testReadyRefreshToProposalReady(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);
        checkTransition(vertx, context,
                KafkaRebalanceState.Ready, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.refresh, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, false));

    }

    @Test
    public void testReadyRefreshToPendingProposalNotEnoughData(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceNotEnoughDataError(ccServer);
        checkTransition(vertx, context,
                KafkaRebalanceState.Ready, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.refresh, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, true));

    }

    @Test
    public void testNotReadyRefreshToPendingProposal(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 1);
        checkTransition(vertx, context,
                KafkaRebalanceState.NotReady, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.refresh, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, true));

    }

    @Test
    public void testNotReadyRefreshToProposalReady(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);
        checkTransition(vertx, context,
                KafkaRebalanceState.NotReady, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.refresh, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, false));

    }

    @Test
    public void testNotReadyRefreshToPendingProposalNotEnoughData(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceNotEnoughDataError(ccServer);
        checkTransition(vertx, context,
                KafkaRebalanceState.NotReady, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.refresh, null, null)
                .onComplete(result -> checkOptimizationResults(result, context, true));

    }

    private static class StateMatchers extends AbstractResourceStateMatchers {

    }
}