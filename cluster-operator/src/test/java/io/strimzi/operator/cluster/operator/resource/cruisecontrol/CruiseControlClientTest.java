/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
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

import static io.strimzi.operator.cluster.JSONObjectMatchers.hasEntry;
import static io.strimzi.operator.cluster.JSONObjectMatchers.hasKeys;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

@ExtendWith(VertxExtension.class)
public class CruiseControlClientTest {

    private static final int PORT = 1080;
    private static final String HOST = "localhost";

    private static final boolean API_AUTHORIZATION_ENABLED = true;
    private static final boolean API_AUTHENTICATION_ENABLED = true;

    private static ClientAndServer ccServer;

    @BeforeAll
    public static void setupServer() throws IOException {
        ccServer = MockCruiseControl.server(PORT);
    }

    @AfterAll
    public static void stopServer() {
        ccServer.stop();
    }

    @BeforeEach
    public void resetServer() {
        ccServer.reset();
    }

    private CruiseControlApi cruiseControlClientProvider(Vertx vertx) {
        return new CruiseControlApiImpl(vertx, MockCruiseControl.CC_SECRET, MockCruiseControl.CO_SECRET, API_AUTHORIZATION_ENABLED, API_AUTHENTICATION_ENABLED);
    }

    @Test
    public void testGetCCState(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCStateResponse(ccServer);

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Checkpoint checkpoint = context.checkpoint();
        client.getCruiseControlState(HOST, PORT, false)
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat(result.getJson().getJsonObject("ExecutorState"),
                        hasEntry("state", "NO_TASK_IN_PROGRESS"));
                checkpoint.flag();
            })));
    }

    @Test
    public void testCCRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);

        RebalanceOptions rbOptions = new RebalanceOptions.RebalanceOptionsBuilder().build();

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Checkpoint checkpoint = context.checkpoint();
        client.rebalance(HOST, PORT, rbOptions, null)
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID));
                assertThat(result.getJson(), hasKeys("summary", "goalSummary", "loadAfterOptimization"));
                checkpoint.flag();
            })));
    }

    @Test
    public void testCCRebalanceVerbose(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);

        RebalanceOptions rbOptions = new RebalanceOptions.RebalanceOptionsBuilder().withVerboseResponse().build();

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Checkpoint checkpoint = context.checkpoint();
        client.rebalance(HOST, PORT, rbOptions, null)
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID));
                assertThat(result.getJson(), hasKeys("summary", "goalSummary", "proposals", "loadAfterOptimization", "loadBeforeOptimization"));
                checkpoint.flag();
            })));
    }

    @Test
    public void testCCRebalanceNotEnoughValidWindowsException(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceNotEnoughDataError(ccServer);

        RebalanceOptions rbOptions = new RebalanceOptions.RebalanceOptionsBuilder().build();

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Checkpoint checkpoint = context.checkpoint();
        client.rebalance(HOST, PORT, rbOptions, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                .onComplete(context.succeeding(result -> {
                    context.verify(() -> assertThat(result.isNotEnoughDataForProposal(), is(true)));
                    checkpoint.flag();
                }));
    }

    @Test
    public void testCCRebalancePropsosalNotReady(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 1);

        RebalanceOptions rbOptions = new RebalanceOptions.RebalanceOptionsBuilder().build();

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Checkpoint checkpoint = context.checkpoint();
        client.rebalance(HOST, PORT, rbOptions, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
            .onComplete(context.succeeding(result -> {
                context.verify(() -> assertThat(result.isProposalStillCalaculating(), is(true)));
                checkpoint.flag();
            }));
    }

    @Test
    public void testCCGetRebalanceUserTask(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, 0, 0);

        CruiseControlApi client = cruiseControlClientProvider(vertx);
        String userTaskID = MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID;

        Checkpoint checkpoint = context.checkpoint();
        client.getUserTaskStatus(HOST, PORT, userTaskID).onComplete(context.succeeding(result -> {
            context.verify(() -> assertThat(result.getUserTaskId(), is(MockCruiseControl.USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID)));
            context.verify(() -> assertThat(result.getJson().getJsonObject(CruiseControlRebalanceKeys.SUMMARY.getKey()), is(notNullValue())));
            checkpoint.flag();
        }));
    }
}
