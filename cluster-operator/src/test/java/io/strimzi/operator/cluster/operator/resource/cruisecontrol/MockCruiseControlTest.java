/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.vertx.core.Future;
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class MockCruiseControlTest {

    private static final int PORT = 1080;
    private static final String HOST = "localhost";

    private static ClientAndServer ccServer;

    @BeforeAll
    public static void startUp() throws IOException, URISyntaxException {
        ccServer = MockCruiseControl.server(PORT);
    }

    @BeforeEach
    public void resetServer() {
        ccServer.reset();
    }

    @AfterAll
    public static void stop() {
        ccServer.stop();
    }

    
    private CruiseControlApi cruiseControlClientProvider(Vertx vertx) {
        return new CruiseControlApiImpl(vertx, MockCruiseControl.CC_SECRET, MockCruiseControl.CO_SECRET, true, true);
    }

    private void runTest(Vertx vertx, VertxTestContext context, String userTaskID, int pendingCalls) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, 0, pendingCalls);

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Future<CruiseControlResponse> statusFuture = client.getUserTaskStatus(HOST, PORT, userTaskID);

        // One interaction is always expected at the end of the test, hence the +1
        Checkpoint expectedInteractions = context.checkpoint(pendingCalls + 1);

        for (int i = 1; i <= pendingCalls; i++) {
            statusFuture = statusFuture.compose(response -> {
                context.verify(() -> assertThat(
                        response.getJson().getString("Status"),
                        is(CruiseControlUserTaskStatus.IN_EXECUTION.toString()))
                );
                expectedInteractions.flag();
                return client.getUserTaskStatus(HOST, PORT, userTaskID);
            });
        }

        statusFuture.compose(response -> {
            context.verify(() -> assertThat(
                    response.getJson().getString("Status"),
                    is(CruiseControlUserTaskStatus.COMPLETED.toString()))
            );
            expectedInteractions.flag();
            return Future.succeededFuture(response);
        });
    }

    @Test
    public void testCCUserTaskNoDelay(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        runTest(vertx, context, MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, 0);
    }

    @Test
    public void testCCUserTaskNoDelayVerbose(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        runTest(vertx, context, MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, 0);
    }

    @Test
    public void testCCUserTaskDelay(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        runTest(vertx, context, MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, 3);
    }

    @Test
    public void testCCUserTaskDelayVerbose(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        runTest(vertx, context, MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, 3);
    }

    @Test
    public void testMockCCServerPendingCallsOverride(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        CruiseControlApi client = cruiseControlClientProvider(vertx);
        String userTaskID = MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID;

        int pendingCalls1 = 2;
        Checkpoint firstPending = context.checkpoint(pendingCalls1);
        int pendingCalls2 = 4;
        Checkpoint secondPending = context.checkpoint(pendingCalls2);

        //When last checkpoint is flagged, then test is marked as completed, so create another checkpoint
        //for test end to prevent premature test success
        Checkpoint completeTest = context.checkpoint();

        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, 0, pendingCalls1);

        Future<CruiseControlResponse> statusFuture = client.getUserTaskStatus(HOST, PORT, userTaskID);

        for (int i = 1; i <= pendingCalls1; i++) {
            statusFuture = statusFuture.compose(response -> {
                context.verify(() -> assertThat(
                        response.getJson().getString("Status"),
                        is(CruiseControlUserTaskStatus.IN_EXECUTION.toString()))
                );
                firstPending.flag();
                return client.getUserTaskStatus(HOST, PORT, userTaskID);
            });
        }

        statusFuture = statusFuture.compose(response -> {
            context.verify(() -> assertThat(
                    response.getJson().getString("Status"),
                    is(CruiseControlUserTaskStatus.COMPLETED.toString()))
            );
            return Future.succeededFuture(response);
        });

        statusFuture = statusFuture.compose(response -> {
            try {
                ccServer.reset();
                MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, 0, pendingCalls2);
            } catch (IOException e) {
                return Future.failedFuture(e);
            } catch (URISyntaxException e) {
                return Future.failedFuture(e);
            }
            return Future.succeededFuture();
        });

        statusFuture = statusFuture.compose(ignore -> client.getUserTaskStatus(HOST, PORT, userTaskID));

        for (int i = 1; i <= pendingCalls2; i++) {
            statusFuture = statusFuture.compose(response -> {
                context.verify(() -> assertThat(
                        response.getJson().getString("Status"),
                        is(CruiseControlUserTaskStatus.IN_EXECUTION.toString()))
                );
                secondPending.flag();
                return client.getUserTaskStatus(HOST, PORT, userTaskID);
            });
        }

        statusFuture.compose(response -> {
            context.verify(() -> assertThat(
                    response.getJson().getString("Status"),
                    is(CruiseControlUserTaskStatus.COMPLETED.toString()))
            );
            return Future.succeededFuture(response);
        }).onComplete(context.succeeding(result -> {
            completeTest.flag();
        }));
    }

}
