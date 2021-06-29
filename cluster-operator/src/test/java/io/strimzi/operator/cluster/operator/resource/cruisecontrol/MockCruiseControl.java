/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.fabric8.kubernetes.api.model.HTTPHeader;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.MockCertManager;
import org.mockserver.configuration.ConfigurationProperties;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.Header;
import org.mockserver.model.JsonBody;
import org.mockserver.model.Parameter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockserver.configuration.ConfigurationProperties.javaLoggerLogLevel;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.Header.header;

public class MockCruiseControl {

    private static final String CC_JSON_ROOT = "io/strimzi/operator/cluster/operator/assembly/CruiseControlJSON/";

    private static final int RESPONSE_DELAY_SEC = 0;

    private static final String SEP = "-";
    private static final String REBALANCE = "rebalance";
    private static final String STATE = "state";
    private static final String NO_GOALS = "no-goals";
    private static final String VERBOSE = "verbose";
    private static final String USER_TASK = "user-task";
    private static final String RESPONSE = "response";
    
    public static final String REBALANCE_NO_GOALS_RESPONSE_UTID = REBALANCE + SEP + NO_GOALS + SEP + RESPONSE;
    public static final String REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID = REBALANCE + SEP + NO_GOALS + SEP + VERBOSE + SEP + RESPONSE;
    public static final String USER_TASK_REBALANCE_NO_GOALS = USER_TASK + SEP + REBALANCE + SEP + NO_GOALS;
    public static final String USER_TASK_REBALANCE_NO_GOALS_VERBOSE_UTID = USER_TASK_REBALANCE_NO_GOALS + SEP + VERBOSE;
    public static final String USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID = USER_TASK_REBALANCE_NO_GOALS + SEP + RESPONSE;
    public static final String USER_TASK_REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID = USER_TASK_REBALANCE_NO_GOALS_VERBOSE_UTID + SEP + RESPONSE;
    public static final String REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR = REBALANCE + SEP + "error";
    public static final String REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR_RESPONSE_UTID = REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR + SEP + RESPONSE;
    public static final String STATE_PROPOSAL_NOT_READY = STATE + SEP + "proposal" + SEP + "not" + SEP + "ready";
    public static final String STATE_PROPOSAL_NOT_READY_RESPONSE = STATE_PROPOSAL_NOT_READY + SEP + RESPONSE;

    private static final String CLUSTER = "my-cluster";
    private static final String NAMESPACE = "my-project";

    public static final Secret CO_SECRET = new SecretBuilder()
            .withNewMetadata()
                .withName(ClusterOperator.secretName(CLUSTER))
                .withNamespace(NAMESPACE)
            .endMetadata()
            .addToData("cluster-operator.p12", MockCertManager.clientsCaCertStore())
            .addToData("cluster-operator.password", MockCertManager.clientsCaCertStorePassword())
            .build();
    public static final Secret CC_SECRET = ResourceUtils.createSecret(CruiseControl.secretName(CLUSTER), NAMESPACE, CruiseControl.generateCruiseControlApiCredentials());
    private static final Header AUTH_HEADER = getAuthHeader();

    /**
     * Sets up and returns the Cruise Control MockSever.
     *
     * @param port The port number the MockServer instance should listen on
     * @return The configured ClientAndServer instance
     * @throws IOException If there are issues connecting to the network port
     */
    public static ClientAndServer server(int port) throws IOException {
        ConfigurationProperties.logLevel("WARN");
        String loggingConfiguration = "" +
                "handlers=org.mockserver.logging.StandardOutConsoleHandler\n" +
                "org.mockserver.logging.StandardOutConsoleHandler.level=WARNING\n" +
                "org.mockserver.logging.StandardOutConsoleHandler.formatter=java.util.logging.SimpleFormatter\n" +
                "java.util.logging.SimpleFormatter.format=%1$tF %1$tT  %3$s  %4$s  %5$s %6$s%n\n" +
                ".level=" + javaLoggerLogLevel() + "\n" +
                "io.netty.handler.ssl.SslHandler.level=WARNING";
        LogManager.getLogManager().readConfiguration(new ByteArrayInputStream(loggingConfiguration.getBytes(UTF_8)));

        return new ClientAndServer(port);
    }

    public static Header getAuthHeader() {
        String password = new String(Util.decodeFromSecret(CC_SECRET, CruiseControl.API_ADMIN_PASSWORD_KEY), StandardCharsets.US_ASCII);
        HTTPHeader header = CruiseControl.generateAuthHttpHeader(CruiseControl.API_ADMIN_NAME, password);
        return new Header(header.getName(), header.getValue());
    }

    private static JsonBody getJsonFromResource(String resource) throws URISyntaxException, IOException {

        URI jsonURI = Objects.requireNonNull(MockCruiseControl.class.getClassLoader().getResource(CC_JSON_ROOT + resource))
                .toURI();

        Optional<String> json = Files.lines(Paths.get(jsonURI), UTF_8)
                .reduce((x, y) -> x + y);

        if (!json.isPresent()) {
            throw new IOException("File " + resource + " from resources was empty");
        }

        return new JsonBody(json.get());
    }

    /**
     * Setup state responses in mock server.
     */
    public static void setupCCStateResponse(ClientAndServer ccServer) throws IOException, URISyntaxException {

        // Non-verbose response
        JsonBody jsonProposalNotReady = getJsonFromResource("CC-State-proposal-not-ready.json");

        ccServer
                .when(
                        request()
                                .withMethod("GET")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true|false"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.key, "true|false"))
                                .withPath(CruiseControlEndpoints.STATE.path)
                                .withHeaders(header(CruiseControlApi.CC_REST_API_USER_ID_HEADER, STATE_PROPOSAL_NOT_READY),
                                        AUTH_HEADER)
                                .withSecure(true))
                .respond(
                        response()
                                .withBody(jsonProposalNotReady)
                                .withHeaders(header("User-Task-ID", STATE_PROPOSAL_NOT_READY_RESPONSE))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));


        // Non-verbose response
        JsonBody json = getJsonFromResource("CC-State.json");

        ccServer
                .when(
                        request()
                                .withMethod("GET")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.key, "false"))
                                .withPath(CruiseControlEndpoints.STATE.path)
                                .withHeader(AUTH_HEADER)
                                .withSecure(true))
                .respond(
                        response()
                                .withBody(json)
                                .withHeaders(header("User-Task-ID", "cruise-control-state"))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));

        // Verbose response
        JsonBody jsonVerbose = getJsonFromResource("CC-State-verbose.json");

        ccServer
                .when(
                        request()
                                .withMethod("GET")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.key, "true"))
                                .withPath(CruiseControlEndpoints.STATE.path)
                                .withHeader(AUTH_HEADER)
                                .withSecure(true))
                .respond(
                        response()
                                .withBody(jsonVerbose)
                                .withHeaders(header("User-Task-ID", "cruise-control-state-verbose"))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));

    }

    /**
     * Setup NotEnoughValidWindows error rebalance response.
     */
    public static void setupCCRebalanceNotEnoughDataError(ClientAndServer ccServer) throws IOException, URISyntaxException {

        // Rebalance response with no goal that returns an error
        JsonBody jsonError = getJsonFromResource("CC-Rebalance-NotEnoughValidWindows-error.json");

        ccServer
                .when(
                        request()
                                .withMethod("POST")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.key, "true|false"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.key, "true|false"))
                                .withPath(CruiseControlEndpoints.REBALANCE.path)
                                .withHeader(AUTH_HEADER)
                                .withSecure(true))

                .respond(
                        response()
                                .withStatusCode(500)
                                .withBody(jsonError)
                                .withHeaders(header(CruiseControlApi.CC_REST_API_USER_ID_HEADER, REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));
    }

    /**
     * Setup rebalance response with no response delay (for quicker tests).
     */
    public static void setupCCRebalanceResponse(ClientAndServer ccServer, int pendingCalls) throws IOException, URISyntaxException {
        setupCCRebalanceResponse(ccServer, pendingCalls, RESPONSE_DELAY_SEC);
    }

    /**
     * Setup rebalance response.
     */
    public static void setupCCRebalanceResponse(ClientAndServer ccServer, int pendingCalls, int responseDelay) throws IOException, URISyntaxException {

        // Rebalance in progress response with no goals set - non-verbose
        JsonBody pendingJson = getJsonFromResource("CC-Rebalance-no-goals-in-progress.json");
        ccServer
                .when(
                        request()
                                .withMethod("POST")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.key, "true|false"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.key, "true|false"))
                                .withPath(CruiseControlEndpoints.REBALANCE.path)
                                .withHeader(AUTH_HEADER)
                                .withSecure(true),
                        Times.exactly(pendingCalls))
                .respond(
                        response()
                                .withBody(pendingJson)
                                .withHeaders(header("User-Task-ID", REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withStatusCode(202)
                                .withDelay(TimeUnit.SECONDS, responseDelay));

        // Rebalance response with no goals set - non-verbose
        JsonBody json = getJsonFromResource("CC-Rebalance-no-goals.json");

        ccServer
                .when(
                        request()
                                .withMethod("POST")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.key, "true|false"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.key, "false"))
                                .withPath(CruiseControlEndpoints.REBALANCE.path)
                                .withHeader(AUTH_HEADER)
                                .withSecure(true),
                        Times.unlimited())
                .respond(
                        response()
                                .withBody(json)
                                .withHeaders(header("User-Task-ID", REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, responseDelay));

        // Rebalance response with no goals set - verbose
        JsonBody jsonVerbose = getJsonFromResource("CC-Rebalance-no-goals-verbose.json");

        ccServer
                .when(
                        request()
                                .withMethod("POST")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.key, "true|false"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.key, "true"))
                                .withPath(CruiseControlEndpoints.REBALANCE.path)
                                .withHeader(AUTH_HEADER)
                                .withSecure(true))
                .respond(
                        response()
                                .withBody(jsonVerbose)
                                .withHeaders(header("User-Task-ID", REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, responseDelay));
    }


    /**
     * Setup responses for various bad goal configurations possible on a rebalance request.
     */
    public static void setupCCRebalanceBadGoalsError(ClientAndServer ccServer) throws IOException, URISyntaxException {

        // Response if the user has set custom goals which do not include all configured hard.goals
        JsonBody jsonError = getJsonFromResource("CC-Rebalance-bad-goals-error.json");

        ccServer
                .when(
                        request()
                                .withMethod("POST")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.key, "true|false"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.key, "true|false"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.GOALS.key, ".+"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.SKIP_HARD_GOAL_CHECK.key, "false"))
                                .withPath(CruiseControlEndpoints.REBALANCE.path)
                                .withHeader(AUTH_HEADER)
                                .withSecure(true))
                .respond(
                        response()
                                .withBody(jsonError)
                                .withHeaders(header("User-Task-ID", REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withStatusCode(500)
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));

        // Response if the user has set custom goals which do not include all configured hard.goals
        // Note: This uses the no-goals example response but the difference between custom goals and default goals is not tested here
        JsonBody jsonSummary = getJsonFromResource("CC-Rebalance-no-goals-verbose.json");

        ccServer
                .when(
                        request()
                                .withMethod("POST")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.key, "true|false"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.VERBOSE.key, "true|false"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.GOALS.key, ".+"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.SKIP_HARD_GOAL_CHECK.key, "true"))
                                .withPath(CruiseControlEndpoints.REBALANCE.path)
                                .withHeader(AUTH_HEADER)
                                .withSecure(true))
                .respond(
                        response()
                                .withBody(jsonSummary)
                                .withHeaders(header("User-Task-ID", REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withStatusCode(200)
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));
    }

    /**
     * Sets up the User Tasks endpoint. These endpoints expect the query to contain the user-task-id returned in the header of the response from
     * the rebalance endpoints.
     *
     * @param ccServer The ClientAndServer instance that this endpoint will be added too.
     * @param activeCalls The number of calls to the User Tasks endpoint that should return "Active" before "inExecution" is returned as the status.
     * @param inExecutionCalls The number of calls to the User Tasks endpoint that should return "InExecution" before "Completed" is returned as the status.
     * @throws IOException If there are issues connecting to the network port.
     * @throws URISyntaxException If any of the configured end points are invalid.
     */
    public static void setupCCUserTasksResponseNoGoals(ClientAndServer ccServer, int activeCalls, int inExecutionCalls) throws IOException, URISyntaxException {

        // User tasks response for the rebalance request with no goals set (non-verbose)
        JsonBody jsonActive = getJsonFromResource("CC-User-task-rebalance-no-goals-Active.json");
        JsonBody jsonInExecution = getJsonFromResource("CC-User-task-rebalance-no-goals-inExecution.json");
        JsonBody jsonCompleted = getJsonFromResource("CC-User-task-rebalance-no-goals-completed.json");

        // The first activeCalls times respond that with a status of "Active"
        ccServer
                .when(
                        request()
                                .withMethod("GET")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.FETCH_COMPLETE.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.USER_TASK_IDS.key, REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withPath(CruiseControlEndpoints.USER_TASKS.path)
                                .withHeader(AUTH_HEADER)
                                .withSecure(true),
                        Times.exactly(activeCalls))
                .respond(
                        response()
                                .withBody(jsonActive)
                                .withHeaders(header("User-Task-ID", USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));

        // The next inExecutionCalls times respond that with a status of "InExecution"
        ccServer
                .when(
                        request()
                                .withMethod("GET")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.FETCH_COMPLETE.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.USER_TASK_IDS.key, REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withPath(CruiseControlEndpoints.USER_TASKS.path)
                                .withHeader(AUTH_HEADER)
                                .withSecure(true),
                        Times.exactly(inExecutionCalls))
                .respond(
                        response()
                                .withBody(jsonInExecution)
                                .withHeaders(header("User-Task-ID", USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));

        // On the N+1 call, respond with a completed rebalance task.
        ccServer
                .when(
                        request()
                                .withMethod("GET")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.FETCH_COMPLETE.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.USER_TASK_IDS.key, REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withPath(CruiseControlEndpoints.USER_TASKS.path)
                                .withHeader(AUTH_HEADER)
                                .withSecure(true),
                        Times.unlimited())
                .respond(
                        response()
                                .withBody(jsonCompleted)
                                .withHeaders(header("User-Task-ID", USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));

        // User tasks response for the rebalance request with no goals set (verbose)
        JsonBody jsonActiveVerbose = getJsonFromResource("CC-User-task-rebalance-no-goals-verbose-Active.json");
        JsonBody jsonInExecutionVerbose = getJsonFromResource("CC-User-task-rebalance-no-goals-verbose-inExecution.json");
        JsonBody jsonCompletedVerbose = getJsonFromResource("CC-User-task-rebalance-no-goals-verbose-completed.json");

        // The first activeCalls times respond that with a status of "Active"
        ccServer
                .when(
                        request()
                                .withMethod("GET")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.FETCH_COMPLETE.key, "true"))
                                .withQueryStringParameter(
                                        Parameter.param(CruiseControlParameters.USER_TASK_IDS.key, REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID))
                                .withPath(CruiseControlEndpoints.USER_TASKS.path)
                                .withHeader(AUTH_HEADER)
                                .withSecure(true),
                        Times.exactly(activeCalls))
                .respond(
                        response()
                                .withBody(jsonActiveVerbose)
                                .withHeaders(header("User-Task-ID", USER_TASK_REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));

        ccServer
                .when(
                        request()
                                .withMethod("GET")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.FETCH_COMPLETE.key, "true"))
                                .withQueryStringParameter(
                                        Parameter.param(CruiseControlParameters.USER_TASK_IDS.key, REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID))
                                .withPath(CruiseControlEndpoints.USER_TASKS.path)
                                .withHeader(AUTH_HEADER)
                                .withSecure(true),
                        Times.exactly(inExecutionCalls))
                .respond(
                        response()
                                .withBody(jsonInExecutionVerbose)
                                .withHeaders(header("User-Task-ID", USER_TASK_REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));

        ccServer
                .when(
                        request()
                                .withMethod("GET")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.FETCH_COMPLETE.key, "true"))
                                .withQueryStringParameter(
                                        Parameter.param(CruiseControlParameters.USER_TASK_IDS.key, REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID))
                                .withPath(CruiseControlEndpoints.USER_TASKS.path)
                                .withHeader(AUTH_HEADER)
                                .withSecure(true),
                        Times.unlimited())
                .respond(
                        response()
                                .withBody(jsonCompletedVerbose)
                                .withHeaders(header("User-Task-ID", USER_TASK_REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));
    }

    /**
     * Setup response of task completed with error.
     */
    public static void setupCCUserTasksCompletedWithError(ClientAndServer ccServer) throws IOException, URISyntaxException {

        // This simulates asking for the status of a task that has Complete with error and fetch_completed_task=true
        JsonBody compWithErrorJson = getJsonFromResource("CC-User-task-status-completed-with-error.json");

        ccServer
                .when(
                        request()
                                .withMethod("GET")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true"))
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.FETCH_COMPLETE.key, "true"))
                                .withPath(CruiseControlEndpoints.USER_TASKS.path)
                                .withHeader(AUTH_HEADER)
                                .withSecure(true))
                .respond(
                        response()
                                .withBody(compWithErrorJson)
                                .withStatusCode(200)
                                .withHeaders(header("User-Task-ID", USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));
    }

    /**
     * Setup response of task being stopped.
     */
    public static void setupCCStopResponse(ClientAndServer ccServer) throws IOException, URISyntaxException {

        JsonBody jsonStop = getJsonFromResource("CC-Stop.json");

        ccServer
                .when(
                        request()
                                .withMethod("POST")
                                .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.key, "true|false"))
                                .withPath(CruiseControlEndpoints.STOP.path)
                                .withHeader(AUTH_HEADER)
                                .withSecure(true))
                .respond(
                        response()
                                .withBody(jsonStop)
                                .withHeaders(header("User-Task-ID", "stopped"))
                                .withDelay(TimeUnit.SECONDS, RESPONSE_DELAY_SEC));

    }
}
