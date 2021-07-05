/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.strimzi.operator.cluster.operator.resource.HttpClientUtils;
import io.fabric8.kubernetes.api.model.HTTPHeader;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.common.Util;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.PfxOptions;

import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class CruiseControlApiImpl implements CruiseControlApi {

    private static final boolean HTTP_CLIENT_ACTIVITY_LOGGING = false;
    private static final int HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS = -1; // use default internal HTTP client timeout
    private static final String STATUS_KEY = "Status";

    private final Vertx vertx;
    private final long idleTimeout;
    private Secret ccSecret;
    private Secret coSecret;
    private boolean apiAuthorizationEnabled;
    private boolean apiAuthenticationEnabled;
    private HTTPHeader authHttpHeader;

    public CruiseControlApiImpl(Vertx vertx, Secret ccSecret, Secret coSecret, Boolean apiAuthorizationEnabled, boolean apiAuthenticationEnabled) {
        this.vertx = vertx;
        this.idleTimeout = HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS;
        this.ccSecret = ccSecret;
        this.coSecret = coSecret;
        this.apiAuthorizationEnabled = apiAuthorizationEnabled;
        this.apiAuthenticationEnabled = apiAuthenticationEnabled;
        this.authHttpHeader = getAuthHttpHeader();
    }

    public CruiseControlApiImpl(Vertx vertx, int idleTimeout, Secret ccSecret, Secret coSecret, Boolean apiAuthorizationEnabled, boolean apiAuthenticationEnabled) {
        this.vertx = vertx;
        this.idleTimeout = idleTimeout;
        this.ccSecret = ccSecret;
        this.coSecret = coSecret;
        this.apiAuthorizationEnabled = apiAuthorizationEnabled;
        this.apiAuthenticationEnabled = apiAuthenticationEnabled;
        this.authHttpHeader = getAuthHttpHeader();
    }

    @Override
    public Future<CruiseControlResponse> getCruiseControlState(String host, int port, boolean verbose) {
        return getCruiseControlState(host, port, verbose, null);
    }

    public HttpClientOptions getHttpClientOptions() {
        if (apiAuthenticationEnabled) {
            return new HttpClientOptions()
                .setLogActivity(HTTP_CLIENT_ACTIVITY_LOGGING)
                .setSsl(true)
                .setTrustAll(true)
                .setVerifyHost(false)
                .setPfxKeyCertOptions(
                    new PfxOptions()
                        .setPassword(new String(Util.decodeFromSecret(coSecret, "cluster-operator.password"), StandardCharsets.US_ASCII))
                        .setValue(Buffer.buffer(Util.decodeFromSecret(coSecret, "cluster-operator.p12")))
                    );
        } else {
            return new HttpClientOptions()
                    .setLogActivity(HTTP_CLIENT_ACTIVITY_LOGGING);
        }
    }

    public HTTPHeader getAuthHttpHeader() {
        if (apiAuthorizationEnabled) {
            String password = new String(Util.decodeFromSecret(ccSecret, CruiseControl.API_ADMIN_PASSWORD_KEY), StandardCharsets.US_ASCII);
            HTTPHeader header = CruiseControl.generateAuthHttpHeader(CruiseControl.API_ADMIN_NAME, password);
            return header;
        } else {
            return null;
        }
    }

    @SuppressWarnings("deprecation")
    public Future<CruiseControlResponse> getCruiseControlState(String host, int port, boolean verbose, String userTaskId) {

        String path = new PathBuilder(CruiseControlEndpoints.STATE)
                .addParameter(CruiseControlParameters.JSON, "true")
                .addParameter(CruiseControlParameters.VERBOSE, String.valueOf(verbose))
                .build();

        HttpClientOptions options = getHttpClientOptions();

        return HttpClientUtils.withHttpClient(vertx, options, (httpClient, result) -> {
            httpClient.request(HttpMethod.GET, port, host, path, request -> {
                if (request.succeeded()) {

                    if (authHttpHeader != null) {
                        request.result().putHeader(authHttpHeader.getName(), authHttpHeader.getValue());
                    }

                    request.result().send(response -> {
                        if (response.succeeded()) {
                            if (response.result().statusCode() == 200 || response.result().statusCode() == 201) {
                                String userTaskID = response.result().getHeader(CC_REST_API_USER_ID_HEADER);
                                response.result().bodyHandler(buffer -> {
                                    JsonObject json = buffer.toJsonObject();
                                    if (json.containsKey(CC_REST_API_ERROR_KEY)) {
                                        result.fail(new CruiseControlRestException(
                                                "Error for request: " + host + ":" + port + path + ". Server returned: " +
                                                        json.getString(CC_REST_API_ERROR_KEY)));
                                    } else {
                                        CruiseControlResponse ccResponse = new CruiseControlResponse(userTaskID, json);
                                        result.complete(ccResponse);
                                    }
                                });

                            } else {
                                result.fail(new CruiseControlRestException(
                                        "Unexpected status code " + response.result().statusCode() + " for request to " + host + ":" + port + path));
                            }
                        } else {
                            httpExceptionHandler(result, response.cause());
                        }
                    });
                } else {
                    result.fail(request.cause());
                }

                if (idleTimeout != HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS) {
                    request.result().setTimeout(idleTimeout * 1000);
                }

                if (userTaskId != null) {
                    request.result().putHeader(CC_REST_API_USER_ID_HEADER, userTaskId);
                }
            });
        });
    }

    @Override
    @SuppressWarnings("deprecation")
    public Future<CruiseControlRebalanceResponse> rebalance(String host, int port, RebalanceOptions rbOptions, String userTaskId) {

        if (rbOptions == null && userTaskId == null) {
            return Future.failedFuture(
                    new IllegalArgumentException("Either rebalance options or user task ID should be supplied, both were null"));
        }

        String path = new PathBuilder(CruiseControlEndpoints.REBALANCE)
                .addParameter(CruiseControlParameters.JSON, "true")
                .addRebalanceParameters(rbOptions)
                .build();

        HttpClientOptions options = getHttpClientOptions();

        return HttpClientUtils.withHttpClient(vertx, options, (httpClient, result) -> {
            httpClient.request(HttpMethod.POST, port, host, path, request -> {
                if (request.succeeded()) {
                    if (idleTimeout != HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS) {
                        request.result().setTimeout(idleTimeout * 1000);
                    }

                    if (userTaskId != null) {
                        request.result().putHeader(CC_REST_API_USER_ID_HEADER, userTaskId);
                    }

                    if (authHttpHeader != null) {
                        request.result().putHeader(authHttpHeader.getName(), authHttpHeader.getValue());
                    }

                    request.result().send(response -> {
                        if (response.succeeded()) {
                            if (response.result().statusCode() == 200 || response.result().statusCode() == 201) {
                                response.result().bodyHandler(buffer -> {
                                    String userTaskID = response.result().getHeader(CC_REST_API_USER_ID_HEADER);
                                    JsonObject json = buffer.toJsonObject();
                                    CruiseControlRebalanceResponse ccResponse = new CruiseControlRebalanceResponse(userTaskID, json);
                                    result.complete(ccResponse);
                                });
                            } else if (response.result().statusCode() == 202) {
                                response.result().bodyHandler(buffer -> {
                                    String userTaskID = response.result().getHeader(CC_REST_API_USER_ID_HEADER);
                                    JsonObject json = buffer.toJsonObject();
                                    CruiseControlRebalanceResponse ccResponse = new CruiseControlRebalanceResponse(userTaskID, json);
                                    if (json.containsKey(CC_REST_API_PROGRESS_KEY)) {
                                        // If the response contains a "progress" key then the rebalance proposal has not yet completed processing
                                        ccResponse.setProposalStillCalaculating(true);
                                    } else {
                                        result.fail(new CruiseControlRestException(
                                                "Error for request: " + host + ":" + port + path +
                                                        ". 202 Status code did not contain progress key. Server returned: " +
                                                        ccResponse.getJson().toString()));
                                    }
                                    result.complete(ccResponse);
                                });
                            } else if (response.result().statusCode() == 500) {
                                response.result().bodyHandler(buffer -> {
                                    String userTaskID = response.result().getHeader(CC_REST_API_USER_ID_HEADER);
                                    JsonObject json = buffer.toJsonObject();
                                    if (json.containsKey(CC_REST_API_ERROR_KEY)) {
                                        // If there was a client side error, check whether it was due to not enough data being available
                                        if (json.getString(CC_REST_API_ERROR_KEY).contains("NotEnoughValidWindowsException")) {
                                            CruiseControlRebalanceResponse ccResponse = new CruiseControlRebalanceResponse(userTaskID, json);
                                            ccResponse.setNotEnoughDataForProposal(true);
                                            result.complete(ccResponse);
                                        } else {
                                            // If there was any other kind of error propagate this to the operator
                                            result.fail(new CruiseControlRestException(
                                                    "Error for request: " + host + ":" + port + path + ". Server returned: " +
                                                            json.getString(CC_REST_API_ERROR_KEY)));
                                        }
                                    } else {
                                        result.fail(new CruiseControlRestException(
                                                "Error for request: " + host + ":" + port + path + ". Server returned: " +
                                                        json.toString()));
                                    }
                                });
                            } else {
                                result.fail(new CruiseControlRestException(
                                        "Unexpected status code " + response.result().statusCode() + " for request to " + host + ":" + port + path));
                            }
                        } else {
                            result.fail(response.cause());
                        }
                    });
                } else {
                    httpExceptionHandler(result, request.cause());
                }
            });
        });
    }

    @Override
    @SuppressWarnings("deprecation")
    public Future<CruiseControlResponse> getUserTaskStatus(String host, int port, String userTaskId) {

        PathBuilder pathBuilder = new PathBuilder(CruiseControlEndpoints.USER_TASKS)
                        .addParameter(CruiseControlParameters.JSON, "true")
                        .addParameter(CruiseControlParameters.FETCH_COMPLETE, "true");

        if (userTaskId != null) {
            pathBuilder.addParameter(CruiseControlParameters.USER_TASK_IDS, userTaskId);
        }

        String path = pathBuilder.build();

        HttpClientOptions options = getHttpClientOptions();

        return HttpClientUtils.withHttpClient(vertx, options, (httpClient, result) -> {
            httpClient.request(HttpMethod.GET, port, host, path, request -> {
                if (request.succeeded()) {

                    if (authHttpHeader != null) {
                        request.result().putHeader(authHttpHeader.getName(), authHttpHeader.getValue());
                    }

                    request.result().send(response -> {
                        if (response.succeeded()) {
                            if (response.result().statusCode() == 200 || response.result().statusCode() == 201) {
                                String userTaskID = response.result().getHeader(CC_REST_API_USER_ID_HEADER);
                                response.result().bodyHandler(buffer -> {
                                    JsonObject json = buffer.toJsonObject();
                                    JsonObject jsonUserTask = json.getJsonArray("userTasks").getJsonObject(0);
                                    // This should not be an error with a 200 status but we play it safe
                                    if (jsonUserTask.containsKey(CC_REST_API_ERROR_KEY)) {
                                        result.fail(new CruiseControlRestException(
                                                "Error for request: " + host + ":" + port + path + ". Server returned: " +
                                                        json.getString(CC_REST_API_ERROR_KEY)));
                                    }
                                    JsonObject statusJson = new JsonObject();
                                    String taskStatusStr = jsonUserTask.getString(STATUS_KEY);
                                    statusJson.put(STATUS_KEY, taskStatusStr);
                                    CruiseControlUserTaskStatus taskStatus = CruiseControlUserTaskStatus.lookup(taskStatusStr);
                                    switch (taskStatus) {
                                        case ACTIVE:
                                            // If the status is ACTIVE there will not be a "summary" so we skip pulling the summary key
                                            break;
                                        case IN_EXECUTION:
                                            // Tasks in execution will be rebalance tasks, so their original response will contain the summary of the rebalance they are executing
                                            // We handle these in the same way as COMPLETED tasks so we drop down to that case.
                                        case COMPLETED:
                                            // Completed tasks will have the original rebalance proposal summary in their original response
                                            JsonObject originalResponse = (JsonObject) Json.decodeValue(jsonUserTask.getString(
                                                    CruiseControlRebalanceKeys.ORIGINAL_RESPONSE.getKey()));
                                            statusJson.put(CruiseControlRebalanceKeys.SUMMARY.getKey(),
                                                    originalResponse.getJsonObject(CruiseControlRebalanceKeys.SUMMARY.getKey()));
                                            // Extract the load before/after information for the brokers
                                            statusJson.put(
                                                    CruiseControlRebalanceKeys.LOAD_BEFORE_OPTIMIZATION.getKey(),
                                                    originalResponse.getJsonObject(CruiseControlRebalanceKeys.LOAD_BEFORE_OPTIMIZATION.getKey()));
                                            statusJson.put(
                                                    CruiseControlRebalanceKeys.LOAD_AFTER_OPTIMIZATION.getKey(),
                                                    originalResponse.getJsonObject(CruiseControlRebalanceKeys.LOAD_AFTER_OPTIMIZATION.getKey()));
                                            break;
                                        case COMPLETED_WITH_ERROR:
                                            // Completed with error tasks will have "CompletedWithError" as their original response, which is not Json.
                                            statusJson.put(CruiseControlRebalanceKeys.SUMMARY.getKey(), jsonUserTask.getString(CruiseControlRebalanceKeys.ORIGINAL_RESPONSE.getKey()));
                                            break;
                                        default:
                                            throw new IllegalStateException("Unexpected user task status: " + taskStatus);
                                    }
                                    result.complete(new CruiseControlResponse(userTaskID, statusJson));
                                });
                            } else if (response.result().statusCode() == 500) {
                                response.result().bodyHandler(buffer -> {
                                    JsonObject json = buffer.toJsonObject();
                                    String errorString;
                                    if (json.containsKey(CC_REST_API_ERROR_KEY)) {
                                        errorString = json.getString(CC_REST_API_ERROR_KEY);
                                    } else {
                                        errorString = json.toString();
                                    }
                                    result.fail(new CruiseControlRestException(
                                            "Error for request: " + host + ":" + port + path + ". Server returned: " + errorString));
                                });
                            } else {
                                result.fail(new CruiseControlRestException(
                                        "Unexpected status code " + response.result().statusCode() + " for GET request to " +
                                                host + ":" + port + path));
                            }
                        } else {
                            result.fail(response.cause());
                        }
                    });

                    if (idleTimeout != HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS) {
                        request.result().setTimeout(idleTimeout * 1000);
                    }

                } else {
                    httpExceptionHandler(result, request.cause());
                }
            });
        });
    }

    @Override
    @SuppressWarnings("deprecation")
    public Future<CruiseControlResponse> stopExecution(String host, int port) {

        String path = new PathBuilder(CruiseControlEndpoints.STOP)
                        .addParameter(CruiseControlParameters.JSON, "true").build();

        HttpClientOptions options = getHttpClientOptions();

        return HttpClientUtils.withHttpClient(vertx, options, (httpClient, result) -> {
            httpClient.request(HttpMethod.POST, port, host, path, request -> {
                if (request.succeeded()) {

                    if (authHttpHeader != null) {
                        request.result().putHeader(authHttpHeader.getName(), authHttpHeader.getValue());
                    }

                    request.result().send(response -> {
                        if (response.succeeded()) {
                            if (response.result().statusCode() == 200 || response.result().statusCode() == 201) {
                                String userTaskID = response.result().getHeader(CC_REST_API_USER_ID_HEADER);
                                response.result().bodyHandler(buffer -> {
                                    JsonObject json = buffer.toJsonObject();
                                    if (json.containsKey(CC_REST_API_ERROR_KEY)) {
                                        result.fail(json.getString(CC_REST_API_ERROR_KEY));
                                    } else {
                                        CruiseControlResponse ccResponse = new CruiseControlResponse(userTaskID, json);
                                        result.complete(ccResponse);
                                    }
                                });

                            } else {
                                result.fail(new CruiseControlRestException(
                                        "Unexpected status code " + response.result().statusCode() + " for GET request to " +
                                                host + ":" + port + path));
                            }
                        } else {
                            result.fail(response.cause());
                        }
                    });
                } else {
                    httpExceptionHandler(result, request.cause());
                }
                if (idleTimeout != HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS) {
                    request.result().setTimeout(idleTimeout * 1000);
                }
            });
        });
    }

    private void httpExceptionHandler(Promise<? extends CruiseControlResponse> result, Throwable t) {
        if (t instanceof TimeoutException) {
            // Vert.x throws a NoStackTraceTimeoutException (inherits from TimeoutException) when the request times out
            // so we catch and raise a TimeoutException instead
            result.fail(new TimeoutException(t.getMessage()));
        } else if (t instanceof ConnectException) {
            // Vert.x throws a AnnotatedConnectException (inherits from ConnectException) when the request times out
            // so we catch and raise a ConnectException instead
            result.fail(new ConnectException(t.getMessage()));
        } else {
            result.fail(t);
        }
    }
}
