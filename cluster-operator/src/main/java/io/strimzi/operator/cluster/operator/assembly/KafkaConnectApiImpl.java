/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.operator.cluster.operator.resource.HttpClientUtils;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.OrderedProperties;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

@SuppressWarnings({"deprecation"})
class KafkaConnectApiImpl implements KafkaConnectApi {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaConnectApiImpl.class);
    public static final TypeReference<Map<String, Object>> TREE_TYPE = new TypeReference<Map<String, Object>>() {
    };
    public static final TypeReference<Map<String, String>> MAP_OF_STRINGS = new TypeReference<Map<String, String>>() {
    };
    public static final TypeReference<Map<String, Map<String, String>>> MAP_OF_MAP_OF_STRINGS = new TypeReference<Map<String, Map<String, String>>>() {
    };
    public static final TypeReference<Map<String, Map<String, List<String>>>> MAP_OF_MAP_OF_LIST_OF_STRING = new TypeReference<Map<String, Map<String, List<String>>>>() {
    };
    private final ObjectMapper mapper = new ObjectMapper();
    private final Vertx vertx;

    public KafkaConnectApiImpl(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<Map<String, Object>> createOrUpdatePutRequest(
            Reconciliation reconciliation,
            String host, int port,
            String connectorName, JsonObject configJson) {
        Buffer data = configJson.toBuffer();
        String path = "/connectors/" + connectorName + "/config";
        LOGGER.debugCr(reconciliation, "Making PUT request to {} with body {}", path, configJson);
        return HttpClientUtils.withHttpClient(vertx, new HttpClientOptions().setLogActivity(true), (httpClient, result) ->
            httpClient.request(HttpMethod.PUT, port, host, path, request -> {
                if (request.succeeded()) {
                    request.result().setFollowRedirects(true)
                            .putHeader("Accept", "application/json")
                            .putHeader("Content-Type", "application/json")
                            .putHeader("Content-Length", String.valueOf(data.length()))
                            .write(data);
                    request.result().send(response -> {
                        if (response.succeeded()) {
                            if (response.result().statusCode() == 200 || response.result().statusCode() == 201) {
                                response.result().bodyHandler(buffer -> {
                                    try {
                                        Map t = mapper.readValue(buffer.getBytes(), Map.class);
                                        LOGGER.debugCr(reconciliation, "Got {} response to PUT request to {}: {}", response.result().statusCode(), path, t);
                                        result.complete(t);
                                    } catch (IOException e) {
                                        result.fail(new ConnectRestException(response.result(), "Could not deserialize response: " + e));
                                    }
                                });
                            } else {
                                // TODO Handle 409 (Conflict) indicating a rebalance in progress
                                LOGGER.debugCr(reconciliation, "Got {} response to PUT request to {}", response.result().statusCode(), path);
                                response.result().bodyHandler(buffer -> {
                                    JsonObject x = buffer.toJsonObject();
                                    result.fail(new ConnectRestException(response.result(), x.getString("message")));
                                });
                            }
                        } else {
                            result.tryFail(response.cause());
                        }
                    });
                } else {
                    result.fail(request.cause());
                }
            }));
    }

    @Override
    public Future<Map<String, Object>> getConnector(
            Reconciliation reconciliation,
            String host, int port,
            String connectorName) {
        return doGet(reconciliation, host, port, String.format("/connectors/%s", connectorName),
                new HashSet<>(asList(200, 201)),
                TREE_TYPE);
    }

    private <T> Future<T> doGet(Reconciliation reconciliation, String host, int port, String path, Set<Integer> okStatusCodes, TypeReference<T> type) {
        LOGGER.debugCr(reconciliation, "Making GET request to {}", path);
        return HttpClientUtils.withHttpClient(vertx, new HttpClientOptions().setLogActivity(true), (httpClient, result) ->
            httpClient.request(HttpMethod.GET, port, host, path, request -> {
                if (request.succeeded()) {
                    request.result().setFollowRedirects(true)
                            .putHeader("Accept", "application/json");
                    request.result().send(response -> {
                        if (response.succeeded()) {
                            if (okStatusCodes.contains(response.result().statusCode())) {
                                response.result().bodyHandler(buffer -> {
                                    try {
                                        T t = mapper.readValue(buffer.getBytes(), type);
                                        LOGGER.debugCr(reconciliation, "Got {} response to GET request to {}: {}", response.result().statusCode(), path, t);
                                        result.complete(t);
                                    } catch (IOException e) {
                                        result.fail(new ConnectRestException(response.result(), "Could not deserialize response: " + e));
                                    }
                                });
                            } else {
                                // TODO Handle 409 (Conflict) indicating a rebalance in progress
                                LOGGER.debugCr(reconciliation, "Got {} response to GET request to {}", response.result().statusCode(), path);
                                response.result().bodyHandler(buffer -> {
                                    JsonObject x = buffer.toJsonObject();
                                    result.fail(new ConnectRestException(response.result(), x.getString("message")));
                                });
                            }
                        } else {
                            result.tryFail(response.cause());
                        }
                    });
                } else {
                    result.tryFail(request.cause());
                }
            }));
    }

    @Override
    public Future<Map<String, String>> getConnectorConfig(
            Reconciliation reconciliation,
            String host, int port,
            String connectorName) {
        return doGet(reconciliation, host, port, String.format("/connectors/%s/config", connectorName),
                new HashSet<>(asList(200, 201)),
                MAP_OF_STRINGS);
    }

    @Override
    public Future<Map<String, String>> getConnectorConfig(Reconciliation reconciliation, BackOff backOff, String host, int port, String connectorName) {
        return withBackoff(reconciliation, backOff, connectorName, Collections.singleton(409),
            () -> getConnectorConfig(reconciliation, host, port, connectorName), "config");
    }

    @Override
    public Future<Void> delete(Reconciliation reconciliation, String host, int port, String connectorName) {
        String path = "/connectors/" + connectorName;
        return HttpClientUtils.withHttpClient(vertx, new HttpClientOptions().setLogActivity(true), (httpClient, result) ->
            httpClient.request(HttpMethod.DELETE, port, host, path, request -> {
                if (request.succeeded()) {
                    request.result().setFollowRedirects(true)
                            .putHeader("Accept", "application/json")
                            .putHeader("Content-Type", "application/json");
                    request.result().send(response -> {
                        if (response.succeeded()) {
                            if (response.result().statusCode() == 204) {
                                LOGGER.debugCr(reconciliation, "Connector was deleted. Waiting for status deletion!");
                                withBackoff(reconciliation, new BackOff(200L, 2, 10), connectorName, Collections.singleton(200),
                                    () -> status(reconciliation, host, port, connectorName, Collections.singleton(404)), "status")
                                    .onComplete(res -> {
                                        if (res.succeeded()) {
                                            result.complete();
                                        } else {
                                            result.fail(res.cause());
                                        }
                                    });
                            } else {
                                // TODO Handle 409 (Conflict) indicating a rebalance in progress
                                response.result().bodyHandler(buffer -> {
                                    JsonObject x = buffer.toJsonObject();
                                    result.fail(new ConnectRestException(response.result(), x.getString("message")));
                                });
                            }
                        } else {
                            result.tryFail(response.cause());
                        }
                    });
                } else {
                    result.tryFail(request.cause());
                }
            }));
    }

    @Override
    public Future<Map<String, Object>> statusWithBackOff(Reconciliation reconciliation, BackOff backOff, String host, int port, String connectorName) {
        return withBackoff(reconciliation, backOff, connectorName, Collections.singleton(404),
            () -> status(reconciliation, host, port, connectorName), "status");
    }

    private <T> Future<T> withBackoff(Reconciliation reconciliation,
                                      BackOff backOff, String connectorName,
                                      Set<Integer> retriableStatusCodes,
                                      Supplier<Future<T>> supplier,
                                      String attribute) {
        Promise<T> result = Promise.promise();

        Handler<Long> handler = new Handler<Long>() {
            @Override
            public void handle(Long tid) {
                supplier.get().onComplete(connectorStatus -> {
                    if (connectorStatus.succeeded()) {
                        result.complete(connectorStatus.result());
                    } else {
                        Throwable cause = connectorStatus.cause();
                        if (cause instanceof ConnectRestException
                                && retriableStatusCodes.contains(((ConnectRestException) cause).getStatusCode())) {
                            if (backOff.done()) {
                                LOGGER.debugCr(reconciliation, "Connector {} {} returned HTTP {} and we run out of back off time", connectorName, attribute, ((ConnectRestException) cause).getStatusCode());
                                result.fail(cause);
                            } else {
                                LOGGER.debugCr(reconciliation, "Connector {} {} returned HTTP {} - backing off", connectorName, attribute, ((ConnectRestException) cause).getStatusCode());
                                rescheduleOrComplete(tid);
                            }
                        } else {
                            result.fail(cause);
                        }
                    }
                });
            }

            void rescheduleOrComplete(Long tid) {
                if (backOff.done()) {
                    LOGGER.warnCr(reconciliation, "Giving up waiting for status of connector {} after {} attempts taking {}ms",
                            connectorName, backOff.maxAttempts(), backOff.totalDelayMs());
                } else {
                    // Schedule ourselves to run again
                    long delay = backOff.delayMs();
                    LOGGER.debugCr(reconciliation, "Status for connector {} not found; " +
                                    "backing off for {}ms (cumulative {}ms)",
                            connectorName, delay, backOff.cumulativeDelayMs());
                    if (delay < 1) {
                        this.handle(tid);
                    } else {
                        vertx.setTimer(delay, this);
                    }
                }
            }
        };

        handler.handle(null);
        return result.future();
    }

    @Override
    public Future<Map<String, Object>> status(Reconciliation reconciliation, String host, int port, String connectorName) {
        return status(reconciliation, host, port, connectorName, Collections.singleton(200));
    }

    @Override
    public Future<Map<String, Object>> status(Reconciliation reconciliation, String host, int port, String connectorName, Set<Integer> okStatusCodes) {
        String path = "/connectors/" + connectorName + "/status";
        return doGet(reconciliation, host, port, path, okStatusCodes, TREE_TYPE);
    }

    @Override
    public Future<Void> pause(String host, int port, String connectorName) {
        return pauseResume(host, port, "/connectors/" + connectorName + "/pause");
    }

    @Override
    public Future<Void> resume(String host, int port, String connectorName) {
        return pauseResume(host, port, "/connectors/" + connectorName + "/resume");
    }

    private Future<Void> pauseResume(String host, int port, String path) {
        return HttpClientUtils.withHttpClient(vertx, new HttpClientOptions().setLogActivity(true), (httpClient, result) ->
                httpClient.request(HttpMethod.PUT, port, host, path, request -> {
                    if (request.succeeded()) {
                        request.result().setFollowRedirects(true)
                                .putHeader("Accept", "application/json");
                        request.result().send(response -> {
                            if (response.succeeded()) {
                                if (response.result().statusCode() == 202) {
                                    response.result().bodyHandler(body -> {
                                        result.complete();
                                    });
                                } else {
                                    result.fail("Unexpected status code " + response.result().statusCode()
                                            + " for GET request to " + host + ":" + port + path);
                                }
                            } else {
                                result.tryFail(response.cause());
                            }
                        });
                    } else {
                        result.tryFail(request.cause());
                    }
                }));
    }

    @Override
    public Future<List<String>> list(String host, int port) {
        String path = "/connectors";
        return HttpClientUtils.withHttpClient(vertx, new HttpClientOptions().setLogActivity(true), (httpClient, result) ->
                httpClient.request(HttpMethod.GET, port, host, path, request -> {

                    if (request.succeeded()) {
                        request.result().setFollowRedirects(true)
                                .putHeader("Accept", "application/json");
                        request.result().send(response -> {
                            if (response.succeeded()) {
                                if (response.result().statusCode() == 200) {
                                    response.result().bodyHandler(buffer -> {
                                        JsonArray objects = buffer.toJsonArray();
                                        List<String> list = new ArrayList<>(objects.size());
                                        for (Object o : objects) {
                                            if (o instanceof String) {
                                                list.add((String) o);
                                            } else {
                                                result.fail(o == null ? "null" : o.getClass().getName());
                                            }
                                        }
                                        result.complete(list);
                                    });
                                } else {
                                    result.fail(new ConnectRestException(response.result(), "Unexpected status code"));
                                }
                            } else {
                                result.tryFail(response.cause());
                            }
                        });
                    } else {
                        result.tryFail(request.cause());
                    }
                }));
    }

    @Override
    public Future<List<ConnectorPlugin>> listConnectorPlugins(Reconciliation reconciliation, String host, int port) {
        String path = "/connector-plugins";
        return HttpClientUtils.withHttpClient(vertx, new HttpClientOptions().setLogActivity(true), (httpClient, result) ->
                httpClient.request(HttpMethod.GET, port, host, path, request -> {
                    if (request.succeeded()) {
                        request.result().setFollowRedirects(true)
                                .putHeader("Accept", "application/json");
                        request.result().send(response -> {
                            if (response.succeeded()) {
                                if (response.result().statusCode() == 200) {
                                    response.result().bodyHandler(buffer -> {
                                        try {
                                            result.complete(asList(mapper.readValue(buffer.getBytes(), ConnectorPlugin[].class)));
                                        } catch (IOException e) {
                                            LOGGER.warnCr(reconciliation, "Failed to parse list of connector plugins", e);
                                            result.fail(new ConnectRestException(response.result(), "Failed to parse list of connector plugins", e));
                                        }
                                    });
                                } else {
                                    result.fail(new ConnectRestException(response.result(), "Unexpected status code"));
                                }
                            } else {
                                result.tryFail(response.cause());
                            }
                        });
                    } else {
                        result.tryFail(request.cause());
                    }
                }));
    }

    private Future<Void> updateConnectorLogger(Reconciliation reconciliation, String host, int port, String logger, String level) {
        String path = "/admin/loggers/" + logger;
        JsonObject levelJO = new JsonObject();
        levelJO.put("level", level);
        LOGGER.debugCr(reconciliation, "Making PUT request to {} with body {}", path, levelJO);
        return HttpClientUtils.withHttpClient(vertx, new HttpClientOptions().setLogActivity(true), (httpClient, result) -> {
            Buffer buffer = levelJO.toBuffer();
            httpClient
                    .request(HttpMethod.PUT, port, host, path, request -> {
                        if (request.succeeded()) {
                            request.result().putHeader("Content-Type", "application/json")
                                    .setFollowRedirects(true)
                                    .putHeader("Content-Length", Integer.toString(buffer.toString().length()))
                                    .write(buffer.toString());
                            request.result().send(response -> {
                                if (response.succeeded()) {
                                    if (response.result().statusCode() == 200) {
                                        response.result().bodyHandler(body -> {
                                            LOGGER.debugCr(reconciliation, "Logger {} updated to level {}", logger, level);
                                            result.complete();
                                        });
                                    } else {
                                        LOGGER.debugCr(reconciliation, "Logger {} did not update to level {} (http code {})", logger, level, response.result().statusCode());
                                        result.fail(new ConnectRestException(response.result(), "Unexpected status code"));
                                    }
                                } else {
                                    result.tryFail(response.cause());
                                }
                            });
                        } else {
                            result.tryFail(request.cause());
                        }
                    });
        });
    }

    @Override
    public Future<Map<String, Map<String, String>>> listConnectLoggers(Reconciliation reconciliation, String host, int port) {
        String path = "/admin/loggers/";
        return HttpClientUtils.withHttpClient(vertx, new HttpClientOptions().setLogActivity(true), (httpClient, result) ->
                httpClient.request(HttpMethod.GET, port, host, path, request -> {
                    if (request.succeeded()) {
                        request.result().setFollowRedirects(true)
                                .putHeader("Accept", "application/json");
                        request.result().send(response -> {
                            if (response.succeeded()) {
                                if (response.result().statusCode() == 200) {
                                    response.result().bodyHandler(buffer -> {
                                        try {
                                            Map<String, Map<String, String>> fetchedLoggers = mapper.readValue(buffer.getBytes(), MAP_OF_MAP_OF_STRINGS);
                                            result.complete(fetchedLoggers);
                                        } catch (IOException e) {
                                            LOGGER.warnCr(reconciliation, "Failed to get list of connector loggers", e);
                                            result.fail(new ConnectRestException(response.result(), "Failed to get connector loggers", e));
                                        }
                                    });
                                } else {
                                    result.fail(new ConnectRestException(response.result(), "Unexpected status code"));
                                }
                            } else {
                                result.tryFail(response.cause());
                            }
                        });
                    } else {
                        result.tryFail(request.cause());
                    }
                }));
    }

    private Future<Void> updateLoggers(Reconciliation reconciliation, String host, int port, String desiredLogging, Map<String, Map<String, String>> fetchedLoggers, OrderedProperties defaultLogging) {
        desiredLogging = Util.expandVars(desiredLogging);
        Map<String, String> updateLoggers = new LinkedHashMap<>();
        defaultLogging.asMap().entrySet().forEach(entry -> {
            // set all logger levels to default
            if (entry.getKey().equals("log4j.rootLogger")) {
                updateLoggers.put("root", Util.expandVar(entry.getValue(), defaultLogging.asMap()));
            } else if (entry.getKey().startsWith("log4j.logger.")) {
                updateLoggers.put(entry.getKey().substring("log4j.logger.".length()), Util.expandVar(entry.getValue(), defaultLogging.asMap()));
            }
        });

        OrderedProperties ops = new OrderedProperties();
        ops.addStringPairs(desiredLogging);
        ops.asMap().entrySet().forEach(entry -> {
            // set desired loggers to desired levels
            if (entry.getKey().equals("log4j.rootLogger")) {
                if (fetchedLoggers.get("root") == null || fetchedLoggers.get("root").get("level") == null ||
                        !entry.getValue().equals(fetchedLoggers.get("root").get("level"))) {
                    updateLoggers.put("root", Util.expandVar(entry.getValue(), ops.asMap()));
                }
            } else if (entry.getKey().startsWith("log4j.logger.")) {
                Map<String, String> fetchedLogger = fetchedLoggers.get(entry.getKey().substring("log4j.logger.".length()));
                if (fetchedLogger == null || fetchedLogger.get("level") == null || parentLogLevelChanged(fetchedLoggers, ops, entry)) {
                    updateLoggers.put(entry.getKey().substring("log4j.logger.".length()), Util.expandVar(entry.getValue(), ops.asMap()));
                }
            }
        });

        LinkedHashMap<String, String> updateSortedLoggers = sortLoggers(updateLoggers);
        Future<Void> result = Future.succeededFuture();
        for (Map.Entry<String, String> logger : updateSortedLoggers.entrySet()) {
            result = result.compose(previous -> updateConnectorLogger(reconciliation, host, port, logger.getKey(), getLoggerLevelFromAppenderCouple(logger.getValue())));
        }
        return result;
    }

    /*
        We do use an optimization of not sending all the logger level each reconciliation. We can send only those which changed.
        However we need to resend all lower hierarchy level logger levels.
        So if root level changed in desired configuration, all other levels need to be reset as well.
        If logger io.org changed, all loggers prefixed by io.org need to be reset.
    */
    protected boolean parentLogLevelChanged(Map<String, Map<String, String>> fetchedLoggers, OrderedProperties ops, Map.Entry<String, String> tested) {
        if (!fetchedLoggers.get("root").get("level").equals(getLoggerLevelFromAppenderCouple(ops.asMap().get("log4j.rootLogger")))) {
            return true;
        }

        AtomicBoolean result = new AtomicBoolean(false);
        List<Map.Entry<String, String>> parents = ops.asMap().entrySet().stream().filter(entry -> entry.getKey().startsWith("log4j.logger." + tested.getKey())).collect(Collectors.toList());
        parents.stream().forEach(entry -> {
            if (!entry.getValue().equals(tested.getValue())) {
                result.set(true);
            }
        });
        return result.get();
    }

    /**
     * Parses logger level from couple LEVEL, APPENDER
     * @param couple tested input
     * @return logger Level
     */
    private String getLoggerLevelFromAppenderCouple(String couple) {
        int index = couple.indexOf(",");
        if (index > 0) {
            return couple.substring(0, index).trim();
        } else {
            return couple.trim();
        }
    }

    @Override
    public Future<Void> updateConnectLoggers(Reconciliation reconciliation, String host, int port, String desiredLogging, OrderedProperties defaultLogging) {
        return listConnectLoggers(reconciliation, host, port)
                .compose(fetchedLoggers -> updateLoggers(reconciliation, host, port, desiredLogging, fetchedLoggers, defaultLogging));
    }

    /**
     * To apply loggers correctly, we need to sort them. The sorting is performed on base of logger generality.
     * Logger "abc.company" is more general than "abc.company.name"
     * @param loggers map of loggers to be sorted
     * @return map of sorted loggers
     */
    private LinkedHashMap<String, String> sortLoggers(Map<String, String> loggers) {
        Comparator<Map.Entry<String, String>> loggerComparator = (e1, e2) -> {
            String k1 = e1.getKey();
            String k2 = e2.getKey();
            if (k1.equals("root")) {
                // we need root logger always to be the first logger to be set via REST API
                return Integer.MIN_VALUE;
            }
            if (k2.equals("root")) {
                return Integer.MAX_VALUE;
            }
            return k1.compareTo(k2);
        };
        List<Map.Entry<String, String>> listOfEntries = new ArrayList<>(loggers.entrySet());
        listOfEntries.sort(loggerComparator);

        LinkedHashMap<String, String> sortedLoggers = new LinkedHashMap<>(listOfEntries.size());
        for (Map.Entry<String, String> entry : listOfEntries) {
            sortedLoggers.put(entry.getKey(), entry.getValue());
        }
        return sortedLoggers;
    }

    @Override
    public Future<Void> restart(String host, int port, String connectorName) {
        return restartConnectorOrTask(host, port, "/connectors/" + connectorName + "/restart");
    }

    @Override
    public Future<Void> restartTask(String host, int port, String connectorName, int taskID) {
        return restartConnectorOrTask(host, port, "/connectors/" + connectorName + "/tasks/" + taskID + "/restart");
    }

    private Future<Void> restartConnectorOrTask(String host, int port, String path) {
        return HttpClientUtils.withHttpClient(vertx, new HttpClientOptions().setLogActivity(true), (httpClient, result) ->
            httpClient.request(HttpMethod.POST, port, host, path, request -> {
                if (request.succeeded()) {
                    request.result().setFollowRedirects(true)
                            .putHeader("Accept", "application/json");
                    request.result().send(response -> {
                        if (response.succeeded()) {
                            if (response.result().statusCode() == 204) {
                                response.result().bodyHandler(body -> {
                                    result.complete();
                                });
                            } else {
                                result.fail("Unexpected status code " + response.result().statusCode()
                                        + " for POST request to " + host + ":" + port + path);
                            }
                        } else {
                            result.tryFail(response.cause());
                        }
                    });
                } else {
                    result.tryFail(request.cause());
                }
            }));
    }

    @Override
    public Future<List<String>> getConnectorTopics(Reconciliation reconciliation, String host, int port, String connectorName) {
        String path = String.format("/connectors/%s/topics", connectorName);
        return HttpClientUtils.withHttpClient(vertx, new HttpClientOptions().setLogActivity(true), (httpClient, result) ->
            httpClient.request(HttpMethod.GET, port, host, path, request -> {
                if (request.succeeded()) {
                    request.result().setFollowRedirects(true)
                            .putHeader("Accept", "application/json");
                } else {
                    result.tryFail(request.cause());
                }
                if (request.succeeded()) {
                    request.result().send(response -> {
                        if (response.succeeded()) {
                            if (response.result().statusCode() == 200) {
                                response.result().bodyHandler(buffer -> {
                                    try {
                                        Map<String, Map<String, List<String>>> t = mapper.readValue(buffer.getBytes(), MAP_OF_MAP_OF_LIST_OF_STRING);
                                        result.complete(t.get(connectorName).get("topics"));
                                    } catch (IOException e) {
                                        LOGGER.warnCr(reconciliation, "Failed to parse list of connector topics", e);
                                        result.fail(new ConnectRestException(response.result(), "Failed to parse list of connector topics", e));
                                    }
                                });
                            } else {
                                result.fail(new ConnectRestException(response.result(), "Unexpected status code"));
                            }
                        } else {
                            result.fail(response.cause());
                        }
                    });
                } else {
                    result.tryFail(request.cause());
                }
            }));
    }
}
