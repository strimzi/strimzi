/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ContainerStateWaiting;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static java.util.Collections.singletonList;

/**
 * <p>Manages the rolling restart of a Kafka cluster.</p>
 *
 * <p>The following algorithm is used:</p>
 *
 * <pre>
 *   0. Start with a list of all the pods
 *   1. While the list is non-empty:
 *     2. Take the next pod from the list.
 *     3. Test whether the pod needs to be restarted.
 *         If not then:
 *           i.  Wait for it to be ready.
 *           ii. Continue from 1.
 *     4. Otherwise, check whether the pod is the controller
 *         If so, and there are still pods to be maybe-restarted then:
 *           i.  Reschedule the restart of this pod by appending it the list
 *           ii. Continue from 1.
 *     5. Otherwise, check whether the pod can be restarted without "impacting availability"
 *         If not then:
 *           i.  Reschedule the restart of this pod by appending it the list
 *           ii. Continue from 1.
 *     6. Otherwise:
 *         i.   Restart the pod
 *         ii.  Wait for it to become ready (in the kube sense)
 *         iii. Continue from 1.
 * </pre>
 *
 * <p>Where "impacting availability" is defined by {@link KafkaAvailability}.</p>
 *
 * <p>Note the following important properties of this algorithm:</p>
 * <ul>
 *     <li>if there is a spontaneous change in controller while the rolling restart is happening, any new
 *     controller is still the last pod to be rolled, thus avoid unnecessary controller elections.</li>
 *     <li>rolling should happen without impacting any topic's min.isr.</li>
 *     <li>even pods which aren't candidates for rolling are checked for readiness which partly avoids
 *     successive reconciliations each restarting a pod which never becomes ready</li>
 * </ul>
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:ParameterNumber"})
public class KafkaRoller {

    private static final Logger log = LogManager.getLogger(KafkaRoller.class);

    private final PodOperator podOperations;
    private final long pollingIntervalMs;
    protected final long operationTimeoutMs;
    protected final Vertx vertx;
    private final String cluster;
    private final Secret clusterCaCertSecret;
    private final Secret coKeySecret;
    private final Integer numPods;
    private final Supplier<BackOff> backoffSupplier;
    protected String namespace;
    private final AdminClientProvider adminClientProvider;
    private final String kafkaConfig;
    private final String kafkaLogging;
    private final KafkaVersion kafkaVersion;
    private final Reconciliation reconciliation;
    private final boolean allowReconfiguration;
    private Admin allClient;

    public KafkaRoller(Vertx vertx, Reconciliation reconciliation, PodOperator podOperations,
                        long pollingIntervalMs, long operationTimeoutMs, Supplier<BackOff> backOffSupplier,
                        StatefulSet sts, Secret clusterCaCertSecret, Secret coKeySecret,
                        String kafkaConfig, String kafkaLogging, KafkaVersion kafkaVersion, boolean allowReconfiguration) {
        this(vertx, reconciliation, podOperations, pollingIntervalMs, operationTimeoutMs, backOffSupplier,
                sts, clusterCaCertSecret, coKeySecret, new DefaultAdminClientProvider(), kafkaConfig, kafkaLogging, kafkaVersion, allowReconfiguration);
    }

    public KafkaRoller(Vertx vertx, Reconciliation reconciliation, PodOperator podOperations,
                        long pollingIntervalMs, long operationTimeoutMs, Supplier<BackOff> backOffSupplier,
                        StatefulSet sts, Secret clusterCaCertSecret, Secret coKeySecret,
                        AdminClientProvider adminClientProvider,
                        String kafkaConfig, String kafkaLogging, KafkaVersion kafkaVersion, boolean allowReconfiguration) {
        this.namespace = sts.getMetadata().getNamespace();
        this.cluster = Labels.cluster(sts);
        this.numPods = sts.getSpec().getReplicas();
        this.backoffSupplier = backOffSupplier;
        this.clusterCaCertSecret = clusterCaCertSecret;
        this.coKeySecret = coKeySecret;
        this.vertx = vertx;
        this.operationTimeoutMs = operationTimeoutMs;
        this.podOperations = podOperations;
        this.pollingIntervalMs = pollingIntervalMs;
        this.adminClientProvider = adminClientProvider;
        this.kafkaConfig = kafkaConfig;
        this.kafkaLogging = kafkaLogging;
        this.kafkaVersion = kafkaVersion;
        this.reconciliation = reconciliation;
        this.allowReconfiguration = allowReconfiguration;
    }

    /**
     * Returns a Future which completed with the actual pod corresponding to the abstract representation
     * of the given {@code pod}.
     */
    protected Future<Pod> pod(Integer podId) {
        return podOperations.getAsync(namespace, KafkaCluster.kafkaPodName(cluster, podId));
    }

    private final ScheduledExecutorService singleExecutor = Executors.newSingleThreadScheduledExecutor(
        runnable -> new Thread(runnable, "kafka-roller"));

    private ConcurrentHashMap<Integer, RestartContext> podToContext = new ConcurrentHashMap<>();
    private Function<Pod, List<String>> podNeedsRestart;

    /**
     * If allClient has not been initialized yet, does exactly that
     * @return true if the creation of AC succeeded, false otherwise
     */
    private boolean initAdminClient() {
        if (this.allClient == null) {
            try {
                this.allClient = adminClient(IntStream.range(0, numPods).boxed().collect(Collectors.toList()), false);
            } catch (ForceableProblem | FatalProblem e) {
                return false;
            }
        }
        return true;
    }
        /**
     * Asynchronously perform a rolling restart of some subset of the pods,
     * completing the returned Future when rolling is complete.
     * Which pods get rolled is determined by {@code podNeedsRestart}.
     * The pods may not be rolled in id order, due to the {@linkplain KafkaRoller rolling algorithm}.
     * @param podNeedsRestart Predicate for determining whether a pod should be rolled.
     * @return A Future completed when rolling is complete.
     */
    public Future<Void> rollingRestart(Function<Pod, List<String>> podNeedsRestart) {
        this.podNeedsRestart = podNeedsRestart;

        Promise<Void> result = Promise.promise();
        singleExecutor.submit(() -> {
            List<Integer> podIds = new ArrayList<>(numPods);

            for (int podId = 0; podId < numPods; podId++) {
                // Order the podIds unready first otherwise repeated reconciliations might each restart a pod
                // only for it not to become ready and thus drive the cluster to a worse state.
                podIds.add(podOperations.isReady(namespace, podName(podId)) ? podIds.size() : 0, podId);
            }
            log.debug("{}: Initial order for rolling restart {}", reconciliation, podIds);
            List<Future> futures = new ArrayList<>(numPods);
            for (Integer podId: podIds) {
                futures.add(schedule(podId, 0, TimeUnit.MILLISECONDS));
            }
            CompositeFuture.join(futures).onComplete(ar -> {
                singleExecutor.shutdown();
                try {
                    if (allClient != null) {
                        allClient.close(Duration.ofSeconds(30));
                    }
                } catch (RuntimeException e) {
                    log.debug("{}: Exception closing admin client", reconciliation, e);
                }
                vertx.runOnContext(ignored -> result.handle(ar.map((Void) null)));
            });
        });
        return result.future();
    }

    protected static class RestartContext {
        final Promise<Void> promise;
        final BackOff backOff;
        private long connectionErrorStart = 0L;

        RestartContext(Supplier<BackOff> backOffSupplier) {
            promise = Promise.promise();
            backOff = backOffSupplier.get();
            backOff.delayMs();
        }

        public void clearConnectionError() {
            connectionErrorStart = 0L;
        }

        long connectionError() {
            return connectionErrorStart;
        }

        void noteConnectionError() {
            if (connectionErrorStart == 0L) {
                connectionErrorStart = System.currentTimeMillis();
            }
        }

        @Override
        public String toString() {
            return "RestartContext{" +
                    "promise=" + promise +
                    ", backOff=" + backOff +
                    '}';
        }
    }

    /**
     * Schedule the rolling of the given pod at or after the given delay,
     * completed the returned Future when the pod is rolled.
     * When called multiple times with the same podId this method will return the same Future instance.
     * Pods will be rolled one-at-a-time so the delay may be overrun.
     * @param podId The pod to roll.
     * @param delay The delay.
     * @param unit The unit of the delay.
     * @return A future which completes when the pod has been rolled.
     */
    private Future<Void> schedule(int podId, long delay, TimeUnit unit) {
        RestartContext ctx = podToContext.computeIfAbsent(podId,
            k -> new RestartContext(backoffSupplier));
        singleExecutor.schedule(() -> {
            if (reconciliation.isCancelled()) {
                log.info("{}: reconciliation cancelled, no longer considering restart of pod {}", reconciliation, podId);
                ctx.promise.complete();
                return;
            }

            log.debug("{}: Considering restart of pod {} after delay of {} {}", reconciliation, podId, delay, unit);

            try {
                restartIfNecessary(podId, ctx);
                ctx.promise.complete();
            } catch (InterruptedException e) {
                // Let the executor deal with interruption.
                Thread.currentThread().interrupt();
            } catch (FatalProblem e) {
                log.info("{}: Could not restart pod {}, giving up after {} attempts. Total delay between attempts {}ms",
                        reconciliation, podId, ctx.backOff.maxAttempts(), ctx.backOff.totalDelayMs(), e);
                ctx.promise.fail(e);
                singleExecutor.shutdownNow();
                podToContext.forEachValue(Integer.MAX_VALUE, f -> {
                    f.promise.tryFail(e);
                });
            } catch (Exception e) {
                if (ctx.backOff.done()) {
                    log.info("{}: Could not roll pod {}, giving up after {} attempts. Total delay between attempts {}ms",
                            reconciliation, podId, ctx.backOff.maxAttempts(), ctx.backOff.totalDelayMs(), e);
                    ctx.promise.fail(e instanceof TimeoutException ?
                            new io.strimzi.operator.common.operator.resource.TimeoutException() :
                            e);
                } else if (reconciliation.isCancelled()) {
                    log.info("{}: Could not roll pod {} due to {}, giving up due to cancelled reconciliation", reconciliation, podId, e);
                    ctx.promise.complete();
                } else {
                    long delay1 = ctx.backOff.delayMs();
                    log.info("{}: Could not roll pod {} due to {}, retrying after at least {}ms",
                            reconciliation, podId, e, delay1);
                    schedule(podId, delay1, TimeUnit.MILLISECONDS);
                }
            }
        }, delay, unit);
        return ctx.promise.future();
    }

    /** Described how the "restart" (which might actually just be a reconfigure) will be performed. */
    static class RestartPlan {
        private final boolean needsRestart;
        private final boolean needsReconfig;
        private final boolean forceRestart;
        private final KafkaBrokerConfigurationDiff diff;
        private final KafkaBrokerLoggingConfigurationDiff logDiff;

        public RestartPlan(boolean needsRestart, boolean needsReconfig, boolean forceRestart, KafkaBrokerConfigurationDiff diff, KafkaBrokerLoggingConfigurationDiff logDiff) {
            this.needsRestart = needsRestart;
            this.needsReconfig = needsReconfig;
            this.forceRestart = forceRestart;
            this.diff = diff;
            this.logDiff = logDiff;
        }

        public RestartPlan(boolean forceRestart) {
            this.needsRestart = false;
            this.needsReconfig = false;
            this.forceRestart = forceRestart;
            this.diff = null;
            this.logDiff = null;
        }
    }

    /**
     * Restart the given pod now if necessary according to {@link #podNeedsRestart}.
     * This method blocks.
     * @param podId The id of the pod to roll.
     * @param restartContext
     * @throws InterruptedException Interrupted while waiting.
     * @throws ForceableProblem Some error. Not thrown when finalAttempt==true.
     * @throws UnforceableProblem Some error, still thrown when finalAttempt==true.
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @SuppressWarnings({"checkstyle:CyclomaticComplexity"})
    private void restartIfNecessary(int podId, RestartContext restartContext)
            throws Exception {
        Pod pod;
        try {
            pod = podOperations.get(namespace, podName(podId));
        } catch (KubernetesClientException e) {
            throw new UnforceableProblem("Error getting pod " + podName(podId), e);
        }

        try {
            RestartPlan restartPlan = restartPlan(podId, pod, restartContext);
            if (restartPlan.forceRestart || restartPlan.needsRestart || restartPlan.needsReconfig) {
                if (!restartPlan.forceRestart && deferController(podId, restartContext)) {
                    log.debug("{}: Pod {} is controller and there are other pods to roll", reconciliation, podId);
                    throw new ForceableProblem("Pod " + podName(podId) + " is currently the controller and there are other pods still to roll");
                } else {
                    if (restartPlan.forceRestart || canRoll(podId, 60_000, TimeUnit.MILLISECONDS, false)) {
                        // Check for rollability before trying a dynamic update so that if the dynamic update fails we can go to a full restart
                        if (restartPlan.forceRestart || !maybeDynamicUpdateBrokerConfig(podId, restartPlan)) {
                            log.debug("{}: Pod {} can be rolled now", reconciliation, podId);
                            restartAndAwaitReadiness(pod, operationTimeoutMs, TimeUnit.MILLISECONDS);
                        } else {
                            awaitReadiness(pod, operationTimeoutMs, TimeUnit.MILLISECONDS);
                        }
                    } else {
                        log.debug("{}: Pod {} cannot be rolled right now", reconciliation, podId);
                        throw new UnforceableProblem("Pod " + podName(podId) + " is currently not rollable");
                    }
                }
            } else {
                // By testing even pods which don't need needsRestart for readiness we prevent successive reconciliations
                // from taking out a pod each time (due, e.g. to a configuration error).
                // We rely on Kube to try restarting such pods.
                log.debug("{}: Pod {} does not need to be restarted", reconciliation, podId);
                log.debug("{}: Waiting for non-restarted pod {} to become ready", reconciliation, podId);
                await(isReady(namespace, KafkaCluster.kafkaPodName(cluster, podId)), operationTimeoutMs, TimeUnit.MILLISECONDS, e -> new FatalProblem("Error while waiting for non-restarted pod " + podName(podId) + " to become ready", e));
                log.debug("{}: Pod {} is now ready", reconciliation, podId);
            }
        } catch (ForceableProblem e) {
            if (isPodStuck(pod) || restartContext.backOff.done() || e.forceNow) {
                if (canRoll(podId, 60_000, TimeUnit.MILLISECONDS, true)) {
                    log.warn("{}: Pod {} will be force-rolled, due to error: {}", reconciliation, podName(podId), e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
                    restartAndAwaitReadiness(pod, operationTimeoutMs, TimeUnit.MILLISECONDS);
                } else {
                    log.warn("{}: Pod {} can't be safely force-rolled; original error: ", reconciliation, podName(podId), e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
                    throw e;
                }
            } else {
                throw e;
            }
        }
    }

    private boolean podWaitingBecauseOfAnyReasons(Pod pod, Set<String> reasons) {
        if (pod != null && pod.getStatus() != null) {
            Optional<ContainerStatus> kafkaContainerStatus = pod.getStatus().getContainerStatuses().stream()
                    .filter(containerStatus -> containerStatus.getName().equals("kafka")).findFirst();
            if (kafkaContainerStatus.isPresent()) {
                ContainerStateWaiting waiting = kafkaContainerStatus.get().getState().getWaiting();
                if (waiting != null) {
                    return reasons.contains(waiting.getReason());
                }
            }
        }
        return false;
    }

    private boolean isPending(Pod pod) {
        if (pod != null && pod.getStatus() != null && "Pending".equals(pod.getStatus().getPhase())) {
            return true;
        }
        return false;
    }

    private boolean isPodStuck(Pod pod) {
        Set<String> set = new HashSet<>();
        set.add("CrashLoopBackOff");
        set.add("ImagePullBackOff");
        set.add("ContainerCreating");
        return isPending(pod) || podWaitingBecauseOfAnyReasons(pod, set);
    }

    /**
     * Dynamically update the broker config if the plan says we can.
     * Return false if the broker was successfully updated dynamically.
     */
    private boolean maybeDynamicUpdateBrokerConfig(int podId, RestartPlan restartPlan) throws InterruptedException {
        boolean updatedDynamically;

        if (restartPlan.needsReconfig) {
            try {
                dynamicUpdateBrokerConfig(podId, allClient, restartPlan.diff, restartPlan.logDiff);
                updatedDynamically = true;
            } catch (ForceableProblem e) {
                log.debug("{}: Pod {} could not be updated dynamically ({}), will restart", reconciliation, podId, e);
                updatedDynamically = false;
            }
        } else {
            updatedDynamically = false;
        }
        return updatedDynamically;
    }

    /**
     * Determine whether the pod should be restarted, or the broker reconfigured.
     */
    private RestartPlan restartPlan(int podId, Pod pod, RestartContext restartContext) throws ForceableProblem, InterruptedException, FatalProblem {

        List<String> reasonToRestartPod = Objects.requireNonNull(podNeedsRestart.apply(pod));
        boolean podStuck = pod != null
                && pod.getStatus() != null
                && "Pending".equals(pod.getStatus().getPhase())
                && pod.getStatus().getConditions().stream().filter(ps ->
                "PodScheduled".equals(ps.getType())
                        && "Unschedulable".equals(ps.getReason())
                        && "False".equals(ps.getStatus())).findFirst().isPresent();
        if (podStuck && !reasonToRestartPod.contains("Pod has old generation")) {
            // If the pod is unschedulable then deleting it, or trying to open an Admin client to it will make no difference
            // Treat this as fatal because if it's not possible to schedule one pod then it's likely that proceeding
            // and deleting a different pod in the meantime will likely result in another unschedulable pod.
            throw new FatalProblem("Pod is unschedulable");
        }
        // Unless the annotation is present, check the pod is at least ready.
        boolean needsRestart = !reasonToRestartPod.isEmpty();
        KafkaBrokerConfigurationDiff diff = null;
        KafkaBrokerLoggingConfigurationDiff loggingDiff = null;
        boolean needsReconfig = false;
        // Always get the broker config. This request gets sent to that specific broker, so it's a proof that we can
        // connect to the broker and that it's capable of responding.
        if (!initAdminClient()) {
            return new RestartPlan(true);
        }
        Config brokerConfig;
        try {
            brokerConfig = brokerConfig(podId);
        } catch (ForceableProblem e) {
            if (restartContext.backOff.done()) {
                needsRestart = true;
                brokerConfig = null;
            } else {
                throw e;
            }
        }

        if (!needsRestart && allowReconfiguration) {
            log.trace("{}: Broker {}: description {}", reconciliation, podId, brokerConfig);
            diff = new KafkaBrokerConfigurationDiff(brokerConfig, kafkaConfig, kafkaVersion, podId);
            loggingDiff = logging(podId);
            if (diff.getDiffSize() > 0) {
                if (diff.canBeUpdatedDynamically()) {
                    log.debug("{}: Pod {} needs to be reconfigured.", reconciliation, podId);
                    needsReconfig = true;
                } else {
                    log.debug("{}: Pod {} needs to be restarted, because reconfiguration cannot be done dynamically", reconciliation, podId);
                    needsRestart = true;
                }
            }

            if (loggingDiff.getDiffSize() > 0) {
                log.debug("{}: Pod {} logging needs to be reconfigured.", reconciliation, podId);
                needsReconfig = true;
            }
        } else if (needsRestart) {
            log.info("{}: Pod {} needs to be restarted. Reason: {}", reconciliation, podId, reasonToRestartPod);
        }
        return new RestartPlan(needsRestart, needsReconfig, podStuck, diff, loggingDiff);
    }

    /**
     * Returns a config of the given broker.
     * @param brokerId The id of the broker.
     * @return a Future which completes with the config of the given broker.
     */
    protected Config brokerConfig(int brokerId) throws ForceableProblem, InterruptedException {
        ConfigResource resource = Util.getBrokersConfig(brokerId);
        return await(Util.kafkaFutureToVertxFuture(vertx, allClient.describeConfigs(singletonList(resource)).values().get(resource)),
            30, TimeUnit.SECONDS,
            error -> {
                log.debug("Error getting broker config: {}", error.getMessage(), error);
                return new ForceableProblem("Error getting broker config", error);
            }
        );
    }

    /**
     * Returns logging of the given broker.
     * @param brokerId The id of the broker.
     * @return a Future which completes with the logging of the given broker.
     */
    protected Config brokerLogging(int brokerId) throws ForceableProblem, InterruptedException {
        ConfigResource resource = Util.getBrokersLogging(brokerId);
        return await(Util.kafkaFutureToVertxFuture(vertx, allClient.describeConfigs(singletonList(resource)).values().get(resource)),
                30, TimeUnit.SECONDS,
            error -> {
                log.debug("Error getting broker logging: {}", error.getMessage(), error);
                return new ForceableProblem("Error getting broker logging", error);
            }
        );
    }

    protected void dynamicUpdateBrokerConfig(int podId, Admin ac, KafkaBrokerConfigurationDiff configurationDiff, KafkaBrokerLoggingConfigurationDiff logDiff)
            throws ForceableProblem, InterruptedException {
        Map<ConfigResource, Collection<AlterConfigOp>> updatedConfig = new HashMap<>(2);
        updatedConfig.put(Util.getBrokersConfig(podId), configurationDiff.getConfigDiff());
        updatedConfig.put(Util.getBrokersLogging(podId), logDiff.getLoggingDiff());

        log.debug("{}: Altering broker configuration {}", reconciliation, podId);
        log.trace("{}: Altering broker configuration {} with {}", reconciliation, podId, updatedConfig);

        AlterConfigsResult alterConfigResult = ac.incrementalAlterConfigs(updatedConfig);
        KafkaFuture<Void> brokerConfigFuture = alterConfigResult.values().get(Util.getBrokersConfig(podId));
        KafkaFuture<Void> brokerLoggingConfigFuture = alterConfigResult.values().get(Util.getBrokersLogging(podId));
        await(Util.kafkaFutureToVertxFuture(vertx, brokerConfigFuture), 30, TimeUnit.SECONDS,
            error -> {
                log.error("Error doing dynamic config update", error);
                return new ForceableProblem("Error doing dynamic update", error);
            });
        await(Util.kafkaFutureToVertxFuture(vertx, brokerLoggingConfigFuture), 30, TimeUnit.SECONDS,
            error -> {
                log.error("Error performing dynamic logging update for pod {}", podId, error);
                return new ForceableProblem("Error performing dynamic logging update for pod " + podId, error);
            });

        log.info("{}: Dynamic reconfiguration for broker {} was successful.", reconciliation, podId);
    }

    private KafkaBrokerLoggingConfigurationDiff logging(int podId)
            throws ForceableProblem, InterruptedException {
        Config brokerLogging = brokerLogging(podId);
        log.trace("{}: Broker {}: logging description {}", reconciliation, podId, brokerLogging);
        return new KafkaBrokerLoggingConfigurationDiff(brokerLogging, kafkaLogging, podId);
    }

    /** Exceptions which we're prepared to ignore (thus forcing a restart) in some circumstances. */
    static final class ForceableProblem extends Exception {
        final boolean forceNow;
        ForceableProblem(String msg) {
            this(msg, null);
        }

        ForceableProblem(String msg, Throwable cause) {
            this(msg, cause, false);
        }

        ForceableProblem(String msg, boolean forceNow) {
            this(msg, null, forceNow);
        }

        ForceableProblem(String msg, Throwable cause, boolean forceNow) {
            super(msg, cause);
            this.forceNow = forceNow;
        }
    }

    /** Exceptions which we're prepared to ignore in the final attempt */
    static final class UnforceableProblem extends Exception {
        UnforceableProblem(String msg) {
            this(msg, null);
        }
        UnforceableProblem(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    /** Immediately aborts rolling */
    static final class FatalProblem extends Exception {
        public FatalProblem(String message) {
            super(message);
        }

        FatalProblem(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    private boolean canRoll(int podId, long timeout, TimeUnit unit, boolean ignoreSslError)
            throws ForceableProblem, InterruptedException {
        try {
            return await(availability(allClient).canRoll(podId), timeout, unit,
                t -> new ForceableProblem("An error while trying to determine rollability", t));
        } catch (ForceableProblem e) {
            // If we're not able to connect then roll
            if (ignoreSslError && e.getCause() instanceof SslAuthenticationException) {
                return true;
            } else {
                throw e;
            }
        }
    }

    /**
     * Synchronously restart the given pod
     * by deleting it and letting it be recreated by K8s, then synchronously wait for it to be ready.
     * @param pod The Pod to restart.
     * @param timeout The timeout.
     * @param unit The timeout unit.
     */
    private void restartAndAwaitReadiness(Pod pod, long timeout, TimeUnit unit)
            throws InterruptedException, UnforceableProblem, FatalProblem {
        String podName = pod.getMetadata().getName();
        log.debug("{}: Rolling pod {}", reconciliation, podName);
        await(restart(pod), timeout, unit, e -> new UnforceableProblem("Error while trying to restart pod " + podName + " to become ready", e));
        awaitReadiness(pod, timeout, unit);
    }

    private void awaitReadiness(Pod pod, long timeout, TimeUnit unit) throws FatalProblem, InterruptedException {
        String podName = pod.getMetadata().getName();
        log.debug("{}: Waiting for restarted pod {} to become ready", reconciliation, podName);
        await(isReady(pod), timeout, unit, e -> new FatalProblem("Error while waiting for restarted pod " + podName + " to become ready", e));
        log.debug("{}: Pod {} is now ready", reconciliation, podName);
    }

    /**
     * Block waiting for up to the given timeout for the given Future to complete, returning its result.
     * @param future The future to wait for.
     * @param timeout The timeout
     * @param unit The timeout unit
     * @param exceptionMapper A function for rethrowing exceptions.
     * @param <T> The result type
     * @param <E> The exception type
     * @return The result of of the future
     * @throws E The exception type returned from {@code exceptionMapper}.
     * @throws TimeoutException If the given future is not completed before the timeout.
     * @throws InterruptedException If the waiting was interrupted.
     */
    private static <T, E extends Exception> T await(Future<T> future, long timeout, TimeUnit unit,
                                            Function<Throwable, E> exceptionMapper)
            throws E, InterruptedException {
        CompletableFuture<T> cf = new CompletableFuture<>();
        future.onComplete(ar -> {
            if (ar.succeeded()) {
                cf.complete(ar.result());
            } else {
                cf.completeExceptionally(ar.cause());
            }
        });
        try {
            return cf.get(timeout, unit);
        } catch (ExecutionException e) {
            throw exceptionMapper.apply(e.getCause());
        } catch (TimeoutException e) {
            throw exceptionMapper.apply(e);
        }
    }

    /**
     * Asynchronously delete the given pod, return a Future which completes when the Pod has been recreated.
     * Note: The pod might not be "ready" when the returned Future completes.
     * @param pod The pod to be restarted
     * @return a Future which completes when the Pod has been recreated
     */
    protected Future<Void> restart(Pod pod) {
        return podOperations.restart("Rolling update of " + namespace + "/" + KafkaCluster.kafkaClusterName(cluster), pod, operationTimeoutMs);
    }

    /**
     * Returns an AdminClient instance bootstrapped from the given pod.
     */
    protected Admin adminClient(List<Integer> bootstrapPods, boolean ceShouldBeFatal) throws ForceableProblem, FatalProblem {
        List<String> podNames = bootstrapPods.stream().map(podId -> podName(podId)).collect(Collectors.toList());
        try {
            String bootstrapHostnames = podNames.stream().map(podName -> KafkaCluster.podDnsName(this.namespace, this.cluster, podName) + ":" + KafkaCluster.REPLICATION_PORT).collect(Collectors.joining(","));
            log.debug("{}: Creating AdminClient for {}", reconciliation, bootstrapHostnames);
            return adminClientProvider.createAdminClient(bootstrapHostnames, this.clusterCaCertSecret, this.coKeySecret, "cluster-operator");
        } catch (KafkaException e) {
            if (ceShouldBeFatal && (e instanceof ConfigException
                    || e.getCause() instanceof ConfigException)) {
                throw new FatalProblem("An error while try to create an admin client with bootstrap brokers " + podNames, e);
            } else {
                throw new ForceableProblem("An error while try to create an admin client with bootstrap brokers " + podNames, e);
            }
        } catch (RuntimeException e) {
            throw new ForceableProblem("An error while try to create an admin client with bootstrap brokers " + podNames, e);
        }
    }

    protected KafkaAvailability availability(Admin ac) {
        return new KafkaAvailability(ac, reconciliation);
    }

    String podName(int podId) {
        return KafkaCluster.kafkaPodName(this.cluster, podId);
    }

    /**
     * Return true if the given {@code podId} is the controller and there are other brokers we might yet have to consider.
     * This ensures that the controller is restarted/reconfigured last.
     */
    private boolean deferController(int podId, RestartContext restartContext) throws Exception {
        Integer controller = controller(podId, operationTimeoutMs, TimeUnit.MILLISECONDS, restartContext);
        int stillRunning = podToContext.reduceValuesToInt(100, v -> v.promise.future().isComplete() ? 0 : 1,
                0, Integer::sum);
        return controller == podId && stillRunning > 1;
    }

    /**
     * Completes the returned future <strong>on the context thread</strong> with the id of the controller of the cluster.
     * This will be -1 if there is not currently a controller.
     * @return A future which completes the the node id of the controller of the cluster,
     * or -1 if there is not currently a controller.
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE") // seems to be completely spurious
    int controller(int podId, long timeout, TimeUnit unit, RestartContext restartContext) throws Exception {
        // Don't use all allClient here, because it will have cache metadata about which is the controller.
        try (Admin ac = adminClient(singletonList(podId), false)) {
            Node controllerNode = null;
            try {
                DescribeClusterResult describeClusterResult = ac.describeCluster();
                KafkaFuture<Node> controller = describeClusterResult.controller();
                controllerNode = controller.get(timeout, unit);
                restartContext.clearConnectionError();
            } catch (ExecutionException | TimeoutException e) {
                maybeTcpProbe(podId, e, restartContext);
            }
            int id = controllerNode == null || Node.noNode().equals(controllerNode) ? -1 : controllerNode.id();
            log.debug("{}: Controller is {}", reconciliation, id);
            return id;
        }
    }

    /**
     * If we've already had trouble connecting to this broker try to probe whether the connection is
     * open on the broker; if it's not then maybe throw a ForceableProblem to immediately force a restart.
     * This is an optimization for brokers which don't seem to be running.
     */
    private void maybeTcpProbe(int podId, Exception executionException, RestartContext restartContext) throws Exception {
        if (restartContext.connectionError() + numPods * 120_000L >= System.currentTimeMillis()) {
            try {
                log.debug("{}: Probing TCP port due to previous problems connecting to pod {}", reconciliation, podId);
                // do a tcp connect and close (with a short connect timeout)
                tcpProbe(podName(podId), KafkaCluster.REPLICATION_PORT);
            } catch (IOException connectionException) {
                throw new ForceableProblem("Unable to connect to " + podName(podId) + ":" + KafkaCluster.REPLICATION_PORT, executionException.getCause(), true);
            }
            throw executionException;
        } else {
            restartContext.noteConnectionError();
            throw new ForceableProblem("Error while trying to determine the cluster controller from pod " + podName(podId), executionException.getCause());
        }
    }

    /**
     * Tries to open and close a TCP connection to the given host and port.
     * @param hostname The host
     * @param port The port
     * @throws IOException if anything went wrong.
     */
    void tcpProbe(String hostname, int port) throws IOException {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(hostname, port), 5_000);
        } finally {
            socket.close();
        }
    }

    @Override
    public String toString() {
        return podToContext.toString();
    }

    protected Future<Void> isReady(Pod pod) {
        return isReady(pod.getMetadata().getNamespace(), pod.getMetadata().getName());
    }

    protected Future<Void> isReady(String namespace, String podName) {
        return podOperations.readiness(namespace, podName, pollingIntervalMs, operationTimeoutMs)
            .recover(error -> {
                log.warn("{}: Error waiting for pod {}/{} to become ready: {}", reconciliation, namespace, podName, error);
                return Future.failedFuture(error);
            });
    }

}
