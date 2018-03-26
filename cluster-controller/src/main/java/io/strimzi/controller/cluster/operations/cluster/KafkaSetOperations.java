/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.controller.cluster.operations.resource.ReconcileResult;
import io.strimzi.controller.cluster.operations.resource.StatefulSetOperations;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Specialization of {@link StatefulSetOperations} for StatefulSets of Kafka brokers
 */
public class KafkaSetOperations extends StatefulSetOperations<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(KafkaSetOperations.class);

    /**
     * Constructor
     *
     * @param vertx  The Vertx instance
     * @param client The Kubernetes client
     */
    public KafkaSetOperations(Vertx vertx, KubernetesClient client) {
        super(vertx, client);
    }

    @Override
    protected Future<ReconcileResult<Boolean>> internalPatch(String namespace, String name, StatefulSet current, StatefulSet desired) {
        StatefulSetDiff diff = new StatefulSetDiff(current, desired);
        if (diff.changesVolumeClaimTemplates()) {
            log.warn("Ignoring change to volumeClaim");
            desired.getSpec().setVolumeClaimTemplates(current.getSpec().getVolumeClaimTemplates());
            diff = new StatefulSetDiff(current, desired);
        }
        if (diff.isEmpty()) {
            return Future.succeededFuture(ReconcileResult.noop());
        } else {
            boolean different = needsRollingUpdate(diff);
            return super.internalPatch(namespace, name, current, desired).map(r -> {
                if (r instanceof ReconcileResult.Patched) {
                    return ReconcileResult.patched(different);
                } else {
                    return r;
                }
            });
        }
    }

    static boolean needsRollingUpdate(StatefulSetDiff diff) {
        return diff.changesLabels()
                    || diff.changesSpecTemplateSpec();
    }
}
