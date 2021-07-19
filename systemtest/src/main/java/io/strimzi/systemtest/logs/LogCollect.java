/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.logs;

public interface LogCollect {

    void collectLogsFromPods();
    void collectEvents();
    void collectConfigMaps();
    void collectDeployments();
    void collectStatefulSets();
    void collectReplicaSets();
}
