/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.externalClients;

import io.vertx.core.AbstractVerticle;

import java.util.concurrent.CompletableFuture;
import java.util.function.IntPredicate;

public abstract class ClientHandlerBase<T> extends AbstractVerticle {

    final CompletableFuture<T> resultPromise;
    final IntPredicate msgCntPredicate;

    public ClientHandlerBase(CompletableFuture<T> resultPromise, IntPredicate msgCntPredicate) {
        this.resultPromise = resultPromise;
        this.msgCntPredicate = msgCntPredicate;
    }

    @Override
    public void start() {
        handleClient();
    }

    protected abstract void handleClient();

    public CompletableFuture<T> getResultPromise() {
        return resultPromise;
    }
}