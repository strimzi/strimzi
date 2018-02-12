/*
 * Copyright 2018 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.controller.topic;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class K8sImpl implements K8s {

    private final static Logger logger = LoggerFactory.getLogger(Controller.class);

    private final LabelPredicate cmPredicate;
    private final String namespace;

    private KubernetesClient client;

    private Vertx vertx;

    public K8sImpl(Vertx vertx, KubernetesClient client, LabelPredicate cmPredicate, String namespace) {
        this.vertx = vertx;
        this.client = client;
        this.cmPredicate = cmPredicate;
        this.namespace = namespace;
    }

    @Override
    public void createConfigMap(ConfigMap cm, Handler<AsyncResult<Void>> handler) {
        vertx.executeBlocking(future -> {
            try {
                client.configMaps().inNamespace(namespace).create(cm);
                future.complete();
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
    }

    @Override
    public void updateConfigMap(ConfigMap cm, Handler<AsyncResult<Void>> handler) {
        vertx.executeBlocking(future -> {
            try {
                client.configMaps().inNamespace(namespace).createOrReplace(cm);
                future.complete();
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
    }

    @Override
    public void deleteConfigMap(MapName mapName, Handler<AsyncResult<Void>> handler) {
        vertx.executeBlocking(future -> {
            try {
                // Delete the CM by the topic name, because neither ZK nor Kafka know the CM name
                client.configMaps().inNamespace(namespace).withName(mapName.toString()).delete();
                future.complete();
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
    }

    @Override
    public void listMaps(Handler<AsyncResult<List<ConfigMap> >> handler) {
        vertx.executeBlocking(future -> {
            try {
                future.complete(client.configMaps().inNamespace(namespace).withLabels(cmPredicate.labels()).list().getItems());
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
    }

    @Override
    public void getFromName(MapName mapName, Handler<AsyncResult<ConfigMap >> handler) {
        vertx.executeBlocking(future -> {
            try {
                future.complete(client.configMaps().inNamespace(namespace).withName(mapName.toString()).get());
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);

    }

    /**
     * Create the given k8s event
     */
    @Override
    public void createEvent(Event event, Handler<AsyncResult<Void>> handler) {
        vertx.executeBlocking(future -> {
            try {
                try {
                    logger.debug("Creating event {}", event);
                    client.events().create(event);
                } catch (KubernetesClientException e) {
                    logger.error("Error creating event {}", event, e);
                }
                future.complete();
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
    }
}
