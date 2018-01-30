package io.strimzi.controller.cluster.operations;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.kubernetes.ManualRollingUpdateOperation;
import io.strimzi.controller.cluster.operations.kubernetes.ScaleDownOperation;
import io.strimzi.controller.cluster.operations.kubernetes.ScaleUpOperation;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.strimzi.controller.cluster.resources.Storage;
import io.strimzi.controller.cluster.resources.ZookeeperCluster;
import io.vertx.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.shareddata.Lock;

import java.util.ArrayList;
import java.util.List;

public class ZookeeperClusterOperation extends ClusterOperation<ZookeeperCluster> {

    private static final Logger log = LoggerFactory.getLogger(ZookeeperClusterOperation.class.getName());

    public ZookeeperClusterOperation(Vertx vertx, K8SUtils k8s) {
        super(vertx, k8s, "zookeeper", "create");
    }

    private final Op<ZookeeperCluster> create = new Op<ZookeeperCluster>() {

        @Override
        public ZookeeperCluster getCluster(K8SUtils k8s, String namespace, String name) {
            return ZookeeperCluster.fromConfigMap(k8s.getConfigmap(namespace, name));
        }

        @Override
        public List<Future> futures(K8SUtils k8s, String namespace, ZookeeperCluster zk) {
            List<Future> result = new ArrayList<>(4);

            if (zk.isMetricsEnabled()) {
                Future<Void> futureConfigMap = Future.future();
                ResourceOperation.configMap(vertx, k8s.getKubernetesClient()).create(zk.generateMetricsConfigMap(), futureConfigMap.completer());
                result.add(futureConfigMap);
            }

            Future<Void> futureService = Future.future();
            ResourceOperation.service(vertx, k8s.getKubernetesClient()).create(zk.generateService(), futureService.completer());
            result.add(futureService);

            Future<Void> futureHeadlessService = Future.future();
            ResourceOperation.service(vertx, k8s.getKubernetesClient()).create(zk.generateHeadlessService(), futureHeadlessService.completer());
            result.add(futureHeadlessService);

            Future<Void> futureStatefulSet = Future.future();
            ResourceOperation.statefulSet(vertx, k8s.getKubernetesClient()).create(zk.generateStatefulSet(k8s.isOpenShift()), futureStatefulSet.completer());
            result.add(futureStatefulSet);

            return result;
        }
    };

    @Override
    protected Op<ZookeeperCluster> createOp() {
        return create;
    }

    private final Op<ZookeeperCluster> delete = new Op<ZookeeperCluster>() {
        @Override
        public List<Future> futures(K8SUtils k8s, String namespace, ZookeeperCluster zk) {
            boolean deleteClaims = zk.getStorage().type() == Storage.StorageType.PERSISTENT_CLAIM
                    && zk.getStorage().isDeleteClaim();
            List<Future> result = new ArrayList<>(4 + (deleteClaims ? zk.getReplicas() : 0));

            // start deleting configMap operation only if metrics are enabled,
            // otherwise the future is already complete (for the "join")
            if (zk.isMetricsEnabled()) {
                Future<Void> futureConfigMap = Future.future();
                ResourceOperation.configMap(vertx, k8s.getKubernetesClient()).delete(namespace, zk.getMetricsConfigName(), futureConfigMap.completer());
                result.add(futureConfigMap);
            }

            Future<Void> futureService = Future.future();
            ResourceOperation.service(vertx, k8s.getKubernetesClient()).delete(namespace, zk.getName(), futureService.completer());
            result.add(futureService);

            Future<Void> futureHeadlessService = Future.future();
            ResourceOperation.service(vertx, k8s.getKubernetesClient()).delete(namespace, zk.getHeadlessName(), futureHeadlessService.completer());
            result.add(futureHeadlessService);

            Future<Void> futureStatefulSet = Future.future();
            ResourceOperation.statefulSet(vertx, k8s.getKubernetesClient()).delete(namespace, zk.getName(), futureStatefulSet.completer());
            result.add(futureStatefulSet);


            if (deleteClaims) {
                for (int i = 0; i < zk.getReplicas(); i++) {
                    Future<Void> f = Future.future();
                    ResourceOperation.persistentVolumeClaim(vertx, k8s.getKubernetesClient()).delete(namespace, zk.getVolumeName() + "-" + zk.getName() + "-" + i, f.completer());
                    result.add(f);
                }
            }
            return result;
        }

        @Override
        public ZookeeperCluster getCluster(K8SUtils k8s, String namespace, String name) {
            return ZookeeperCluster.fromStatefulSet(k8s, namespace, name);
        }
    };


    @Override
    protected Op<ZookeeperCluster> deleteOp() {
        return delete;
    }

    public void update(String namespace, String name, Handler<AsyncResult<Void>> handler) {

        final String lockName = getLockName(namespace, name);
        vertx.sharedData().getLockWithTimeout(lockName, LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                ClusterDiffResult diff;
                ZookeeperCluster zk;
                ConfigMap zkConfigMap = k8s.getConfigmap(namespace, name);

                if (zkConfigMap != null)    {

                    try {

                        zk = ZookeeperCluster.fromConfigMap(zkConfigMap);
                        log.info("Updating Zookeeper cluster {} in namespace {}", zk.getName(), namespace);
                        diff = zk.diff(k8s, namespace);

                    } catch (Exception ex) {

                        log.error("Error while parsing cluster ConfigMap", ex);
                        handler.handle(Future.failedFuture("ConfigMap parsing error"));
                        lock.release();
                        return;
                    }

                } else {
                    log.error("ConfigMap {} doesn't exist anymore in namespace {}", name, namespace);
                    handler.handle(Future.failedFuture("ConfigMap doesn't exist anymore"));
                    lock.release();
                    return;
                }

                Future<Void> chainFuture = Future.future();

                scaleDown(zk, namespace, diff)
                        .compose(i -> patchService(zk, namespace, diff))
                        .compose(i -> patchHeadlessService(zk, namespace, diff))
                        .compose(i -> patchStatefulSet(zk, namespace, diff))
                        .compose(i -> patchMetricsConfigMap(zk, namespace, diff))
                        .compose(i -> rollingUpdate(zk, namespace, diff))
                        .compose(i -> scaleUp(zk, namespace, diff))
                        .compose(chainFuture::complete, chainFuture);

                chainFuture.setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("Zookeeper cluster {} successfully updated in namespace {}", zk.getName(), namespace);
                        handler.handle(Future.succeededFuture());
                        lock.release();
                    } else {
                        log.error("Zookeeper cluster {} failed to update in namespace {}", zk.getName(), namespace);
                        handler.handle(Future.failedFuture("Failed to update Zookeeper cluster"));
                        lock.release();
                    }
                });
            } else {
                log.error("Failed to acquire lock to create Zookeeper cluster {}", lockName);
                handler.handle(Future.failedFuture("Failed to acquire lock to create Zookeeper cluster"));
            }
        });
    }

    private Future<Void> scaleDown(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        Future<Void> scaleDown = Future.future();

        if (diff.getScaleDown())    {
            log.info("Scaling down stateful set {} in namespace {}", zk.getName(), namespace);
            new ScaleDownOperation(k8s.getStatefulSetResource(namespace, zk.getName()), zk.getReplicas()).scaleDown(vertx, k8s, scaleDown.completer());
        }
        else {
            scaleDown.complete();
        }

        return scaleDown;
    }

    private Future<Void> patchService(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchService = Future.future();
            ResourceOperation.service(vertx, k8s.getKubernetesClient()).patch(k8s.getServiceResource(namespace, zk.getName()), zk.patchService(k8s.getService(namespace, zk.getName())), patchService.completer());
            return patchService;
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchHeadlessService(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchService = Future.future();
            ResourceOperation.service(vertx, k8s.getKubernetesClient()).patch(k8s.getServiceResource(namespace, zk.getHeadlessName()), zk.patchHeadlessService(k8s.getService(namespace, zk.getHeadlessName())), patchService.completer());
            return patchService;
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchStatefulSet(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchStatefulSet = Future.future();
            ResourceOperation.statefulSet(vertx, k8s.getKubernetesClient()).patch(k8s.getStatefulSetResource(namespace, zk.getName()).cascading(false), zk.patchStatefulSet(k8s.getStatefulSet(namespace, zk.getName())), patchStatefulSet.completer());
            return patchStatefulSet;
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchMetricsConfigMap(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        if (diff.isMetricsChanged()) {
            Future<Void> patchConfigMap = Future.future();
            ResourceOperation.configMap(vertx, k8s.getKubernetesClient()).patch(k8s.getConfigmapResource(namespace, zk.getMetricsConfigName()),
                    zk.patchMetricsConfigMap(k8s.getConfigmap(namespace, zk.getMetricsConfigName())),
                    patchConfigMap.completer());
            return patchConfigMap;
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> rollingUpdate(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        Future<Void> rollingUpdate = Future.future();

        if (diff.getRollingUpdate()) {
            new ManualRollingUpdateOperation(namespace, zk.getName(), k8s.getStatefulSet(namespace, zk.getName()).getSpec().getReplicas()).rollingUpdate(vertx, k8s, rollingUpdate.completer());
        }
        else {
            rollingUpdate.complete();
        }

        return rollingUpdate;
    }

    private Future<Void> scaleUp(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        Future<Void> scaleUp = Future.future();

        if (diff.getScaleUp()) {
            new ScaleUpOperation(k8s.getStatefulSetResource(namespace, zk.getName()), zk.getReplicas()).scaleUp(vertx, k8s, scaleUp.completer());
        }
        else {
            scaleUp.complete();
        }

        return scaleUp;
    }
}
