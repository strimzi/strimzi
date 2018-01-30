package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.Build;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigList;
import io.fabric8.openshift.api.model.DoneableBuildConfig;
import io.fabric8.openshift.api.model.DoneableImageStream;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.BuildConfigResource;
import io.strimzi.controller.cluster.operations.ResourceOperation;
import io.strimzi.controller.cluster.resources.Source2Image;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Updates all Source2Image resources
 */
public class UpdateS2IOperation extends S2IOperation {

    private static final Logger log = LoggerFactory.getLogger(UpdateS2IOperation.class.getName());
    private final ResourceOperation<OpenShiftClient, BuildConfig, BuildConfigList, DoneableBuildConfig, BuildConfigResource<BuildConfig, DoneableBuildConfig, Void, Build>> buildConfigOperation;
    private final ResourceOperation<OpenShiftClient, ImageStream, ImageStreamList, DoneableImageStream, Resource<ImageStream, DoneableImageStream>> buildImageStreamOperation;

    /**
     * Constructor
     */
    public UpdateS2IOperation(Vertx vertx, OpenShiftClient client) {
        super(vertx, "update");
        buildConfigOperation = ResourceOperation.buildConfig(vertx, client);
        buildImageStreamOperation = ResourceOperation.imageStream(vertx, client);
    }


    @Override
    protected List<Future> futures(Source2Image s2i) {
        if (s2i.diff(buildImageStreamOperation, buildConfigOperation).getDifferent()) {
            List<Future> result = new ArrayList<>(3);
            Future<Void> futureSourceImageStream = Future.future();

            buildImageStreamOperation.patch(
                    s2i.getNamespace(), s2i.getSourceImageStreamName(),
                    s2i.patchSourceImageStream(
                            buildImageStreamOperation.get(s2i.getNamespace(), s2i.getSourceImageStreamName())),
                    futureSourceImageStream.completer());
            result.add(futureSourceImageStream);

            Future<Void> futureTargetImageStream = Future.future();
            buildImageStreamOperation
                    .patch(s2i.getNamespace(), s2i.getName(),
                            s2i.patchTargetImageStream(
                                    buildImageStreamOperation.get(s2i.getNamespace(), s2i.getName())),
                            futureTargetImageStream.completer());
            result.add(futureTargetImageStream);

            Future<Void> futureBuildConfig = Future.future();
            buildConfigOperation
                    .patch(s2i.getNamespace(), s2i.getName(),
                            s2i.patchBuildConfig(
                                    buildConfigOperation.get(s2i.getNamespace(), s2i.getName())),
                            futureBuildConfig.completer());
            result.add(futureBuildConfig);

            return result;
        } else {
            log.info("No S2I {} differences found in namespace {}", s2i.getName(), s2i.getNamespace());
            return Collections.emptyList();
        }
    }
}
