/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.DoneableEndpoints;
import io.fabric8.kubernetes.api.model.DoneablePersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointsList;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentList;
import io.fabric8.kubernetes.api.model.extensions.DoneableDeployment;
import io.fabric8.kubernetes.api.model.extensions.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.EditReplacePatchDeletable;
import io.fabric8.kubernetes.client.dsl.ExtensionsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import org.mockito.stubbing.OngoingStubbing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockKube {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockKube.class);

    private final Map<String, ConfigMap> cmDb = db(emptySet(), ConfigMap.class, DoneableConfigMap.class);
    private final Collection<Watcher<ConfigMap>> cmWatchers = new ArrayList();
    private final Map<String, PersistentVolumeClaim> pvcDb = db(emptySet(), PersistentVolumeClaim.class, DoneablePersistentVolumeClaim.class);
    private final Map<String, Service> svcDb = db(emptySet(), Service.class, DoneableService.class);
    private final Map<String, Endpoints> endpointDb = db(emptySet(), Endpoints.class, DoneableEndpoints.class);
    private final Map<String, Pod> podDb = db(emptySet(), Pod.class, DoneablePod.class);
    private final Collection<Watcher<Pod>> podWatchers = new HashSet<>();
    private final Map<String, StatefulSet> ssDb = db(emptySet(), StatefulSet.class, DoneableStatefulSet.class);
    private final Map<String, Deployment> depDb = db(emptySet(), Deployment.class, DoneableDeployment.class);

    public MockKube withInitialCms(Set<ConfigMap> initialCms) {
        this.cmDb.putAll(db(initialCms, ConfigMap.class, DoneableConfigMap.class));
        return this;
    }

    public KubernetesClient build() {
        KubernetesClient mockClient = mock(KubernetesClient.class);
        MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> mockCms = mockCms();
        MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> mockPvcs = mockPvcs();
        MixedOperation<Endpoints, EndpointsList, DoneableEndpoints, Resource<Endpoints, DoneableEndpoints>> mockEndpoints = mockEndpoints();
        MixedOperation<Service, ServiceList, DoneableService, Resource<Service, DoneableService>> mockSvc = mockSvc();
        MixedOperation<StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>> mockSs = mockSs();
        MixedOperation<Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>> mockDep = mockDeployment();
        MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> mockPods = mockPods();

        when(mockClient.configMaps()).thenReturn(mockCms);

        when(mockClient.services()).thenReturn(mockSvc);
        AppsAPIGroupDSL api = mock(AppsAPIGroupDSL.class);

        when(api.statefulSets()).thenReturn(mockSs);
        when(mockClient.apps()).thenReturn(api);
        ExtensionsAPIGroupDSL ext = mock(ExtensionsAPIGroupDSL.class);
        when(mockClient.extensions()).thenReturn(ext);
        when(ext.deployments()).thenReturn(mockDep);
        when(mockClient.pods()).thenReturn(mockPods);
        when(mockClient.endpoints()).thenReturn(mockEndpoints);
        when(mockClient.persistentVolumeClaims()).thenReturn(mockPvcs);

        return mockClient;
    }

    private static <T extends HasMetadata, D extends Doneable<T>> Map<String, T> db(Collection<T> initialResources, Class<T> cls, Class<D> doneableClass) {
        return new HashMap(initialResources.stream().collect(Collectors.toMap(
            c -> c.getMetadata().getName(),
            c -> copyResource(c, cls, doneableClass))));
    }

    private static <T extends HasMetadata, D extends Doneable<T>> T copyResource(T resource, Class<T> resourceClass, Class<D> doneableClass) {
        try {
            D doneableInstance = doneableClass.getDeclaredConstructor(resourceClass).newInstance(resource);
            T done = (T) Doneable.class.getMethod("done").invoke(doneableInstance);
            return done;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This method is just used to appease javac and avoid having a very ugly "double cast" (cast to raw Class,
     * followed by a cast to parameterised Class) in all the calls to
     * {@link #crudMock(String, Map, Class, Class, BiConsumer)}
     */
    @SuppressWarnings("unchecked")
    private static <T extends HasMetadata, D extends Doneable<T>, R extends Resource<T, D>, R2 extends Resource>
            Class<R> castClass(Class<R2> c) {
        return (Class) c;
    }

    /**
     * Generate a stateful mock for CRUD-like interactions.
     * @param db In-memory db of resources (e.g. ConfigMap's by their name)
     * @param resourceClass The type of {@link Resource} class
     * @param extraMocksOnResource A callback for adding extra mocks to the mock for the Resource type.
     *                             This is necessary for those things like scalable and "ready-able" resources.
     * @param <CM> The type of resource (e.g. ConfigMap)
     * @param <CML> The type of listable resource
     * @param <DCM> The type of doneable resource
     * @param <R> The type of the Resource
     * @return The mock
     */
    private <CM extends HasMetadata,
            CML extends KubernetesResource<CM> & KubernetesResourceList<CM>,
            DCM extends Doneable<CM>,
            R extends Resource<CM, DCM>>
                MixedOperation<CM, CML, DCM, R> crudMock(String resourceType, Map<String, CM> db,
                                                         Class<R> resourceClass,
                                                         Class<CML> listClass,
                                                         BiConsumer<R, String> extraMocksOnResource) {

        MixedOperation<CM, CML, DCM, R> mixed = mock(MixedOperation.class);

        when(mixed.inNamespace(any())).thenReturn(mixed);
        when(mixed.list()).thenAnswer(i -> {
            KubernetesResourceList<CM> l = mock(listClass);
            when(l.getItems()).thenAnswer(i3 -> {
                List<CM> r = new ArrayList<>(db.values());
                LOGGER.debug("{} list -> {}", resourceType, r);
                return r;
            });
            return l;
        });
        when(mixed.withName(any())).thenAnswer(invocation -> {
            String resourceName = invocation.getArgument(0);
            R resource = mock(resourceClass);
            extraMocksOnResource.accept(resource, resourceName);
            return resource;
        });
        return mixed;
    }

    private <CM extends HasMetadata, DCM extends Doneable<CM>, R extends Resource<CM, DCM>>
        void mockDelete(String resourceType, Map<String, CM> db, Collection<Watcher<CM>> watchers, String resourceName, R resource) {
        when(resource.delete()).thenAnswer(i -> {
            LOGGER.debug("delete {} {}", resourceType, resourceName);
            CM removed = db.remove(resourceName);
            if (removed != null && watchers != null) {
                for (Watcher<CM> watcher : watchers) {
                    watcher.eventReceived(Watcher.Action.DELETED, removed);
                }
            }
            return removed != null;
        });
    }

    private <CM extends HasMetadata, DCM extends Doneable<CM>, R extends Resource<CM, DCM>>
        void mockPatch(String resourceType, Map<String, CM> db, Collection<Watcher<CM>> watchers, String resourceName, R resource) {
        when(resource.patch(any())).thenAnswer(i -> {
            checkDoesExist(db, resourceType, resourceName);
            CM argument = i.getArgument(0);
            LOGGER.debug("patch {} {} -> {}", resourceType, resourceName, resource);
            db.put(resourceName, argument);
            if (watchers != null) {
                for (Watcher<CM> watcher : watchers) {
                    watcher.eventReceived(Watcher.Action.MODIFIED, argument);
                }
            }
            return argument;
        });
    }

    private <CM extends HasMetadata, DCM extends Doneable<CM>, R extends Resource<CM, DCM>>
        void mockCascading(R resource) {
        EditReplacePatchDeletable<CM, CM, DCM, Boolean> c = mock(EditReplacePatchDeletable.class);
        when(resource.cascading(true)).thenReturn(c);
    }

    private <CM extends HasMetadata, DCM extends Doneable<CM>, R extends Resource<CM, DCM>>
    void mockWatch(String resourceType, Collection<Watcher<CM>> watchers, R resource) {
        when(resource.watch(any())).thenAnswer(i -> {
            Watcher<CM> argument = (Watcher<CM>) i.getArguments()[0];
            LOGGER.debug("watch {} {} ", resourceType, argument);
            watchers.add(argument);
            Watch watch = mock(Watch.class);
            doAnswer(z -> {
                watchers.remove(argument);
                return null;
            }).when(watch).close();
            return watch;
        });
    }

    private <CM extends HasMetadata, DCM extends Doneable<CM>, R extends Resource<CM, DCM>>
        void mockCreate(String resourceType, Map<String, CM> db, Collection<Watcher<CM>> watchers, String resourceName, R resource) {
        when(resource.create(any())).thenAnswer(i -> {
            checkNotExists(db, resourceType, resourceName);
            CM argument = (CM) i.getArguments()[0];
            LOGGER.debug("create {} {} -> {}", resourceType, resourceName, argument);
            db.put(resourceName, argument);
            if (watchers != null) {
                for (Watcher<CM> watcher : watchers) {
                    watcher.eventReceived(Watcher.Action.ADDED, argument);
                }
            }
            return argument;
        });
    }

    private <CM extends HasMetadata, DCM extends Doneable<CM>, R extends Resource<CM, DCM>>
        OngoingStubbing<CM> mockGet(String resourceType, Map<String, CM> db, String resourceName, R resource) {
        return when(resource.get()).thenAnswer(i -> {
            CM r = db.get(resourceName);
            LOGGER.debug("{} {} get {}", resourceType, resourceName, r);
            return r;
        });
    }

    private <CM extends HasMetadata, DCM extends Doneable<CM>, R extends Resource<CM, DCM>>
        OngoingStubbing<Boolean> mockIsReady(String resourceType, String resourceName, R resource) {
        return when(resource.isReady()).thenAnswer(i -> {
            LOGGER.debug("{} {} is ready", resourceType, resourceName);
            return Boolean.TRUE;
        });
    }


    // ConfigMaps
    private MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> mockCms() {
        String resourceType = "configmap";
        return crudMock(resourceType, this.cmDb,
            castClass(Resource.class),
            ConfigMapList.class,
            (resource, resourceName) -> {
                mockGet(resourceType, cmDb, resourceName, resource);
                mockWatch(resourceType, cmWatchers, resource);
                mockCreate(resourceType, cmDb, cmWatchers, resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceType, cmDb, cmWatchers, resourceName, resource);
                mockDelete(resourceType, cmDb, cmWatchers, resourceName, resource);
            });
    }

    // Pvcs
    private MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> mockPvcs() {
        String resourceType = "persistenvolumeclaim";
        return crudMock(resourceType, this.pvcDb,
            castClass(Resource.class),
            PersistentVolumeClaimList.class,
            (resource, resourceName) -> {
                mockGet(resourceType, pvcDb, resourceName, resource);
                mockCreate(resourceType, pvcDb, null, resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceType, pvcDb, null, resourceName, resource);
                mockDelete(resourceType, pvcDb, null, resourceName, resource);
            });
    }

    // Endpoints
    private MixedOperation<Endpoints, EndpointsList, DoneableEndpoints, Resource<Endpoints, DoneableEndpoints>> mockEndpoints() {
        String resourceType = "endpoint";
        return crudMock(resourceType, this.endpointDb,
            castClass(Resource.class),
                EndpointsList.class,
            (resource, resourceName) -> {
                mockGet(resourceType, endpointDb, resourceName, resource);
                mockCreate(resourceType, endpointDb, null, resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceType, endpointDb, null, resourceName, resource);
                mockDelete(resourceType, endpointDb, null, resourceName, resource);
                mockIsReady(resourceType, resourceName, resource);
            });
    }


    // Services
    private MixedOperation<Service, ServiceList, DoneableService, Resource<Service, DoneableService>> mockSvc() {
        String resourceType = "service";
        return crudMock(resourceType, this.svcDb,
            castClass(Resource.class),
            ServiceList.class,
            (resource, resourceName) -> {
                mockGet(resourceType, svcDb, resourceName, resource);
                //mockCreate("endpoint", endpointDb, resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceType, svcDb, null, resourceName, resource);
                mockDelete(resourceType, svcDb, null, resourceName, resource);
                when(resource.create(any())).thenAnswer(i -> {
                    Service argument = i.getArgument(0);
                    svcDb.put(resourceName, argument);
                    LOGGER.debug("create {} (and endpoint) {} ", resourceType, resourceName);
                    endpointDb.put(resourceName, new Endpoints());
                    return argument;
                });
            }
        );
    }

    // Pods
    private MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> mockPods() {
        String resourceType = "pod";
        return crudMock(resourceType, this.podDb,
            castClass(PodResource.class),
            PodList.class,
            (resource, resourceName) -> {
                mockGet(resourceType, podDb, resourceName, resource);
                mockWatch(resourceType, podWatchers, resource);
                mockCreate(resourceType, podDb, podWatchers, resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceType, podDb, podWatchers, resourceName, resource);
                mockDelete(resourceType, podDb, podWatchers, resourceName, resource);
                mockIsReady(resourceType, resourceName, resource);
            });
    }

    // Deployments
    private MixedOperation<Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>> mockDeployment() {
        String resourceType = "deployment";
        return crudMock(resourceType, depDb,
            castClass(ScalableResource.class),
            DeploymentList.class,
            (resource, resourceName) -> {
                mockGet(resourceType, depDb, resourceName, resource);
                mockCreate(resourceType, depDb, null, resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceType, depDb, null, resourceName, resource);
                mockDelete(resourceType, depDb, null, resourceName, resource);
            }
        );
    }

    // StatefulSets
    private MixedOperation<StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>> mockSs() {
        String resourceType = "statefulset";
        return this.crudMock(resourceType, this.ssDb,
            castClass(RollableScalableResource.class),
            StatefulSetList.class,
            (resource, resourceName) -> {
                mockGet(resourceType, ssDb, resourceName, resource);
                //mockCreate("endpoint", endpointDb, resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceType, ssDb, null, resourceName, resource);
                mockDelete(resourceType, ssDb, null, resourceName, resource);
                mockIsReady(resourceType, resourceName, resource);
                when(resource.create(any())).thenAnswer(cinvocation -> {
                    checkNotExists(ssDb, resourceType, resourceName);
                    StatefulSet argument = cinvocation.getArgument(0);
                    LOGGER.debug("create {} {} -> {}", resourceType, resourceName, argument);
                    ssDb.put(resourceName, argument);
                    for (int i = 0; i < argument.getSpec().getReplicas(); i++) {
                        String podName = argument.getMetadata().getName() + "-" + i;
                        podDb.put(podName,
                                new PodBuilder().withNewMetadata()
                                        .withNamespace(argument.getMetadata().getNamespace())
                                        .withName(podName)
                                .endMetadata().build());
                    }
                    return argument;
                });
                EditReplacePatchDeletable<StatefulSet, StatefulSet, DoneableStatefulSet, Boolean> c = mock(EditReplacePatchDeletable.class);
                when(resource.cascading(false)).thenReturn(c);
                when(c.patch(any())).thenAnswer(i -> {
                    StatefulSet argument = i.getArgument(0);
                    ssDb.put(resourceName, argument);
                    return argument;
                });
                when(resource.isReady()).thenAnswer(i -> {
                    LOGGER.debug("{} {} is ready", resourceType, resourceName);
                    return true;
                });
                when(resource.scale(anyInt())).thenAnswer(i -> {
                    checkDoesExist(ssDb, resourceType, resourceName);
                    int scale = i.getArgument(0);
                    LOGGER.debug("scale {} {} to {}", resourceType, resourceName, scale);
                    return ssDb.get(resourceName);
                });
                when(resource.scale(anyInt(), anyBoolean())).thenAnswer(i -> {
                    checkDoesExist(ssDb, resourceType, resourceName);
                    int scale = i.getArgument(0);
                    LOGGER.debug("scale {} {} to {}, waiting {}", resourceType, resourceName, scale, i.getArgument(1));
                    return ssDb.get(resourceName);
                });
            });
    }

    private void checkNotExists(Map<String, ?> map, String resourceType, String resourceName) {
        if (map.containsKey(resourceName)) {
            throw new KubernetesClientException(resourceType + " " + resourceName + " already exists");
        }

    }

    private void checkDoesExist(Map<String, ?> map, String resourceType, String resourceName) {
        if (!map.containsKey(resourceName)) {
            throw new KubernetesClientException(resourceType + " " + resourceName + " does not exist");
        }
    }

}
