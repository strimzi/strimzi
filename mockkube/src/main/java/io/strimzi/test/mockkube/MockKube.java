/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointsList;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountList;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetList;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressList;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyList;
import io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudgetList;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingList;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingList;
import io.fabric8.kubernetes.api.model.rbac.RoleList;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.V1ApiextensionAPIGroupDSL;
import io.fabric8.kubernetes.client.V1NetworkAPIGroupDSL;
import io.fabric8.kubernetes.client.V1beta1NetworkAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.ApiextensionsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.CreateOrReplaceable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NetworkAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.PolicyAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.RbacAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.V1beta1PolicyAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.Build;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigList;
import io.fabric8.openshift.api.model.RouteList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.BuildConfigResource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockKube {

    private final Map<String, ConfigMap> cmDb = db(emptySet());
    private final Map<String, PersistentVolumeClaim> pvcDb = db(emptySet());
    private final Map<String, Service> svcDb = db(emptySet());
    private final Map<String, Endpoints> endpointDb = db(emptySet());
    private final Map<String, Pod> podDb = db(emptySet());
    private final Map<String, StatefulSet> ssDb = db(emptySet());
    private final Map<String, Deployment> depDb = db(emptySet());
    private final Map<String, Secret> secretDb = db(emptySet());
    private final Map<String, ServiceAccount> serviceAccountDb = db(emptySet());
    private final Map<String, NetworkPolicy> policyDb = db(emptySet());
    private final Map<String, Route> routeDb = db(emptySet());
    private final Map<String, BuildConfig> buildConfigDb = db(emptySet());
    private final Map<String, PodDisruptionBudget> pdbDb = db(emptySet());
    private final Map<String, RoleBinding> pdbRb = db(emptySet());
    private final Map<String, Role> roleDb = db(emptySet());
    private final Map<String, ClusterRoleBinding> pdbCrb = db(emptySet());
    private final Map<String, Ingress> ingressDb = db(emptySet());
    private final Map<String, io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress> ingressV1Beta1Db = db(emptySet());

    private Map<String, CreateOrReplaceable> crdMixedOps = new HashMap<>();
    private MockBuilder<ConfigMap, ConfigMapList, Resource<ConfigMap>> configMapMockBuilder;
    private MockBuilder<Endpoints, EndpointsList, Resource<Endpoints>> endpointMockBuilder;
    private ServiceMockBuilder serviceMockBuilder;
    private MockBuilder<Secret, SecretList, Resource<Secret>> secretMockBuilder;
    private MockBuilder<ServiceAccount, ServiceAccountList, Resource<ServiceAccount>> serviceAccountMockBuilder;
    private MockBuilder<Route, RouteList, Resource<Route>> routeMockBuilder;
    private MockBuilder<BuildConfig, BuildConfigList, BuildConfigResource<BuildConfig, Void, Build>> buildConfigMockBuilder;
    private MockBuilder<PodDisruptionBudget, PodDisruptionBudgetList, Resource<PodDisruptionBudget>> podDisruptionBudgedMockBuilder;
    private MockBuilder<Role, RoleList, Resource<Role>> roleMockBuilder;
    private MockBuilder<RoleBinding, RoleBindingList, Resource<RoleBinding>> roleBindingMockBuilder;
    private MockBuilder<ClusterRoleBinding, ClusterRoleBindingList, Resource<ClusterRoleBinding>> clusterRoleBindingMockBuilder;
    private MockBuilder<NetworkPolicy, NetworkPolicyList, Resource<NetworkPolicy>> networkPolicyMockBuilder;
    private MockBuilder<Pod, PodList, PodResource<Pod>> podMockBuilder;
    private MockBuilder<PersistentVolumeClaim, PersistentVolumeClaimList, Resource<PersistentVolumeClaim>> persistentVolumeClaimMockBuilder;
    private MockBuilder<Ingress, IngressList, Resource<Ingress>> ingressMockBuilder;
    private MockBuilder<io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress, io.fabric8.kubernetes.api.model.networking.v1beta1.IngressList, Resource<io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress>> ingressV1Beta1MockBuilder;
    private DeploymentMockBuilder deploymentMockBuilder;
    private KubernetesClient mockClient;

    public MockKube withInitialCms(Set<ConfigMap> initialCms) {
        this.cmDb.putAll(db(initialCms));
        return this;
    }

    public MockKube withInitialStatefulSets(Set<StatefulSet> initial) {
        this.ssDb.putAll(db(initial));
        return this;
    }

    public MockKube withInitialPods(Set<Pod> initial) {
        this.podDb.putAll(db(initial));
        return this;
    }

    public MockKube withInitialSecrets(Set<Secret> initial) {
        this.secretDb.putAll(db(initial));
        return this;
    }

    public MockKube withInitialNetworkPolicy(Set<NetworkPolicy> initial) {
        this.policyDb.putAll(db(initial));
        return this;
    }

    public MockKube withInitialRoute(Set<Route> initial) {
        this.routeDb.putAll(db(initial));
        return this;
    }

    public MockKube withInitialBuildConfig(Set<BuildConfig> initial) {
        this.buildConfigDb.putAll(db(initial));
        return this;
    }

    private final List<MockedCrd> mockedCrds = new ArrayList<>();

    public class MockedCrd<T extends CustomResource, L extends KubernetesResourceList<T>,
            S> {
        private final CustomResourceDefinition crd;
        private final Class<T> crClass;
        private final Class<L> crListClass;
        private final Map<String, T> instances;
        private final Function<T, S> getStatus;
        private final BiConsumer<T, S> setStatus;

        private MockedCrd(CustomResourceDefinition crd,
                          Class<T> crClass, Class<L> crListClass,
                          Function<T, S> getStatus,
                          BiConsumer<T, S> setStatus) {
            this.crd = crd;
            this.crClass = crClass;
            this.crListClass = crListClass;
            this.getStatus = getStatus;
            this.setStatus = setStatus;
            instances = db(emptySet());
        }

        Map<String, T> getInstances() {
            return instances;
        }

        CustomResourceDefinition getCrd() {
            return crd;
        }

        Class<T> getCrClass() {
            return crClass;
        }

        Class<L> getCrListClass() {
            return crListClass;
        }

        Function<T, S> getStatus() {
            return getStatus;
        }

        BiConsumer<T, S> setStatus() {
            return setStatus;
        }

        public MockedCrd<T, L, S> withInitialInstances(Set<T> instances) {
            for (T instance : instances) {
                this.instances.put(instance.getMetadata().getName(), instance);
            }
            return this;
        }
        public MockKube end() {
            return MockKube.this;
        }
    }

    public <T extends CustomResource, L extends KubernetesResourceList<T>,
            S> MockedCrd<T, L, S>
            withCustomResourceDefinition(CustomResourceDefinition crd, Class<T> instanceClass, Class<L> instanceListClass,
                                         Function<T, S> getStatus,
                                         BiConsumer<T, S> setStatus) {
        MockedCrd<T, L, S> mockedCrd = new MockedCrd<>(crd, instanceClass, instanceListClass, getStatus, setStatus);
        this.mockedCrds.add(mockedCrd);
        return mockedCrd;
    }

    @SuppressWarnings("unchecked")
    public <T extends CustomResource, L extends KubernetesResourceList<T>, S> MockedCrd<T, L, S>
        withCustomResourceDefinition(CustomResourceDefinition crd, Class<T> instanceClass, Class<L> instanceListClass) {
        return withCustomResourceDefinition(crd, instanceClass, instanceListClass, t -> (S) t.getStatus(), T::setStatus);
    }

    private final Map<Class<? extends HasMetadata>, MockBuilder<?, ?, ?>> mockBuilders = new HashMap<>();
    private final Map<String, MockBuilder<?, ?, ?>> mockBuilders2 = new HashMap<>();
    private final Map<String, Class<? extends HasMetadata>> mockBuilders3 = new HashMap<>();

    <T extends MockBuilder<?, ?, ?>> T addMockBuilder(String plural, T mockBuilder) {
        mockBuilders.put(mockBuilder.resourceTypeClass, mockBuilder);
        mockBuilders2.put(plural, mockBuilder);
        mockBuilders3.put(plural, mockBuilder.resourceTypeClass);
        return mockBuilder;
    }

    @SuppressWarnings("unchecked")
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST_OF_RETURN_VALUE")
    public KubernetesClient build() {
        if (mockClient != null) {
            return mockClient;
        }
        configMapMockBuilder = addMockBuilder("configmaps", new MockBuilder<>(ConfigMap.class, ConfigMapList.class, MockBuilder.castClass(Resource.class), cmDb));
        endpointMockBuilder = addMockBuilder("endpoints", new MockBuilder<>(Endpoints.class, EndpointsList.class, MockBuilder.castClass(Resource.class), endpointDb));
        serviceMockBuilder = addMockBuilder("services", new ServiceMockBuilder(svcDb, endpointDb));
        secretMockBuilder = addMockBuilder("secrets", new MockBuilder<>(Secret.class, SecretList.class, MockBuilder.castClass(Resource.class), secretDb));
        serviceAccountMockBuilder = addMockBuilder("serviceaccounts", new MockBuilder<>(ServiceAccount.class, ServiceAccountList.class, MockBuilder.castClass(Resource.class), serviceAccountDb));
        routeMockBuilder = addMockBuilder("routes", new MockBuilder<>(Route.class, RouteList.class, MockBuilder.castClass(Resource.class), routeDb));
        buildConfigMockBuilder = addMockBuilder("buildConfigs", new MockBuilder<>(BuildConfig.class, BuildConfigList.class, MockBuilder.castClass(Resource.class), buildConfigDb));
        podDisruptionBudgedMockBuilder = addMockBuilder("poddisruptionbudgets", new MockBuilder<>(PodDisruptionBudget.class, PodDisruptionBudgetList.class, MockBuilder.castClass(Resource.class), pdbDb));
        roleBindingMockBuilder = addMockBuilder("rolebindings", new MockBuilder<>(RoleBinding.class, RoleBindingList.class, MockBuilder.castClass(Resource.class), pdbRb));
        roleMockBuilder = addMockBuilder("roles", new MockBuilder<>(Role.class, RoleList.class, MockBuilder.castClass(Resource.class), roleDb));
        clusterRoleBindingMockBuilder = addMockBuilder("clusterrolebindings", new MockBuilder<>(ClusterRoleBinding.class, ClusterRoleBindingList.class, MockBuilder.castClass(Resource.class), pdbCrb));
        networkPolicyMockBuilder = addMockBuilder("networkpolicies", new MockBuilder<>(NetworkPolicy.class, NetworkPolicyList.class, MockBuilder.castClass(Resource.class), policyDb));
        ingressMockBuilder = addMockBuilder("ingresses",  new MockBuilder<>(Ingress.class, IngressList.class, MockBuilder.castClass(Resource.class), ingressDb));
        ingressV1Beta1MockBuilder = addMockBuilder("ingresses",  new MockBuilder<>(io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress.class, io.fabric8.kubernetes.api.model.networking.v1beta1.IngressList.class, MockBuilder.castClass(Resource.class), ingressV1Beta1Db));

        podMockBuilder = addMockBuilder("pods", new MockBuilder<>(Pod.class, PodList.class, MockBuilder.castClass(PodResource.class), podDb));
        MixedOperation<Pod, PodList, PodResource<Pod>> mockPods = podMockBuilder.build();

        persistentVolumeClaimMockBuilder = addMockBuilder("persistentvolumeclaims", new MockBuilder<>(PersistentVolumeClaim.class, PersistentVolumeClaimList.class, MockBuilder.castClass(Resource.class), pvcDb));
        MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, Resource<PersistentVolumeClaim>> mockPersistentVolumeClaims = persistentVolumeClaimMockBuilder.build();
        deploymentMockBuilder = addMockBuilder("deployments", new DeploymentMockBuilder(depDb, mockPods));
        MixedOperation<StatefulSet, StatefulSetList,
                RollableScalableResource<StatefulSet>> mockSs = buildStatefulSets(podMockBuilder, mockPods, mockPersistentVolumeClaims);

        // Top level group
        mockClient = mock(KubernetesClient.class);
        configMapMockBuilder.build2(mockClient::configMaps);
        serviceMockBuilder.build2(mockClient::services);
        secretMockBuilder.build2(mockClient::secrets);
        serviceAccountMockBuilder.build2(mockClient::serviceAccounts);
        when(mockClient.pods()).thenReturn(mockPods);
        endpointMockBuilder.build2(mockClient::endpoints);
        when(mockClient.persistentVolumeClaims()).thenReturn(mockPersistentVolumeClaims);

        // API group
        AppsAPIGroupDSL api = mock(AppsAPIGroupDSL.class);
        when(mockClient.apps()).thenReturn(api);
        when(api.statefulSets()).thenReturn(mockSs);
        deploymentMockBuilder.build2(api::deployments);

        MixedOperation<CustomResourceDefinition, CustomResourceDefinitionList, Resource<CustomResourceDefinition>> mockCrds = mock(MixedOperation.class);

        // Custom Resources
        if (mockedCrds != null && !mockedCrds.isEmpty()) {
            NonNamespaceOperation<CustomResourceDefinition, CustomResourceDefinitionList,
                    Resource<CustomResourceDefinition>> crds = mock(MixedOperation.class);
            for (MockedCrd<?, ?, ?> mockedCrd : this.mockedCrds) {
                CustomResourceDefinition crd = mockedCrd.crd;
                Resource crdResource = mock(Resource.class);
                when(crdResource.get()).thenReturn(crd);
                when(crds.withName(crd.getMetadata().getName())).thenReturn(crdResource);
                String key = crdKey(mockedCrd.crClass);
                CreateOrReplaceable crdMixedOp = crdMixedOps.get(key);
                if (crdMixedOp == null) {
                    CustomResourceMockBuilder customResourceMockBuilder = addMockBuilder(crd.getSpec().getNames().getPlural(), new CustomResourceMockBuilder<>((MockedCrd) mockedCrd));
                    crdMixedOp = (MixedOperation<CustomResource, ? extends KubernetesResource, Resource<CustomResource>>) customResourceMockBuilder.build();
                    crdMixedOps.put(key, crdMixedOp);
                }
                when(mockCrds.withName(eq(crd.getMetadata().getName()))).thenReturn(crdResource);
            }

            ApiextensionsAPIGroupDSL mockApiEx = mock(ApiextensionsAPIGroupDSL.class);
            V1ApiextensionAPIGroupDSL mockv1 = mock(V1ApiextensionAPIGroupDSL.class);

            when(mockClient.apiextensions()).thenReturn(mockApiEx);
            when(mockApiEx.v1()).thenReturn(mockv1);
            when(mockv1.customResourceDefinitions()).thenReturn(mockCrds);

            mockCrs(mockClient);
        }

        // Network group
        NetworkAPIGroupDSL network = mock(NetworkAPIGroupDSL.class);
        V1NetworkAPIGroupDSL networkV1 = mock(V1NetworkAPIGroupDSL.class);
        V1beta1NetworkAPIGroupDSL networkV1beta1 = mock(V1beta1NetworkAPIGroupDSL.class);
        when(mockClient.network()).thenReturn(network);
        when(network.v1()).thenReturn(networkV1);
        when(network.v1beta1()).thenReturn(networkV1beta1);
        networkPolicyMockBuilder.build2(network::networkPolicies);
        ingressMockBuilder.build2(networkV1::ingresses);
        ingressV1Beta1MockBuilder.build2(networkV1beta1::ingresses);

        // Policy group
        PolicyAPIGroupDSL policy = mock(PolicyAPIGroupDSL.class);
        V1beta1PolicyAPIGroupDSL v1beta1policy = mock(V1beta1PolicyAPIGroupDSL.class);
        when(mockClient.policy()).thenReturn(policy);
        when(policy.v1beta1()).thenReturn(v1beta1policy);
        podDisruptionBudgedMockBuilder.build2(mockClient.policy().v1beta1()::podDisruptionBudget);

        // RBAC group
        RbacAPIGroupDSL rbac = mock(RbacAPIGroupDSL.class);
        when(mockClient.rbac()).thenReturn(rbac);
        roleBindingMockBuilder.build2(mockClient.rbac()::roleBindings);
        roleMockBuilder.build2(mockClient.rbac()::roles);
        clusterRoleBindingMockBuilder.buildNns(mockClient.rbac()::clusterRoleBindings);

        // Openshift group
        OpenShiftClient mockOpenShiftClient = mock(OpenShiftClient.class);
        when(mockClient.adapt(OpenShiftClient.class)).thenReturn(mockOpenShiftClient);
        routeMockBuilder.build2(mockOpenShiftClient::routes);
        buildConfigMockBuilder.build2(mockOpenShiftClient::buildConfigs);
        if (mockedCrds != null && !mockedCrds.isEmpty()) {
            ApiextensionsAPIGroupDSL mockApiEx = mock(ApiextensionsAPIGroupDSL.class);
            V1ApiextensionAPIGroupDSL mockv1 = mock(V1ApiextensionAPIGroupDSL.class);

            when(mockOpenShiftClient.apiextensions()).thenReturn(mockApiEx);
            when(mockApiEx.v1()).thenReturn(mockv1);
            when(mockv1.customResourceDefinitions()).thenReturn(mockCrds);
            mockCrs(mockOpenShiftClient);
        }

        doAnswer(i -> {
            for (MockBuilder<?, ?, ?> a : mockBuilders.values()) {
                a.assertNoWatchers();
            }
            return null;
        }).when(mockClient).close();
        return mockClient;
    }

    public <T extends CustomResource> String crdKey(Class<T> crClass) {
        return crClass.getName();
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    public void mockCrs(KubernetesClient mockClient) {
        when(mockClient.customResources(any(CustomResourceDefinitionContext.class),
                any(Class.class),
                any(Class.class))).thenAnswer(invocation -> {
                    Class<CustomResource> crClass = invocation.getArgument(1);
                    String key = crdKey(crClass);
                    CreateOrReplaceable createOrReplaceable = crdMixedOps.get(key);
                    if (createOrReplaceable == null) {
                        throw new RuntimeException("Unknown CRD " + key);
                    }
                    return createOrReplaceable;
                });

        when(mockClient.customResources(any(Class.class), any(Class.class)))
                .thenAnswer(invocation -> {
                    Class<CustomResource> crClass = invocation.getArgument(0);
                    String key = crdKey(crClass);
                    CreateOrReplaceable createOrReplaceable = crdMixedOps.get(key);
                    if (createOrReplaceable == null) {
                        throw new RuntimeException("Unknown CRD " + key);
                    }
                    return createOrReplaceable;
                });
    }

    private MixedOperation<StatefulSet, StatefulSetList, RollableScalableResource<StatefulSet>>
        buildStatefulSets(MockBuilder<Pod, PodList, PodResource<Pod>> podMockBuilder, MixedOperation<Pod, PodList, PodResource<Pod>> mockPods,
                          MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList,
                                  Resource<PersistentVolumeClaim>> mockPvcs) {
        MixedOperation<StatefulSet, StatefulSetList, RollableScalableResource<StatefulSet>> result = new StatefulSetMockBuilder(podMockBuilder, ssDb, podDb, mockPods, mockPvcs).build();
        return result;
    }


    private static <T extends HasMetadata> Map<String, T> db(Collection<T> initialResources) {
        return new ConcurrentHashMap<>(initialResources.stream().collect(Collectors.toMap(
            c -> c.getMetadata().getName(),
            c -> copyResource(c))));
    }


    @SuppressWarnings("unchecked")
    protected static <T extends HasMetadata> T copyResource(T resource) {
        if (resource == null) {
            return null;
        } else {
            ObjectMapper objectMapper = new ObjectMapper();
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                objectMapper.writeValue(baos, resource);
                return (T) objectMapper.readValue(baos.toByteArray(), resource.getClass());
            } catch (IOException e) {
                return null;
            }
        }
    }

}
