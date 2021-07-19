/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorRequirement;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.OngoingStubbing;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Partially mocks the Fabric8 API for a given resource type.
 * Essentially this mocks some/most of the calls from the fabric 8 Kubernetes client API which look like:
 * <pre><code>
 *   ...inNamespace(namespace).{doThing()}
 *   ...inNamespace(namespace).withName(name).{doThing()}
 * </code></pre>
 *
 * @param <T> The resource type (e.g. Pod)
 * @param <L> The list type (e.g. PodList)
 * @param <R> The resource type (e.g. Resource, or ScalableResource)
 */
class MockBuilder<T extends HasMetadata,
        L extends KubernetesResource/*<T>*/ & KubernetesResourceList/*<T>*/,
        R extends Resource<T>> {

    /**
     * This method is just used to appease javac and avoid having a very ugly "double cast" (cast to raw Class,
     * followed by a cast to parameterised Class) in all the calls to
     * {@link MockBuilder#MockBuilder(Class, Class, Class, Map)}
     */
    @SuppressWarnings("unchecked")
    protected static <T extends HasMetadata, R extends Resource<T>, R2 extends Resource> Class<R> castClass(Class<R2> c) {
        return (Class) c;
    }

    private static final Logger LOGGER = LogManager.getLogger(MockBuilder.class);

    protected final Class<T> resourceTypeClass;
    protected final Class<L> listClass;
    protected final Class<R> resourceClass;
    /** In-memory database of resource name to resource instance */
    protected final Map<String, T> db;
    protected final String resourceType;
    protected final Collection<PredicatedWatcher<T>> watchers = Collections.synchronizedList(new ArrayList<>(2));
    private List<Observer<T>> observers = null;

    public void assertNumWatchers(int expectedNumWatchers) {
        if (watchers.size() != expectedNumWatchers) {
            throw new AssertionError("Unclosed watchers " + watchers);
        }
    }

    public void assertNoWatchers() {
        assertNumWatchers(0);
    }

    public MockBuilder(Class<T> resourceTypeClass, Class<L> listClass,
                       Class<R> resourceClass, Map<String, T> db) {
        this.resourceTypeClass = resourceTypeClass;
        this.resourceType = resourceTypeClass.getSimpleName();
        this.resourceClass = resourceClass;
        this.db = db;
        this.listClass = listClass;
    }

    public MockBuilder<T, L, R> addObserver(Observer<T> observer) {
        if (observers == null) {
            observers = new ArrayList<>();
        }
        this.observers.add(observer);
        return this;
    }

    @SuppressWarnings("unchecked")
    protected T copyResource(T resource) {
        if (resource == null) {
            return null;
        } else {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                objectMapper.writeValue(baos, resource);
                return (T) objectMapper.readValue(baos.toByteArray(), resource.getClass());
            } catch (IOException e) {
                return null;
            }
        }
    }

    /**
     * Generate a stateful mock for CRUD-like interactions.
     * @return The mock
     */
    @SuppressWarnings("unchecked")
    public MixedOperation<T, L, R> build() {
        MixedOperation<T, L, R> mixed = mock(MixedOperation.class);

        when(mixed.inNamespace(any())).thenReturn(mixed);
        when(mixed.list()).thenAnswer(i -> mockList(p -> true));
        when(mixed.withLabels(any())).thenAnswer(i -> {
            MixedOperation<T, L, R> mixedWithLabels = mock(MixedOperation.class);
            Map<String, String> labels = i.getArgument(0);
            when(mixedWithLabels.list()).thenAnswer(i2 -> mockList(p -> {
                Map<String, String> m = new HashMap(p.getMetadata().getLabels());
                m.keySet().retainAll(labels.keySet());
                return labels.equals(m);
            }));
            return mixedWithLabels;
        });
        when(mixed.withName(any())).thenAnswer(invocation -> {
            String resourceName = invocation.getArgument(0);
            R resource = mock(resourceClass);
            nameScopedMocks(resourceName, resource);
            return resource;
        });
        when(mixed.watch(any())).thenAnswer(i -> {
            Watcher watcher = i.getArgument(0);
            LOGGER.debug("Watcher {} installed on {}", watcher, mixed);
            return addWatcher(PredicatedWatcher.watcher(resourceTypeClass.getName(), watcher));
        });
        when(mixed.create((T) any())).thenAnswer(i -> {
            T resource = i.getArgument(0);
            String resourceName = resource.getMetadata().getName();
            return doCreate(resourceName, resource);
        });
        when(mixed.create()).thenAnswer(i -> {
            T resource = i.getArgument(0);
            String resourceName = resource.getMetadata().getName();
            if (db.containsKey(resourceName)) {
                return mixed.withName(resourceName).patch(resource);
            } else {
                return doCreate(resourceName, resource);
            }
        });
        when(mixed.createOrReplace()).thenAnswer(i -> {
            T resource = i.getArgument(0);
            return doCreate(resource.getMetadata().getName(), resource);
        });
        when(mixed.createOrReplace((T) any())).thenAnswer(i -> {
            T resource = i.getArgument(0);
            return doCreate(resource.getMetadata().getName(), resource);
        });
        when(mixed.delete(ArgumentMatchers.<T[]>any())).thenAnswer(i -> {
            T resource = i.getArgument(0);
            String resourceName = resource.getMetadata().getName();
            return doDelete(resourceName);
        });
        when(mixed.withLabel(any())).thenAnswer(i -> {
            String label = i.getArgument(0);
            return mockWithLabel(label);
        });
        when(mixed.withLabel(any(), any())).thenAnswer(i -> {
            String label = i.getArgument(0);
            String value = i.getArgument(1);
            return mockWithLabels(singletonMap(label, value));
        });
        when(mixed.withLabelSelector(any())).thenAnswer(i -> {
            LabelSelector labelSelector = i.getArgument(0);
            Map<String, String> matchLabels = labelSelector.getMatchLabels();
            List<LabelSelectorRequirement> matchExpressions = labelSelector.getMatchExpressions();
            if (matchExpressions != null && !matchExpressions.isEmpty()) {
                throw new RuntimeException("MockKube doesn't support match expressions yet");
            }
            return mockWithLabelPredicate(p -> {
                Map<String, String> m = new HashMap<>(p.getMetadata().getLabels());
                m.keySet().retainAll(matchLabels.keySet());
                return matchLabels.equals(m);
            });
        });
        when(mixed.withLabels(any())).thenAnswer(i -> {
            Map<String, String> labels = i.getArgument(0);
            return mockWithLabels(labels);
        });
        return mixed;
    }

    public MixedOperation<T, L, R> build2(Supplier<MixedOperation<T, L, R>> x) {
        MixedOperation<T, L, R> build = build();
        when(x.get()).thenReturn(build);
        return build;
    }

    public MixedOperation<T, L, R> buildNns(Supplier<NonNamespaceOperation<T, L, R>> x) {
        MixedOperation<T, L, R> build = build();
        when(x.get()).thenReturn(build);
        return build;
    }

    MixedOperation<T, L, R> mockWithLabels(Map<String, String> labels) {
        return mockWithLabelPredicate(p -> {
            Map<String, String> m = new HashMap<>(p.getMetadata().getLabels() == null ? emptyMap() : p.getMetadata().getLabels());
            m.keySet().retainAll(labels.keySet());
            return labels.equals(m);
        });
    }

    MixedOperation<T, L, R> mockWithLabel(String label) {
        return mockWithLabelPredicate(p -> p.getMetadata().getLabels().containsKey(label));
    }

    @SuppressWarnings("unchecked")
    MixedOperation<T, L, R> mockWithLabelPredicate(Predicate<T> predicate) {
        MixedOperation<T, L, R> mixedWithLabels = mock(MixedOperation.class);
        when(mixedWithLabels.list()).thenAnswer(i2 -> {
            return mockList(predicate);
        });
        when(mixedWithLabels.watch(any())).thenAnswer(i2 -> {
            Watcher watcher = i2.getArgument(0);
            return addWatcher(PredicatedWatcher.predicatedWatcher(resourceTypeClass.getName(), "watch on labeled", predicate, watcher));
        });
        return mixedWithLabels;
    }

    @SuppressWarnings("unchecked")
    private KubernetesResourceList<T> mockList(Predicate<? super T> predicate) {
        KubernetesResourceList<T> l = mock(listClass);
        Collection<T> values;
        synchronized (db) {
            values = db.values().stream().filter(predicate).map(resource -> copyResource(resource)).collect(Collectors.toList());
        }
        when(l.getItems()).thenAnswer(i3 -> {
            LOGGER.debug("{} list -> {}", resourceTypeClass.getSimpleName(), values);
            return values;
        });
        return l;
    }

    /**
     * Mock operations on the given {@code resource} which are scoped to accessing the given {@code resourceName}.
     * For example the methods accessible from
     * {@code client.configMaps().inNamespace(ns).withName(resourceName)...}
     *
     * @param resourceName The resource name
     * @param resource The (mocked) resource
     */
    @SuppressWarnings("unchecked")
    protected void nameScopedMocks(String resourceName, R resource) {
        mockGet(resourceName, resource);
        mockWatch(resourceName, resource);
        mockCreate(resourceName, resource);
        mockSetStatus(resourceName, resource);
        when(resource.createOrReplace(any())).thenAnswer(i -> {
            T resource2 = i.getArgument(0);
            if (db.containsKey(resourceName)) {
                return resource.patch(resource2);
            } else {
                return doCreate(resourceName, resource2);
            }
        });

        when(resource.withGracePeriod(anyLong())).thenReturn(resource);
        mockWithPropagationPolicy(resource);
        mockPatch(resourceName, resource);
        when(resource.edit()).thenAnswer(i -> {
            T t = resource.get();
            Function f = i.getArgument(0);
            return doPatch(t.getMetadata().getName(), resource, (T) f.apply(t));
        });
        when(resource.edit(any(UnaryOperator.class))).thenAnswer(i -> {
            T t = resource.get();
            Function f = i.getArgument(0);
            return doPatch(t.getMetadata().getName(), resource, (T) f.apply(t));
        });
        mockDelete(resourceName, resource);
        mockIsReady(resourceName, resource);

        try {
            when(resource.waitUntilCondition(any(), anyLong(), any())).thenAnswer(i -> {
                Predicate<T> p = i.getArgument(0);
                T t = resource.get();
                boolean done = p.test(t);
                long argument = i.getArgument(1);
                TimeUnit tu = i.getArgument(2);
                long deadline = System.currentTimeMillis() + tu.toMillis(argument);
                while (!done) {
                    Thread.sleep(1_000);
                    if (System.currentTimeMillis() > deadline) {
                        throw new TimeoutException();
                    }
                    t = resource.get();
                    done = p.test(t);
                }
                return t;
            });
        } catch (KubernetesClientTimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    protected void checkNotExists(String resourceName) {
        if (db.containsKey(resourceName)) {
            throw new KubernetesClientException(resourceType + " " + resourceName + " already exists");
        }
    }

    protected void checkDoesExist(String resourceName) {
        if (!db.containsKey(resourceName)) {
            throw new KubernetesClientException(resourceType + " " + resourceName + " does not exist");
        }
    }

    protected void mockDelete(String resourceName, R resource) {
        when(resource.withPropagationPolicy(DeletionPropagation.FOREGROUND).delete()).thenAnswer(i -> {
            return doDelete(resourceName);
        });
    }

    private Object doDelete(String resourceName) {
        LOGGER.debug("delete {} {}", resourceType, resourceName);
        T removed = db.remove(resourceName);
        if (removed != null) {
            fireWatchers(resourceName, removed, Watcher.Action.DELETED, "delete");
        }
        return removed != null;
    }

    protected void fireWatchers(String resourceName, T resource, Watcher.Action action, String cause) {
        if (observers != null) {
            for (Observer<T> observer : observers) {
                LOGGER.debug("Firing observer.beforeWatcherFire() {} on {} for {} due to {}", observer, resourceName, action, cause);
                observer.beforeWatcherFire(action, resource);
            }
        }
        LOGGER.debug("Firing watchers on {}", resourceName);
        for (PredicatedWatcher<T> watcher : watchers) {
            LOGGER.debug("Firing watcher {} on {} for {} due to {}", watcher, resourceName, action, cause);
            watcher.maybeFire(resource, action);
        }
        LOGGER.debug("Finished firing watchers on {} for {} due to {}", resourceName, action, cause);
        if (observers != null) {
            for (int i = observers.size() - 1; i >= 0; i--) {
                Observer<T> observer = observers.get(i);
                LOGGER.debug("Firing observer.afterWatcherFire() {} on {} for {} due to {}", observer, resourceName, action, cause);
                observer.afterWatcherFire(action, resource);
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected void mockPatch(String resourceName, R resource) {
        when(resource.patch((T) any())).thenAnswer(invocation -> {
            return doPatch(resourceName, resource, invocation.getArgument(0));
        });
    }

    private T doPatch(String resourceName, R resource, T instance) {
        checkDoesExist(resourceName);
        T argument = copyResource(instance);
        LOGGER.debug("patch {} {} -> {}", resourceType, resourceName, resource);
        db.put(resourceName, incrementGeneration(incrementResourceVersion(argument)));
        fireWatchers(resourceName, argument, Watcher.Action.MODIFIED, "patch");
        return copyResource(argument);
    }

    protected void mockWithPropagationPolicy(R resource) {
        when(resource.withPropagationPolicy(any(DeletionPropagation.class))).thenReturn(resource);
    }

    protected void mockWatch(String resourceName, R resource) {
        when(resource.watch(any())).thenAnswer(i -> {
            return mockedWatcher(resourceName, i);
        });
    }

    private Watch mockedWatcher(String resourceName, InvocationOnMock i) {
        Watcher<T> watcher = i.getArgument(0);
        LOGGER.debug("watch {} {} ", resourceType, watcher);
        return addWatcher(PredicatedWatcher.namedWatcher(resourceTypeClass.getName(), resourceName, watcher));
    }

    private Watch addWatcher(PredicatedWatcher<T> predicatedWatcher) {
        watchers.add(predicatedWatcher);
        return () -> {
            watchers.remove(predicatedWatcher);
            LOGGER.debug("Watcher {} removed", predicatedWatcher);
        };
    }

    @SuppressWarnings("unchecked")
    protected void mockCreate(String resourceName, R resource) {
        when(resource.create((T) any())).thenAnswer(i -> {
            T argument = i.getArgument(0);
            return doCreate(resourceName, argument);
        });
    }

    private T doCreate(String resourceName, T argument) {
        checkNotExists(resourceName);
        LOGGER.debug("create {} {} -> {}", resourceType, resourceName, argument);
        db.put(resourceName, incrementGeneration(incrementResourceVersion(copyResource(argument))));
        fireWatchers(resourceName, argument, Watcher.Action.ADDED, "create");
        return copyResource(argument);
    }

    protected T incrementResourceVersion(T resource) {
        String resourceVersion = resource.getMetadata().getResourceVersion();
        if (resourceVersion == null || resourceVersion.isEmpty()) {
            resourceVersion = "0";
        }
        resource.getMetadata().setResourceVersion(Long.toString(Long.parseLong(resourceVersion) +  1));
        return resource;
    }

    protected T incrementGeneration(T resource) {
        Long generation = resource.getMetadata().getGeneration();
        if (generation == null) {
            resource.getMetadata().setGeneration(0L);
        } else {
            resource.getMetadata().setGeneration(generation + 1);
        }
        return resource;
    }

    protected OngoingStubbing<T> mockGet(String resourceName, R resource) {
        return when(resource.get()).thenAnswer(i -> {
            T r = copyResource(db.get(resourceName));
            LOGGER.debug("{} {} get {}", resourceType, resourceName, r);
            return r;
        });
    }

    protected OngoingStubbing<Boolean> mockIsReady(String resourceName, R resource) {
        return when(resource.isReady()).thenAnswer(i -> {
            LOGGER.debug("{} {} is ready", resourceType, resourceName);
            return Boolean.TRUE;
        });
    }

    @SuppressWarnings("unchecked")
    protected OngoingStubbing<T> mockSetStatus(String resourceName, R resource) {
        return when(resource.replaceStatus((T) any())).thenAnswer(i -> {
            T r = i.getArgument(0);
            updateStatus(r.getMetadata().getNamespace(), r.getMetadata().getName(), r);
            LOGGER.debug("{} {} setStatus {}", resourceType, resourceName, r);
            return copyResource(db.get(resourceName));
        });
    }

    public void updateStatus(String resourceNamespace, String resourceName, T resourceWithStatus) {
        throw new UnsupportedOperationException();
    }
}
