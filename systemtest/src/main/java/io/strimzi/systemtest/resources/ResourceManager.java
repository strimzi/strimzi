/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.Spec;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.Status;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.enums.DeploymentTypes;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.resources.kubernetes.ClusterRoleBindingResource;
import io.strimzi.systemtest.resources.kubernetes.DeploymentResource;
import io.strimzi.systemtest.resources.kubernetes.JobResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.resources.kubernetes.RoleBindingResource;
import io.strimzi.systemtest.resources.kubernetes.ServiceResource;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.HelmClient;
import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cmdClient.KubeCmdClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling",
    "checkstyle:ClassFanOutComplexity",
    "checkstyle:ClassDataAbstractionCoupling"})
@SuppressFBWarnings({"MS_MUTABLE_COLLECTION_PKGPROTECT", "MS_MUTABLE_COLLECTION"})
public class ResourceManager {

    private static final Logger LOGGER = LogManager.getLogger(ResourceManager.class);

    public static final long CR_CREATION_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness();

    public static final Map<String, Stack<ResourceItem>> STORED_RESOURCES = new LinkedHashMap<>();

    private static String coDeploymentName = Constants.STRIMZI_DEPLOYMENT_NAME;
    private static ResourceManager instance;

    public static synchronized ResourceManager getInstance() {
        if (instance == null) {
            instance = new ResourceManager();
        }
        return instance;
    }

    public static KubeClient kubeClient() {
        return KubeClusterResource.kubeClient();
    }

    public static KubeCmdClient cmdKubeClient() {
        return KubeClusterResource.cmdKubeClient();
    }

    public static HelmClient helmClient() {
        return KubeClusterResource.helmClusterClient();
    }

    private final ResourceType<?>[] resourceTypes = new ResourceType[]{
        new KafkaBridgeResource(),
        new KafkaClientsResource(),
        new KafkaConnectorResource(),
        new KafkaConnectResource(),
        new KafkaMirrorMaker2Resource(),
        new KafkaMirrorMakerResource(),
        new KafkaRebalanceResource(),
        new KafkaResource(),
        new KafkaTopicResource(),
        new KafkaUserResource(),
        new BundleResource(),
        new ClusterRoleBindingResource(),
        new DeploymentResource(),
        new JobResource(),
        new NetworkPolicyResource(),
        new RoleBindingResource(),
        new ServiceResource()
    };

    @SafeVarargs
    public final <T extends HasMetadata> void createResource(ExtensionContext testContext, T... resources) {
        createResource(testContext, true, resources);
    }

    @SafeVarargs
    public final <T extends HasMetadata> void createResource(ExtensionContext testContext, boolean waitReady, T... resources) {
        for (T resource : resources) {
            ResourceType<T> type = findResourceType(resource);
            LOGGER.info("Create/Update of {} {} in namespace {}",
                resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace() == null ? "(not set)" : resource.getMetadata().getNamespace());

            // if it is parallel namespace test we are gonna replace resource a namespace
            if (StUtils.isParallelNamespaceTest(testContext)) {
                if (!Environment.isNamespaceRbacScope()) {
                    final String namespace = testContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.NAMESPACE_KEY).toString();
                    LOGGER.info("Using namespace: {}", namespace);
                    resource.getMetadata().setNamespace(namespace);
                }
            }

            type.create(resource);

            synchronized (this) {
                STORED_RESOURCES.computeIfAbsent(testContext.getDisplayName(), k -> new Stack<>());
                STORED_RESOURCES.get(testContext.getDisplayName()).push(
                    new ResourceItem<T>(
                        () -> deleteResource(resource),
                        resource
                    ));
            }
        }

        if (waitReady) {
            for (T resource : resources) {
                ResourceType<T> type = findResourceType(resource);
                assertTrue(waitResourceCondition(resource, ResourceCondition.readiness(type)),
                    String.format("Timed out waiting for %s %s in namespace %s to be ready", resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace()));
            }
        }
    }

    @SafeVarargs
    public final <T extends HasMetadata> void deleteResource(T... resources) {
        for (T resource : resources) {
            ResourceType<T> type = findResourceType(resource);
            if (type == null) {
                LOGGER.warn("Can't find resource type, please delete it manually");
                continue;
            }

            LOGGER.info("Delete of {} {} in namespace {}",
                resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace() == null ? "(not set)" : resource.getMetadata().getNamespace());

            type.delete(resource);
            assertTrue(waitResourceCondition(resource, ResourceCondition.deletion()),
                String.format("Timed out deleting %s %s in namespace %s", resource.getKind(), resource.getMetadata().getName(), resource.getMetadata().getNamespace()));
        }
    }
    public final <T extends HasMetadata> boolean waitResourceCondition(T resource, ResourceCondition<T> condition) {
        assertNotNull(resource);
        assertNotNull(resource.getMetadata());
        assertNotNull(resource.getMetadata().getName());

        // cluster role binding does not need namespace...
        if (!(resource instanceof ClusterRoleBinding)) {
            assertNotNull(resource.getMetadata().getNamespace());
        }

        ResourceType<T> type = findResourceType(resource);
        assertNotNull(type);
        boolean[] resourceReady = new boolean[1];

        TestUtils.waitFor("Resource condition: " + condition.getConditionName() + " is fulfilled for resource " + resource.getKind() + ":" + resource.getMetadata().getName(),
            Constants.GLOBAL_POLL_INTERVAL_MEDIUM, ResourceOperation.getTimeoutForResourceReadiness(resource.getKind()),
            () -> {
                T res = type.get(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
                resourceReady[0] =  condition.getPredicate().test(res);
                if (!resourceReady[0]) {
                    type.delete(res);
                }
                return resourceReady[0];
            });

        return resourceReady[0];
    }

    /**
     * Synchronizing all resources which are inside specific extension context.
     * @param testContext context of the test case
     * @param <T> type of the resource which inherits from HasMetadata f.e Kafka, KafkaConnect, Pod, Deployment etc..
     */
    @SuppressWarnings(value = "unchecked")
    public final <T extends HasMetadata> void synchronizeResources(ExtensionContext testContext) {
        Stack<ResourceItem> resources = STORED_RESOURCES.get(testContext.getDisplayName());

        // sync all resources
        for (ResourceItem resource : resources) {
            if (resource.getResource() == null) {
                continue;
            }
            ResourceType<T> type = findResourceType((T) resource.getResource());

            waitResourceCondition((T) resource.getResource(), ResourceCondition.readiness(type));
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    public static <T extends CustomResource, L extends CustomResourceList<T>> void replaceCrdResource(Class<T> crdClass, Class<L> listClass, String resourceName, Consumer<T> editor, String namespace) {
        Resource<T> namedResource = Crds.operation(kubeClient().getClient(), crdClass, listClass).inNamespace(namespace).withName(resourceName);
        T resource = namedResource.get();
        editor.accept(resource);
        namedResource.replace(resource);
    }

    public static <T extends CustomResource, L extends CustomResourceList<T>> void replaceCrdResource(Class<T> crdClass, Class<L> listClass, String resourceName, Consumer<T> editor) {
        Resource<T> namedResource = Crds.operation(kubeClient().getClient(), crdClass, listClass).inNamespace(kubeClient().getNamespace()).withName(resourceName);
        T resource = namedResource.get();
        editor.accept(resource);
        namedResource.replace(resource);
    }

    public void deleteResources(ExtensionContext testContext) throws Exception {
        LOGGER.info(String.join("", Collections.nCopies(76, "#")));
        LOGGER.info("Going to clear all resources for {}", testContext.getDisplayName());
        LOGGER.info(String.join("", Collections.nCopies(76, "#")));
        if (!STORED_RESOURCES.containsKey(testContext.getDisplayName()) || STORED_RESOURCES.get(testContext.getDisplayName()).isEmpty()) {
            LOGGER.info("In context {} is everything deleted.", testContext.getDisplayName());
        }
        while (STORED_RESOURCES.containsKey(testContext.getDisplayName()) && !STORED_RESOURCES.get(testContext.getDisplayName()).isEmpty()) {
            STORED_RESOURCES.get(testContext.getDisplayName()).pop().getThrowableRunner().run();
        }
        LOGGER.info(String.join("", Collections.nCopies(76, "#")));
        STORED_RESOURCES.remove(testContext.getDisplayName());
    }

    /**
     * Log actual status of custom resource with pods.
     * @param customResource - Kafka, KafkaConnect etc. - every resource that HasMetadata and HasStatus (Strimzi status)
     */
    public static <T extends CustomResource<? extends Spec, ? extends Status>> void logCurrentResourceStatus(T customResource) {
        if (customResource != null) {
            List<String> printWholeCR = Arrays.asList(KafkaConnector.RESOURCE_KIND, KafkaTopic.RESOURCE_KIND, KafkaUser.RESOURCE_KIND);

            String kind = customResource.getKind();
            String name = customResource.getMetadata().getName();

            if (printWholeCR.contains(kind)) {
                LOGGER.info(customResource);
            } else {
                List<String> log = new ArrayList<>(asList("\n", kind, " status:\n", "\nConditions:\n"));

                if (customResource.getStatus() != null) {
                    List<Condition> conditions = customResource.getStatus().getConditions();
                    if (conditions != null) {
                        for (Condition condition : customResource.getStatus().getConditions()) {
                            if (condition.getMessage() != null) {
                                log.add("\tType: " + condition.getType() + "\n");
                                log.add("\tMessage: " + condition.getMessage() + "\n");
                            }
                        }
                    }

                    log.add("\nPods with conditions and messages:\n\n");

                    for (Pod pod : kubeClient().namespace(customResource.getMetadata().getNamespace()).listPodsByPrefixInName(name)) {
                        log.add(pod.getMetadata().getName() + ":");
                        for (PodCondition podCondition : pod.getStatus().getConditions()) {
                            if (podCondition.getMessage() != null) {
                                log.add("\n\tType: " + podCondition.getType() + "\n");
                                log.add("\tMessage: " + podCondition.getMessage() + "\n");
                            }
                        }
                        log.add("\n\n");
                    }
                    LOGGER.info("{}", String.join("", log));
                }
            }
        }
    }

    /**
     * Wait until the CR is in desired state
     * @param operation - client of CR - for example kafkaClient()
     * @param resource - custom resource
     * @param status - desired status
     * @return returns CR
     */
    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatus(MixedOperation<T, ?, ?> operation, T resource, Enum<?> status, long resourceTimeout) {
        waitForResourceStatus(operation, resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName(), status, resourceTimeout);
        return true;
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatus(MixedOperation<T, ?, ?> operation, String kind, String namespace, String name, Enum<?> status, long resourceTimeoutMs) {
        LOGGER.info("Wait for {}: {} will have desired state: {}", kind, name, status);

        TestUtils.waitFor(String.format("Wait for %s: %s will have desired state: %s", kind, name, status),
            Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, resourceTimeoutMs,
            () -> operation.inNamespace(namespace)
                .withName(name)
                .get().getStatus().getConditions().stream().anyMatch(condition -> condition.getType().equals(status.toString()) && condition.getStatus().equals("True")),
            () -> logCurrentResourceStatus(operation.inNamespace(namespace)
                .withName(name)
                .get()));

        LOGGER.info("{}: {} is in desired state: {}", kind, name, status);
        return true;
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> boolean waitForResourceStatus(MixedOperation<T, ?, ?> operation, T resource, Enum<?> status) {
        long resourceTimeout = ResourceOperation.getTimeoutForResourceReadiness(resource.getKind());
        return waitForResourceStatus(operation, resource, status, resourceTimeout);
    }

    public static String getCoDeploymentName() {
        return coDeploymentName;
    }

    public static void setCoDeploymentName(String newName) {
        coDeploymentName = newName;
    }

    public static void waitForResourceReadiness(String resourceType, String resourceName) {
        LOGGER.info("Waiting for " + resourceType + "/" + resourceName + " readiness");

        TestUtils.waitFor("resource " + resourceType + "/" + resourceName + " readiness",
                Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_CMD_CLIENT_TIMEOUT,
            () -> ResourceManager.cmdKubeClient().getResourceReadiness(resourceType, resourceName));
        LOGGER.info("Resource " + resourceType + "/" + resourceName + " is ready");
    }

    @SuppressWarnings(value = "unchecked")
    private <T extends HasMetadata> ResourceType<T> findResourceType(T resource) {

        // for conflicting deployment types
        if (resource.getKind().equals(Constants.DEPLOYMENT)) {
            String deploymentType = resource.getMetadata().getLabels().get(Constants.DEPLOYMENT_TYPE);
            DeploymentTypes deploymentTypes = DeploymentTypes.valueOf(deploymentType);

            switch (deploymentTypes) {
                case BundleClusterOperator:
                    return (ResourceType<T>) new BundleResource();
                case KafkaClients:
                    return (ResourceType<T>) new KafkaClientsResource();
                default:
                    return (ResourceType<T>) new DeploymentResource();
            }
        }

        // other no conflicting types
        for (ResourceType<?> type : resourceTypes) {
            if (type.getKind().equals(resource.getKind())) {
                return (ResourceType<T>) type;
            }
        }
        return null;
    }
}
