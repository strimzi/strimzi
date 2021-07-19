/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.kubernetes.ClusterRoleBindingResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.resources.kubernetes.RoleBindingResource;
import io.strimzi.systemtest.resources.kubernetes.RoleResource;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.resources.specific.HelmResource;
import io.strimzi.systemtest.resources.specific.OlmResource;
import io.strimzi.systemtest.templates.kubernetes.ClusterRoleBindingTemplates;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cluster.Minishift;
import io.strimzi.test.k8s.cluster.OpenShift;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class SetupClusterOperator {

    private static final Logger LOGGER = LogManager.getLogger(SetupClusterOperator.class);
    public static final String CO_INSTALL_DIR = TestUtils.USER_PATH + "/../packaging/install/cluster-operator";

    private KubeClusterResource cluster = KubeClusterResource.getInstance();
    private Stack<String> clusterOperatorConfigs = new Stack<>();
    private HelmResource helmResource;
    private OlmResource olmResource;

    private ExtensionContext extensionContext;
    private String clusterOperatorName;
    private String namespaceInstallTo;
    private String namespaceToWatch;
    private List<String> bindingsNamespaces;
    private long operationTimeout;
    private long reconciliationInterval;
    private List<EnvVar> extraEnvVars;

    SetupClusterOperator() {}
    SetupClusterOperator(SetupClusterOperatorBuilder builder) {
        this.extensionContext = builder.extensionContext;
        this.clusterOperatorName = builder.clusterOperatorName;
        this.namespaceInstallTo = builder.namespaceInstallTo;
        this.namespaceToWatch = builder.namespaceToWatch;
        this.bindingsNamespaces = builder.bindingsNamespaces;
        this.operationTimeout = builder.operationTimeout;
        this.reconciliationInterval = builder.reconciliationInterval;
        this.extraEnvVars = builder.extraEnvVars;

        // assign defaults is something is not specified
        if (this.clusterOperatorName == null || this.clusterOperatorName.isEmpty()) this.clusterOperatorName = Constants.STRIMZI_DEPLOYMENT_NAME;
        if (this.namespaceToWatch == null) this.namespaceToWatch = this.namespaceInstallTo;
        if (this.bindingsNamespaces == null) this.bindingsNamespaces = Collections.singletonList(this.namespaceInstallTo);
        if (this.operationTimeout == 0) this.operationTimeout = Constants.CO_OPERATION_TIMEOUT_DEFAULT;
        if (this.reconciliationInterval == 0) this.reconciliationInterval = Constants.RECONCILIATION_INTERVAL;
    }

    /**
     * This method install Strimzi Cluster Operator based on environment variable configuration.
     * It can install operator by classic way (apply bundle yamls) or use OLM. For OLM you need to set all other OLM env variables.
     * Don't use this method in tests, where specific configuration of CO is needed.
     */
    public SetupClusterOperator runInstallation() {
        if (Environment.isOlmInstall()) {
            LOGGER.info("Going to install ClusterOperator via OLM");
            // cluster-wide olm co-operator
            if (namespaceToWatch.equals(Constants.WATCH_ALL_NAMESPACES)) {
                // if RBAC is enable we don't run tests in parallel mode and with that said we don't create another namespaces
                if (!Environment.isNamespaceRbacScope()) {
                    cluster.createNamespaces(extensionContext, namespaceInstallTo, bindingsNamespaces);
                    createClusterRoleBindings();
                    olmResource = new OlmResource(cluster.getDefaultOlmNamespace());
                    olmResource.create(extensionContext, operationTimeout, reconciliationInterval);
                }
            // single-namespace olm co-operator
            } else {
                cluster.setNamespace(namespaceInstallTo);
                cluster.createNamespaces(extensionContext, namespaceInstallTo, bindingsNamespaces);
                olmResource = new OlmResource(namespaceInstallTo);
                olmResource.create(extensionContext, operationTimeout, reconciliationInterval);
            }
        } else if (Environment.isHelmInstall()) {
            LOGGER.info("Going to install ClusterOperator via Helm");
            helmResource = new HelmResource(namespaceInstallTo, namespaceToWatch);
            cluster.setNamespace(namespaceInstallTo);
            cluster.createNamespaces(extensionContext, namespaceInstallTo, bindingsNamespaces);
            helmResource.create(extensionContext, operationTimeout, reconciliationInterval);
        } else {
            LOGGER.info("Going to install ClusterOperator via Yaml bundle");
            prepareEnvForOperator(extensionContext, namespaceInstallTo, bindingsNamespaces);
            if (Environment.isNamespaceRbacScope()) {
                // if roles only, only deploy the rolebindings
                for (String namespace : bindingsNamespaces) {
                    applyRoles(namespace);
                    applyRoleBindings(extensionContext, namespaceInstallTo, namespace);
                }
                applyRoleBindings(extensionContext, namespaceInstallTo, namespaceInstallTo);
            } else {
                applyBindings(extensionContext, namespaceInstallTo, bindingsNamespaces);
            }
            // cluster-wide installation
            if (namespaceToWatch.equals(Constants.WATCH_ALL_NAMESPACES)) {
                if (Environment.isNamespaceRbacScope()) {
                    // we override namespaceToWatch to where cluster operator is installed because RBAC is
                    // enabled and we have use only single namespace
                    namespaceToWatch = namespaceInstallTo;
                } else {
                    // if RBAC is enable we don't run tests in parallel mode and with that said we don't create another namespaces
                    createClusterRoleBindings();
                }
            }

            // copy image-pull secret
            if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
                StUtils.copyImagePullSecret(namespaceInstallTo);
            }
            // 060-Deployment
            ResourceManager.setCoDeploymentName(clusterOperatorName);
            ResourceManager.getInstance().createResource(extensionContext,
                new BundleResource.BundleResourceBuilder()
                    .withName(Constants.STRIMZI_DEPLOYMENT_NAME)
                    .withNamespace(namespaceInstallTo)
                    .withWatchingNamespaces(namespaceToWatch)
                    .withOperationTimeout(operationTimeout)
                    .withReconciliationInterval(reconciliationInterval)
                    .withExtraEnvVars(extraEnvVars)
                    .buildBundleInstance()
                    .buildBundleDeployment()
                    .build());
        }
        return this;
    }

    private void createClusterRoleBindings() {
        // Create ClusterRoleBindings that grant cluster-wide access to all OpenShift projects
        List<ClusterRoleBinding> clusterRoleBindingList = ClusterRoleBindingTemplates.clusterRoleBindingsForAllNamespaces(namespaceInstallTo);
        clusterRoleBindingList.forEach(clusterRoleBinding ->
            ClusterRoleBindingResource.clusterRoleBinding(extensionContext, clusterRoleBinding));
    }

    public static class SetupClusterOperatorBuilder {

        private ExtensionContext extensionContext;
        private String clusterOperatorName;
        private String namespaceInstallTo;
        private String namespaceToWatch;
        private List<String> bindingsNamespaces;
        private long operationTimeout;
        private long reconciliationInterval;
        private List<EnvVar> extraEnvVars;

        public SetupClusterOperatorBuilder withExtensionContext(ExtensionContext extensionContext) {
            this.extensionContext = extensionContext;
            return self();
        }
        public SetupClusterOperatorBuilder withClusterOperatorName(String clusterOperatorName) {
            this.clusterOperatorName = clusterOperatorName;
            return self();
        }
        public SetupClusterOperatorBuilder withNamespace(String namespaceInstallTo) {
            this.namespaceInstallTo = namespaceInstallTo;
            return self();
        }
        public SetupClusterOperatorBuilder withWatchingNamespaces(String namespaceToWatch) {
            this.namespaceToWatch = namespaceToWatch;
            return self();
        }
        public SetupClusterOperatorBuilder withBindingsNamespaces(List<String> bindingsNamespaces) {
            this.bindingsNamespaces = bindingsNamespaces;
            return self();
        }
        public SetupClusterOperatorBuilder withOperationTimeout(long operationTimeout) {
            this.operationTimeout = operationTimeout;
            return self();
        }
        public SetupClusterOperatorBuilder withReconciliationInterval(long reconciliationInterval) {
            this.reconciliationInterval = reconciliationInterval;
            return self();
        }

        // currently supported only for Bundle installation
        public SetupClusterOperatorBuilder withExtraEnvVars(List<EnvVar> envVars) {
            this.extraEnvVars = envVars;
            return self();
        }

        private SetupClusterOperatorBuilder self() {
            return this;
        }

        public SetupClusterOperator createInstallation() {
            return new SetupClusterOperator(this);
        }
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     * @param namespaces list of namespaces which will be created
     * @param resources list of path to yaml files with resources specifications
     */
    public void prepareEnvForOperator(ExtensionContext extensionContext, String clientNamespace, List<String> namespaces, String... resources) {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());
        cluster.createNamespaces(extensionContext, clientNamespace, namespaces);
        cluster.createCustomResources(resources);
        applyClusterOperatorInstallFiles(clientNamespace);
        NetworkPolicyResource.applyDefaultNetworkPolicySettings(extensionContext, namespaces);

        if (cluster.cluster() instanceof Minishift || cluster.cluster() instanceof OpenShift) {
            // This is needed in case you are using internal kubernetes registry and you want to pull images from there
            if (kubeClient().getNamespace(Environment.STRIMZI_ORG) != null) {
                for (String namespace : namespaces) {
                    LOGGER.debug("Setting group policy for Openshift registry in namespace: " + namespace);
                    Exec.exec(null, Arrays.asList("oc", "policy", "add-role-to-group", "system:image-puller", "system:serviceaccounts:" + namespace, "-n", Environment.STRIMZI_ORG), 0, false, false);
                }
            }
        }
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     * @param resources list of path to yaml files with resources specifications
     */
    public void prepareEnvForOperator(ExtensionContext extensionContext, String clientNamespace, String... resources) {
        prepareEnvForOperator(extensionContext, clientNamespace, Collections.singletonList(clientNamespace), resources);
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     */
    public void prepareEnvForOperator(ExtensionContext extensionContext, String clientNamespace) {
        prepareEnvForOperator(extensionContext, clientNamespace, Collections.singletonList(clientNamespace));
    }

    /**
     * Perform application of ServiceAccount, Roles and CRDs needed for proper cluster operator deployment.
     * Configuration files are loaded from packaging/install/cluster-operator directory.
     */
    public void applyClusterOperatorInstallFiles(String namespace) {
        clusterOperatorConfigs.clear();
        List<File> operatorFiles = Arrays.stream(new File(CO_INSTALL_DIR).listFiles()).sorted()
            .filter(File::isFile)
            .filter(file ->
                !file.getName().matches(".*(Binding|Deployment)-.*"))
            .collect(Collectors.toList());

        for (File operatorFile : operatorFiles) {
            File createFile = operatorFile;
            if (operatorFile.getName().contains("ClusterRole-")) {
                createFile = switchClusterRolesToRolesIfNeeded(createFile);
            }

            LOGGER.info("Creating configuration file: {}", createFile.getAbsolutePath());
            cmdKubeClient().namespace(namespace).createOrReplace(createFile);
            clusterOperatorConfigs.push(createFile.getPath());
        }
    }

    /**
     * Replace all references to ClusterRole to Role.
     * This includes ClusterRoles themselves as well as RoleBindings that reference them.
     */
    public static File switchClusterRolesToRolesIfNeeded(File oldFile) {
        if (Environment.isNamespaceRbacScope()) {
            try {
                File tmpFile = File.createTempFile("rbac-" + oldFile.getName().replace(".yaml", ""), ".yaml");
                TestUtils.writeFile(tmpFile.getAbsolutePath(), TestUtils.readFile(oldFile).replace("ClusterRole", "Role"));
                LOGGER.info("Replaced ClusterRole for Role in {}", oldFile.getAbsolutePath());

                return tmpFile;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return oldFile;
        }
    }

    public static void applyRoleBindings(ExtensionContext extensionContext, String namespace, String bindingsNamespace) {
        // 020-RoleBinding
        RoleBindingResource.roleBinding(extensionContext, Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml", namespace, bindingsNamespace);
        // 031-RoleBinding
        RoleBindingResource.roleBinding(extensionContext, Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml", namespace, bindingsNamespace);
    }

    public static void applyRoles(String namespace) {
        File roleFile = new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/020-ClusterRole-strimzi-cluster-operator-role.yaml");
        RoleResource.role(switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace);

        roleFile = new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/021-ClusterRole-strimzi-cluster-operator-role.yaml");
        RoleResource.role(switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace);

        roleFile = new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/030-ClusterRole-strimzi-kafka-broker.yaml");
        RoleResource.role(switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace);

        roleFile = new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/031-ClusterRole-strimzi-entity-operator.yaml");
        RoleResource.role(switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace);

        roleFile = new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/033-ClusterRole-strimzi-kafka-client.yaml");
        RoleResource.role(switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace);
    }

    /**
     * Method to apply Strimzi cluster operator specific RoleBindings and ClusterRoleBindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     * @param bindingsNamespaces list of namespaces where Bindings should be deployed to
     */
    public static void applyBindings(ExtensionContext extensionContext, String namespace, List<String> bindingsNamespaces) {
        for (String bindingsNamespace : bindingsNamespaces) {
            applyClusterRoleBindings(extensionContext, namespace);
            applyRoleBindings(extensionContext, namespace, bindingsNamespace);
        }
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     */
    public static void applyBindings(ExtensionContext extensionContext, String namespace) {
        applyBindings(extensionContext, namespace, Collections.singletonList(namespace));
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     * @param bindingsNamespaces array of namespaces where Bindings should be deployed to
     */
    public static void applyBindings(ExtensionContext extensionContext, String namespace, String... bindingsNamespaces) {
        applyBindings(extensionContext, namespace, Arrays.asList(bindingsNamespaces));
    }

    private static void applyClusterRoleBindings(ExtensionContext extensionContext, String namespace) {
        // 021-ClusterRoleBinding
        ClusterRoleBindingResource.clusterRoleBinding(extensionContext, Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/021-ClusterRoleBinding-strimzi-cluster-operator.yaml", namespace);
        // 030-ClusterRoleBinding
        ClusterRoleBindingResource.clusterRoleBinding(extensionContext, Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/030-ClusterRoleBinding-strimzi-cluster-operator-kafka-broker-delegation.yaml", namespace);
        // 033-ClusterRoleBinding
        ClusterRoleBindingResource.clusterRoleBinding(extensionContext, Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/033-ClusterRoleBinding-strimzi-cluster-operator-kafka-client-delegation.yaml", namespace);
    }

    /**
     * Delete ServiceAccount, Roles and CRDs from kubernetes cluster.
     */
    public void deleteClusterOperatorInstallFiles() {
        while (!clusterOperatorConfigs.empty()) {
            String clusterOperatorConfig = clusterOperatorConfigs.pop();
            LOGGER.info("Deleting configuration file: {}", clusterOperatorConfig);
            cmdKubeClient().delete(clusterOperatorConfig);
        }
    }
}
