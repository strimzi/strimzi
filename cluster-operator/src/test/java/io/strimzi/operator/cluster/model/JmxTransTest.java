/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxTransSpec;
import io.strimzi.api.kafka.model.JmxTransSpecBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaJmxAuthenticationPasswordBuilder;
import io.strimzi.api.kafka.model.KafkaJmxOptionsBuilder;
import io.strimzi.api.kafka.model.template.ContainerTemplate;
import io.strimzi.api.kafka.model.template.JmxTransOutputDefinitionTemplateBuilder;
import io.strimzi.api.kafka.model.template.JmxTransQueryTemplateBuilder;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.components.JmxTransOutputWriter;
import io.strimzi.operator.cluster.model.components.JmxTransQueries;
import io.strimzi.operator.cluster.model.components.JmxTransServer;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.operator.cluster.CustomMatchers.hasEntries;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;

@ParallelSuite
public class JmxTransTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "image";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final Map<String, Object> metricsCm = singletonMap("animal", "wombat");
    private final String metricsCmJson = "{\"animal\":\"wombat\"}";
    private final String metricsCMName = "metrics-cm";
    private final JmxPrometheusExporterMetrics jmxMetricsConfig = io.strimzi.operator.cluster.TestUtils.getJmxPrometheusExporterMetrics(AbstractModel.ANCILLARY_CM_KEY_METRICS, metricsCMName);
    private final Map<String, Object> configuration = singletonMap("foo", "bar");
    private final InlineLogging kafkaLog = new InlineLogging();
    private final InlineLogging zooLog = new InlineLogging();

    private final JmxTransSpec jmxTransSpec = new JmxTransSpecBuilder()
            .withOutputDefinitions(new JmxTransOutputDefinitionTemplateBuilder()
                    .withName("Name")
                    .withOutputType("output")
                    .build())
            .withKafkaQueries(new JmxTransQueryTemplateBuilder()
                    .withOutputs("name")
                    .withAttributes("attributes")
                    .withNewTargetMBean("mbean")
                    .build())
            .build();

    private final Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout, jmxMetricsConfig, configuration, kafkaLog, zooLog))
            .editSpec()
                .withJmxTrans(jmxTransSpec)
                .editKafka().withJmxOptions(new KafkaJmxOptionsBuilder()
                    .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder().build())
                .build())
            .endKafka()
            .endSpec()
            .build();

    private final JmxTrans jmxTrans = JmxTrans.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, VERSIONS);

    @ParallelTest
    public void testOutputDefinitionWriterDeserialization() {
        JmxTransOutputWriter outputWriter = new JmxTransOutputWriter();

        outputWriter.setAtClass("class");
        outputWriter.setHost("host");
        outputWriter.setPort(9999);
        outputWriter.setFlushDelayInSeconds(1);
        outputWriter.setTypeNames(Collections.singletonList("SingleType"));

        JsonObject targetJson = JsonObject.mapFrom(outputWriter);

        assertThat(targetJson.getString("host"), is("host"));
        assertThat(targetJson.getString("@class"), is("class"));
        assertThat(targetJson.getInteger("port"), is(9999));
        assertThat(targetJson.getInteger("flushDelayInSeconds"), is(1));
        assertThat(targetJson.getJsonArray("typeNames").size(), is(1));
        assertThat(targetJson.getJsonArray("typeNames").getList().get(0), is("SingleType"));

    }

    @ParallelTest
    public void testServersDeserialization() {
        JmxTransServer server = new JmxTransServer();

        server.setHost("host");
        server.setPort(9999);
        server.setUsername("username");
        server.setPassword("password");
        server.setQueries(Collections.emptyList());

        JsonObject targetJson = JsonObject.mapFrom(server);

        assertThat(targetJson.getString("host"), is("host"));
        assertThat(targetJson.getInteger("port"), is(9999));
        assertThat(targetJson.getString("username"), is("username"));
        assertThat(targetJson.getString("password"), is("password"));
        assertThat(targetJson.getJsonArray("queries").getList().size(), is(0));
    }

    @ParallelTest
    public void testQueriesDeserialization() {
        JmxTransOutputWriter outputWriter = new JmxTransOutputWriter();

        outputWriter.setAtClass("class");
        outputWriter.setHost("host");
        outputWriter.setPort(9999);
        outputWriter.setFlushDelayInSeconds(1);
        outputWriter.setTypeNames(Collections.singletonList("SingleType"));

        JmxTransQueries queries = new JmxTransQueries();

        queries.setObj("object");
        queries.setAttr(Collections.singletonList("attribute"));

        queries.setOutputWriters(Collections.singletonList(outputWriter));

        JsonObject targetJson = JsonObject.mapFrom(queries);
        JsonObject outputWriterJson = targetJson.getJsonArray("outputWriters").getJsonObject(0);

        assertThat(targetJson.getString("obj"), is("object"));
        assertThat(targetJson.getJsonArray("attr").size(), is(1));
        assertThat(targetJson.getJsonArray("attr").getString(0), is("attribute"));

        assertThat(outputWriterJson.getString("host"), is("host"));
        assertThat(outputWriterJson.getString("@class"), is("class"));
        assertThat(outputWriterJson.getInteger("port"), is(9999));
        assertThat(outputWriterJson.getInteger("flushDelayInSeconds"), is(1));
        assertThat(outputWriterJson.getJsonArray("typeNames").size(), is(1));
        assertThat(outputWriterJson.getJsonArray("typeNames").getList().get(0), is("SingleType"));
    }

    @ParallelTest
    public void testConfigMapOnScaleUp() throws JsonProcessingException  {
        ConfigMap originalCM = jmxTrans.generateJmxTransConfigMap(jmxTransSpec, 1);
        ConfigMap scaledCM = jmxTrans.generateJmxTransConfigMap(jmxTransSpec, 2);

        assertThat(originalCM.getData().get(JmxTrans.JMXTRANS_CONFIGMAP_KEY).length() <
                        scaledCM.getData().get(JmxTrans.JMXTRANS_CONFIGMAP_KEY).length(),
                is(true));
    }

    @ParallelTest
    public void testConfigMapOnScaleDown() throws JsonProcessingException  {
        ConfigMap originalCM = jmxTrans.generateJmxTransConfigMap(jmxTransSpec, 2);
        ConfigMap scaledCM = jmxTrans.generateJmxTransConfigMap(jmxTransSpec, 1);

        assertThat(originalCM.getData().get(JmxTrans.JMXTRANS_CONFIGMAP_KEY).length() >
                        scaledCM.getData().get(JmxTrans.JMXTRANS_CONFIGMAP_KEY).length(),
                is(true));
    }

    @ParallelTest
    public void testTemplate() {
        Map<String, String> depLabels = TestUtils.map("l1", "v1", "l2", "v2",
                Labels.KUBERNETES_PART_OF_LABEL, "custom-part",
                Labels.KUBERNETES_MANAGED_BY_LABEL, "custom-managed-by");
        Map<String, String> expectedDepLabels = new HashMap<>(depLabels);
        expectedDepLabels.remove(Labels.KUBERNETES_MANAGED_BY_LABEL);

        Map<String, String> depAnots = TestUtils.map("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = TestUtils.map("l3", "v3", "l4", "v4");
        Map<String, String> podAnots = TestUtils.map("a3", "v3", "a4", "v4");

        Map<String, String> saLabels = TestUtils.map("l5", "v5", "l6", "v6");
        Map<String, String> saAnots = TestUtils.map("a5", "v5", "a6", "v6");

        Affinity affinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("key1")
                                    .withOperator("In")
                                    .withValues("value1", "value2")
                                .endMatchExpression()
                                .build())
                        .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .build();

        List<Toleration> tolerations = singletonList(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key1")
                .withOperator("Equal")
                .withValue("value1")
                .build());

        Kafka resource = new KafkaBuilder(kafkaAssembly)
                .editSpec()
                    .editOrNewJmxTrans()
                        .editOrNewTemplate()
                            .withNewDeployment()
                                .withNewMetadata()
                                    .withLabels(depLabels)
                                    .withAnnotations(depAnots)
                                .endMetadata()
                            .endDeployment()
                            .withNewPod()
                                .withNewMetadata()
                                    .withLabels(podLabels)
                                    .withAnnotations(podAnots)
                                .endMetadata()
                                .withPriorityClassName("top-priority")
                                .withSchedulerName("my-scheduler")
                                .withAffinity(affinity)
                                .withTolerations(tolerations)
                                .withEnableServiceLinks(false)
                            .endPod()
                            .withNewServiceAccount()
                                .withNewMetadata()
                                    .withLabels(saLabels)
                                    .withAnnotations(saAnots)
                                .endMetadata()
                            .endServiceAccount()
                        .endTemplate()
                    .endJmxTrans()
                .endSpec()
                .build();
        JmxTrans jmxTrans = JmxTrans.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);

        // Check Deployment
        Deployment dep = jmxTrans.generateDeployment(null, null);
        assertThat(dep.getMetadata().getLabels(), hasEntries(expectedDepLabels));
        assertThat(dep.getMetadata().getAnnotations(), hasEntries(depAnots));
        assertThat(dep.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));
        assertThat(dep.getSpec().getTemplate().getSpec().getAffinity(), is(affinity));
        assertThat(dep.getSpec().getTemplate().getSpec().getTolerations(), is(tolerations));

        // Check Pods
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels(), hasEntries(podLabels));
        assertThat(dep.getSpec().getTemplate().getMetadata().getAnnotations(), hasEntries(podAnots));
        assertThat(dep.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(dep.getSpec().getTemplate().getSpec().getEnableServiceLinks(), is(false));

        // Check Service Account
        ServiceAccount sa = jmxTrans.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnots.entrySet()), is(true));
    }

    @ParallelTest
    public void testContainerEnvVars() {

        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = "TEST_ENV_2";
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate container = new ContainerTemplate();
        container.setEnv(testEnvs);

        Kafka resource = new KafkaBuilder(kafkaAssembly)
                .editSpec()
                    .editJmxTrans()
                        .withNewTemplate()
                            .withNewContainer()
                                .withEnv(testEnvs)
                            .endContainer()
                        .endTemplate()
                    .endJmxTrans()
                .endSpec()
                .build();

        List<EnvVar> envVars = JmxTrans.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS).getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                envVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                envVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));
    }

    @ParallelTest
    public void testContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = JmxTrans.ENV_VAR_JMXTRANS_LOGGING_LEVEL;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        ContainerTemplate container = new ContainerTemplate();
        container.setEnv(testEnvs);

        Kafka resource = new KafkaBuilder(kafkaAssembly)
                .editSpec()
                    .editJmxTrans()
                        .withNewTemplate()
                            .withNewContainer()
                                .withEnv(testEnvs)
                            .endContainer()
                        .endTemplate()
                    .endJmxTrans()
                .endSpec()
                .build();

        List<EnvVar> envVars = JmxTrans.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS).getEnvVars();

        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                envVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));
    }

    @ParallelTest
    public void testContainerSecurityContext() {

        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addNewDrop("ALL")
                .endCapabilities()
                .build();

        Kafka resource = new KafkaBuilder(kafkaAssembly)
                .editSpec()
                    .editJmxTrans()
                        .withNewTemplate()
                            .withNewContainer()
                                .withSecurityContext(securityContext)
                            .endContainer()
                        .endTemplate()
                    .endJmxTrans()
                .endSpec()
                .build();

        JmxTrans jmxTrans = JmxTrans.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource, VERSIONS);
        assertThat(jmxTrans.templateContainerSecurityContext, is(securityContext));

        Deployment deployment = jmxTrans.generateDeployment(null, null);

        assertThat(deployment.getSpec().getTemplate().getSpec().getContainers(),
                hasItem(allOf(
                        hasProperty("name", equalTo(cluster + "-kafka-jmx-trans")),
                        hasProperty("securityContext", equalTo(securityContext))
                )));
    }
}
