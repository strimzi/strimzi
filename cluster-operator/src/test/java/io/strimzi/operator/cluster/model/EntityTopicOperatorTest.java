/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpecBuilder;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.SystemProperty;
import io.strimzi.api.kafka.model.SystemPropertyBuilder;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.strimzi.test.TestUtils.map;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class EntityTopicOperatorTest {

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final InlineLogging topicOperatorLogging = new InlineLogging();
    {
        topicOperatorLogging.setLoggers(Collections.singletonMap("topic-operator.root.logger", "OFF"));
    }
    private final Probe livenessProbe = new Probe();
    {
        livenessProbe.setInitialDelaySeconds(15);
        livenessProbe.setTimeoutSeconds(20);
        livenessProbe.setFailureThreshold(12);
        livenessProbe.setSuccessThreshold(5);
        livenessProbe.setPeriodSeconds(180);
    }

    private final Probe readinessProbe = new Probe();
    {
        readinessProbe.setInitialDelaySeconds(15);
        readinessProbe.setInitialDelaySeconds(20);
        readinessProbe.setFailureThreshold(12);
        readinessProbe.setSuccessThreshold(5);
        readinessProbe.setPeriodSeconds(180);
    }

    private final String toWatchedNamespace = "my-topic-namespace";
    private final String toImage = "my-topic-operator-image";
    private final int toReconciliationInterval = 120;
    private final int toZookeeperSessionTimeout = 18;
    private final int toTopicMetadataMaxAttempts = 3;

    private final List<SystemProperty> javaSystemProperties = new ArrayList<SystemProperty>() {{
            add(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build());
            add(new SystemPropertyBuilder().withName("something.else").withValue("42").build());
        }};



    private final EntityTopicOperatorSpec entityTopicOperatorSpec = new EntityTopicOperatorSpecBuilder()
            .withWatchedNamespace(toWatchedNamespace)
            .withImage(toImage)
            .withReconciliationIntervalSeconds(toReconciliationInterval)
            .withZookeeperSessionTimeoutSeconds(toZookeeperSessionTimeout)
            .withTopicMetadataMaxAttempts(toTopicMetadataMaxAttempts)
            .withLivenessProbe(livenessProbe)
            .withReadinessProbe(readinessProbe)
            .withLogging(topicOperatorLogging)
            .withNewJvmOptions()
                    .withNewXms("128m")
                    .addAllToJavaSystemProperties(javaSystemProperties)
            .endJvmOptions()
            .build();

    private final EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
            .withTopicOperator(entityTopicOperatorSpec)
            .build();

    private final Kafka resource =
            new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                    .editSpec()
                    .withEntityOperator(entityOperatorSpec)
                    .endSpec()
                    .build();

    private final EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource);

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_RESOURCE_LABELS).withValue(ModelUtils.defaultResourceLabels(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_KAFKA_BOOTSTRAP_SERVERS).withValue(EntityTopicOperator.defaultBootstrapServers(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_ZOOKEEPER_CONNECT).withValue(String.format("%s:%d", "localhost", EntityTopicOperatorSpec.DEFAULT_ZOOKEEPER_PORT)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_WATCHED_NAMESPACE).withValue(toWatchedNamespace).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS).withValue(String.valueOf(toReconciliationInterval * 1000)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS).withValue(String.valueOf(toZookeeperSessionTimeout * 1000)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_TOPIC_METADATA_MAX_ATTEMPTS).withValue(String.valueOf(toTopicMetadataMaxAttempts)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_SECURITY_PROTOCOL).withValue(EntityTopicOperatorSpec.DEFAULT_SECURITY_PROTOCOL).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_TLS_ENABLED).withValue(Boolean.toString(true)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_STRIMZI_GC_LOG_ENABLED).withValue(Boolean.toString(AbstractModel.DEFAULT_JVM_GC_LOGGING_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_STRIMZI_JAVA_OPTS).withValue("-Xms128m").build());
        expected.add(new EnvVarBuilder().withName(EntityTopicOperator.ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES).withValue("-Djavax.net.debug=verbose -Dsomething.else=42").build());
        return expected;
    }

    @ParallelTest
    public void testEnvVars()   {
        assertThat(entityTopicOperator.getEnvVars(), is(getExpectedEnvVars()));
    }

    @ParallelTest
    public void testFromCrd() {
        assertThat(entityTopicOperator.namespace, is(namespace));
        assertThat(entityTopicOperator.cluster, is(cluster));
        assertThat(entityTopicOperator.image, is(toImage));
        assertThat(entityTopicOperator.readinessProbeOptions.getInitialDelaySeconds(), is(readinessProbe.getInitialDelaySeconds()));
        assertThat(entityTopicOperator.readinessProbeOptions.getTimeoutSeconds(), is(readinessProbe.getTimeoutSeconds()));
        assertThat(entityTopicOperator.readinessProbeOptions.getSuccessThreshold(), is(readinessProbe.getSuccessThreshold()));
        assertThat(entityTopicOperator.readinessProbeOptions.getFailureThreshold(), is(readinessProbe.getFailureThreshold()));
        assertThat(entityTopicOperator.readinessProbeOptions.getPeriodSeconds(), is(readinessProbe.getPeriodSeconds()));
        assertThat(entityTopicOperator.livenessProbeOptions.getInitialDelaySeconds(), is(livenessProbe.getInitialDelaySeconds()));
        assertThat(entityTopicOperator.livenessProbeOptions.getTimeoutSeconds(), is(livenessProbe.getTimeoutSeconds()));
        assertThat(entityTopicOperator.livenessProbeOptions.getSuccessThreshold(), is(livenessProbe.getSuccessThreshold()));
        assertThat(entityTopicOperator.livenessProbeOptions.getFailureThreshold(), is(livenessProbe.getFailureThreshold()));
        assertThat(entityTopicOperator.livenessProbeOptions.getPeriodSeconds(), is(livenessProbe.getPeriodSeconds()));
        assertThat(entityTopicOperator.getWatchedNamespace(), is(toWatchedNamespace));
        assertThat(entityTopicOperator.getReconciliationIntervalMs(), is(toReconciliationInterval * 1000));
        assertThat(entityTopicOperator.getZookeeperSessionTimeoutMs(), is(toZookeeperSessionTimeout * 1000));
        assertThat(entityTopicOperator.getZookeeperConnect(), is(EntityTopicOperator.defaultZookeeperConnect(cluster)));
        assertThat(entityTopicOperator.getKafkaBootstrapServers(), is(EntityTopicOperator.defaultBootstrapServers(cluster)));
        assertThat(entityTopicOperator.getResourceLabels(), is(ModelUtils.defaultResourceLabels(cluster)));
        assertThat(entityTopicOperator.getTopicMetadataMaxAttempts(), is(toTopicMetadataMaxAttempts));
        assertThat(entityTopicOperator.getLogging().getType(), is(topicOperatorLogging.getType()));
        assertThat(((InlineLogging) entityTopicOperator.getLogging()).getLoggers(), is(topicOperatorLogging.getLoggers()));
    }

    @ParallelTest
    public void testFromCrdDefault() {
        EntityTopicOperatorSpec entityTopicOperatorSpec = new EntityTopicOperatorSpecBuilder()
                .build();
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
                .withTopicOperator(entityTopicOperatorSpec)
                .build();
        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();
        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource);

        assertThat(entityTopicOperator.getWatchedNamespace(), is(namespace));
        assertThat(entityTopicOperator.getImage(), is("quay.io/strimzi/operator:latest"));
        assertThat(entityTopicOperator.getReconciliationIntervalMs(), is(EntityTopicOperatorSpec.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1000));
        assertThat(entityTopicOperator.getZookeeperSessionTimeoutMs(), is(EntityTopicOperatorSpec.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS * 1000));
        assertThat(entityTopicOperator.getTopicMetadataMaxAttempts(), is(EntityTopicOperatorSpec.DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS));
        assertThat(entityTopicOperator.getZookeeperConnect(), is(EntityTopicOperator.defaultZookeeperConnect(cluster)));
        assertThat(entityTopicOperator.getKafkaBootstrapServers(), is(EntityTopicOperator.defaultBootstrapServers(cluster)));
        assertThat(entityTopicOperator.getResourceLabels(), is(ModelUtils.defaultResourceLabels(cluster)));
        assertThat(entityTopicOperator.readinessProbeOptions.getInitialDelaySeconds(), is(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY));
        assertThat(entityTopicOperator.readinessProbeOptions.getTimeoutSeconds(), is(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT));
        assertThat(entityTopicOperator.livenessProbeOptions.getInitialDelaySeconds(), is(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY));
        assertThat(entityTopicOperator.livenessProbeOptions.getTimeoutSeconds(), is(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT));
        assertThat(entityTopicOperator.getLogging(), is(nullValue()));
    }

    @ParallelTest
    public void testFromCrdNoEntityOperator() {
        Kafka resource = ResourceUtils.createKafka(namespace, cluster, replicas, image,
                healthDelay, healthTimeout);
        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource);
        assertThat(entityTopicOperator, is(nullValue()));
    }

    @ParallelTest
    public void testFromCrdNoTopicOperatorInEntityOperator() {
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder().build();
        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();
        EntityTopicOperator entityTopicOperator = EntityTopicOperator.fromCrd(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource);
        assertThat(entityTopicOperator, is(nullValue()));
    }

    @ParallelTest
    public void testGetContainers() {
        List<Container> containers = entityTopicOperator.getContainers(null);
        assertThat(containers.size(), is(1));

        Container container = containers.get(0);
        assertThat(container.getName(), is(EntityTopicOperator.TOPIC_OPERATOR_CONTAINER_NAME));
        assertThat(container.getImage(), is(entityTopicOperator.getImage()));
        assertThat(container.getEnv(), is(getExpectedEnvVars()));
        assertThat(container.getLivenessProbe().getInitialDelaySeconds(), is(Integer.valueOf(livenessProbe.getInitialDelaySeconds())));
        assertThat(container.getLivenessProbe().getTimeoutSeconds(), is(Integer.valueOf(livenessProbe.getTimeoutSeconds())));
        assertThat(container.getReadinessProbe().getInitialDelaySeconds(), is(Integer.valueOf(readinessProbe.getInitialDelaySeconds())));
        assertThat(container.getReadinessProbe().getTimeoutSeconds(), is(Integer.valueOf(readinessProbe.getTimeoutSeconds())));
        assertThat(container.getPorts().size(), is(1));
        assertThat(container.getPorts().get(0).getContainerPort(), is(Integer.valueOf(EntityTopicOperator.HEALTHCHECK_PORT)));
        assertThat(container.getPorts().get(0).getName(), is(EntityTopicOperator.HEALTHCHECK_PORT_NAME));
        assertThat(container.getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(EntityOperatorTest.volumeMounts(container.getVolumeMounts()), is(map(
                EntityTopicOperator.TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME, AbstractModel.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH,
                "entity-topic-operator-metrics-and-logging", "/opt/topic-operator/custom-config/",
                EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT,
                EntityOperator.TLS_SIDECAR_EO_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT)));
    }

    @ParallelTest
    public void testRoleBindingInOtherNamespace()   {
        RoleBinding binding = entityTopicOperator.generateRoleBindingForRole(namespace, toWatchedNamespace);

        assertThat(binding.getSubjects().get(0).getNamespace(), is(namespace));
        assertThat(binding.getMetadata().getNamespace(), is(toWatchedNamespace));
        assertThat(binding.getMetadata().getOwnerReferences().size(), is(0));

        assertThat(binding.getRoleRef().getKind(), is("Role"));
        assertThat(binding.getRoleRef().getName(), is("foo-entity-operator"));
    }

    @ParallelTest
    public void testRoleBindingInTheSameNamespace()   {
        RoleBinding binding = entityTopicOperator.generateRoleBindingForRole(namespace, namespace);

        assertThat(binding.getSubjects().get(0).getNamespace(), is(namespace));
        assertThat(binding.getMetadata().getNamespace(), is(namespace));
        assertThat(binding.getMetadata().getOwnerReferences().size(), is(1));

        assertThat(binding.getRoleRef().getKind(), is("Role"));
        assertThat(binding.getRoleRef().getName(), is("foo-entity-operator"));
    }
}
