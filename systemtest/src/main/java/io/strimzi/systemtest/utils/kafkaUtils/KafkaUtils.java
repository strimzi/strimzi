/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.ConfigModels;
import io.strimzi.kafka.config.model.Scope;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.KafkaClusterSpec.FORBIDDEN_PREFIXES;
import static io.strimzi.api.kafka.model.KafkaClusterSpec.FORBIDDEN_PREFIX_EXCEPTIONS;
import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.api.kafka.model.KafkaResources.zookeeperStatefulSetName;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.TestUtils.indent;
import static io.strimzi.test.TestUtils.waitFor;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaUtils.class);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private KafkaUtils() {}

    public static boolean waitForKafkaReady(String namespaceName, String clusterName) {
        return waitForKafkaStatus(namespaceName, clusterName, Ready);
    }

    public static boolean waitForKafkaReady(String clusterName) {
        return waitForKafkaStatus(kubeClient().getNamespace(), clusterName, Ready);
    }

    public static boolean waitForKafkaNotReady(String namespaceName, String clusterName) {
        return waitForKafkaStatus(namespaceName, clusterName, NotReady);
    }

    public static boolean waitForKafkaNotReady(String clusterName) {
        return waitForKafkaStatus(kubeClient().getNamespace(), clusterName, NotReady);
    }

    public static boolean waitForKafkaStatus(String namespaceName, String clusterName, Enum<?>  state) {
        Kafka kafka = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get();
        return ResourceManager.waitForResourceStatus(KafkaResource.kafkaClient(), kafka, state);
    }

    /**
     * Waits for the Kafka Status to be updated after changed. It checks the generation and observed generation to
     * ensure the status is up to date.
     *
     * @param namespaceName Namespace name
     * @param clusterName   Name of the Kafka cluster which should be checked
     */
    public static void waitForKafkaStatusUpdate(String namespaceName, String clusterName) {
        LOGGER.info("Waiting for Kafka status to be updated");
        TestUtils.waitFor("KafkaStatus update", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT, () -> {
            Kafka k = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get();
            return k.getMetadata().getGeneration() == k.getStatus().getObservedGeneration();
        });
    }

    public static void waitUntilKafkaStatusConditionContainsMessage(String clusterName, String namespace, String message, long timeout) {
        TestUtils.waitFor("Kafka Status with message [" + message + "]",
            Constants.GLOBAL_POLL_INTERVAL, timeout, () -> {
                List<Condition> conditions = KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get().getStatus().getConditions();
                for (Condition condition : conditions) {
                    String conditionMessage = condition.getMessage();
                    if (conditionMessage.matches(message)) {
                        return true;
                    }
                }
                return false;
            });
    }

    public static void waitUntilKafkaStatusConditionContainsMessage(String clusterName, String namespace, String message) {
        waitUntilKafkaStatusConditionContainsMessage(clusterName, namespace, message, Constants.GLOBAL_STATUS_TIMEOUT);
    }

    public static void waitForZkMntr(String namespaceName, String clusterName, Pattern pattern, int... podIndexes) {
        long timeoutMs = 120_000L;
        long pollMs = 1_000L;

        for (int podIndex : podIndexes) {
            String zookeeperPod = KafkaResources.zookeeperPodName(clusterName, podIndex);
            String zookeeperPort = String.valueOf(12181);
            waitFor("mntr", pollMs, timeoutMs, () -> {
                    try {
                        String output = cmdKubeClient(namespaceName).execInPod(zookeeperPod,
                            "/bin/bash", "-c", "echo mntr | nc localhost " + zookeeperPort).out();

                        if (pattern.matcher(output).find()) {
                            return true;
                        }
                    } catch (KubeClusterException e) {
                        LOGGER.trace("Exception while waiting for ZK to become leader/follower, ignoring", e);
                    }
                    return false;
                },
                () -> LOGGER.info("zookeeper `mntr` output at the point of timeout does not match {}:{}{}",
                    pattern.pattern(),
                    System.lineSeparator(),
                    indent(cmdKubeClient(namespaceName).execInPod(zookeeperPod, "/bin/bash", "-c", "echo mntr | nc localhost " + zookeeperPort).out()))
            );
        }
    }

    public static String getKafkaStatusCertificates(String listenerType, String namespace, String clusterName) {
        String certs = "";
        List<ListenerStatus> kafkaListeners = KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get().getStatus().getListeners();

        for (ListenerStatus listener : kafkaListeners) {
            if (listener.getType().equals(listenerType))
                certs = listener.getCertificates().toString();
        }
        certs = certs.substring(1, certs.length() - 1);
        return certs;
    }

    public static String getKafkaSecretCertificates(String namespaceName, String secretName, String certType) {
        String secretCerts = "";
        secretCerts = kubeClient(namespaceName).getSecret(namespaceName, secretName).getData().get(certType);
        byte[] decodedBytes = Base64.getDecoder().decode(secretCerts);
        secretCerts = new String(decodedBytes, Charset.defaultCharset());

        return secretCerts;
    }

    public static String getKafkaSecretCertificates(String secretName, String certType) {
        return getKafkaSecretCertificates(kubeClient().getNamespace(), secretName, certType);
    }

    @SuppressWarnings("unchecked")
    public static void waitForClusterStability(String namespaceName, String clusterName) {
        LOGGER.info("Waiting for cluster stability");
        Map<String, String>[] zkPods = new Map[1];
        Map<String, String>[] kafkaPods = new Map[1];
        Map<String, String>[] eoPods = new Map[1];
        int[] count = {0};
        zkPods[0] = StatefulSetUtils.ssSnapshot(namespaceName, zookeeperStatefulSetName(clusterName));
        kafkaPods[0] = StatefulSetUtils.ssSnapshot(namespaceName, kafkaStatefulSetName(clusterName));
        eoPods[0] = DeploymentUtils.depSnapshot(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName));
        TestUtils.waitFor("Cluster stable and ready", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_CLUSTER_STABLE, () -> {
            Map<String, String> zkSnapshot = StatefulSetUtils.ssSnapshot(namespaceName, zookeeperStatefulSetName(clusterName));
            Map<String, String> kafkaSnaptop = StatefulSetUtils.ssSnapshot(namespaceName, kafkaStatefulSetName(clusterName));
            Map<String, String> eoSnapshot = DeploymentUtils.depSnapshot(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName));
            boolean zkSameAsLast = zkSnapshot.equals(zkPods[0]);
            boolean kafkaSameAsLast = kafkaSnaptop.equals(kafkaPods[0]);
            boolean eoSameAsLast = eoSnapshot.equals(eoPods[0]);
            if (!zkSameAsLast) {
                LOGGER.warn("ZK Cluster not stable");
            }
            if (!kafkaSameAsLast) {
                LOGGER.warn("Kafka Cluster not stable");
            }
            if (!eoSameAsLast) {
                LOGGER.warn("EO not stable");
            }
            if (zkSameAsLast && kafkaSameAsLast && eoSameAsLast) {
                int c = count[0]++;
                LOGGER.debug("All stable after {} polls", c);
                if (c > 60) {
                    LOGGER.info("Kafka cluster is stable after {} polls.", c);
                    return true;
                }
                return false;
            }
            zkPods[0] = zkSnapshot;
            kafkaPods[0] = kafkaSnaptop;
            count[0] = 0;
            return false;
        });
    }

    public static void waitForClusterStability(String clusterName) {
        waitForClusterStability(kubeClient().getNamespace(), clusterName);
    }

    /**
     * Method which, update/replace Kafka configuration
     * @param clusterName name of the cluster where Kafka resource can be found
     * @param brokerConfigName key of specific property
     * @param value value of specific property
     */
    public static void updateSpecificConfiguration(String clusterName, String brokerConfigName, Object value) {
        KafkaResource.replaceKafkaResource(clusterName, kafka -> {
            LOGGER.info("Kafka config before updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
            Map<String, Object> config = kafka.getSpec().getKafka().getConfig();
            config.put(brokerConfigName, value);
            kafka.getSpec().getKafka().setConfig(config);
            LOGGER.info("Kafka config after updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
        });
    }

    /**
     * Method which, extends the @link updateConfiguration(String clusterName, KafkaConfiguration kafkaConfiguration, Object value) method
     * with stability and ensures after update of Kafka resource there will be not rolling update
     * @param clusterName name of the cluster where Kafka resource can be found
     * @param brokerConfigName key of specific property
     * @param value value of specific property
     */
    public static void  updateConfigurationWithStabilityWait(String clusterName, String brokerConfigName, Object value) {
        updateSpecificConfiguration(clusterName, brokerConfigName, value);
        waitForClusterStability(clusterName);
    }

    /**
     * Verifies that updated configuration was successfully changed inside Kafka CR
     * @param brokerConfigName key of specific property
     * @param value value of specific property
     */
    public static boolean verifyCrDynamicConfiguration(String clusterName, String brokerConfigName, Object value) {
        LOGGER.info("Dynamic Configuration in Kafka CR is {}={} and expected is {}={}",
            brokerConfigName,
            KafkaResource.kafkaClient().inNamespace(kubeClient().getNamespace()).withName(clusterName).get().getSpec().getKafka().getConfig().get(brokerConfigName),
            brokerConfigName,
            value);

        return KafkaResource.kafkaClient().inNamespace(kubeClient().getNamespace()).withName(clusterName).get().getSpec().getKafka().getConfig().get(brokerConfigName).equals(value);
    }

    /**
     * Verifies that updated configuration was successfully changed inside Kafka pods
     * @param kafkaPodNamePrefix prefix of Kafka pods
     * @param brokerConfigName key of specific property
     * @param value value of specific property
     * @return
     * true = if specific property match the excepted property
     * false = if specific property doesn't match the excepted property
     */
    public static boolean verifyPodDynamicConfiguration(String kafkaPodNamePrefix, String brokerConfigName, Object value) {

        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(kafkaPodNamePrefix);

        for (Pod pod : kafkaPods) {

            TestUtils.waitFor("Wait until dyn.configuration is changed", Constants.GLOBAL_POLL_INTERVAL, Constants.RECONCILIATION_INTERVAL + Duration.ofSeconds(10).toMillis(),
                () -> {
                    String result = cmdKubeClient().execInPod(pod.getMetadata().getName(), "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe").out();

                    LOGGER.debug("This dyn.configuration {} inside the Kafka pod {}", result, pod.getMetadata().getName());

                    if (!result.contains(brokerConfigName + "=" + value)) {
                        LOGGER.error("Kafka Pod {} doesn't contain {} with value {}", pod.getMetadata().getName(), brokerConfigName, value);
                        LOGGER.error("Kafka configuration {}", result);
                        return false;
                    }
                    return true;
                });
        }
        return true;
    }

    /**
     * Loads all kafka config parameters supported by the given {@code kafkaVersion}, as generated by #KafkaConfigModelGenerator in config-model-generator.
     * @param kafkaVersion specific kafka version
     * @return all supported kafka properties
     */
    public static Map<String, ConfigModel> readConfigModel(String kafkaVersion) {
        String name = TestUtils.USER_PATH + "/../cluster-operator/src/main/resources/kafka-" + kafkaVersion + "-config-model.json";
        try {
            try (InputStream in = new FileInputStream(name)) {
                ConfigModels configModels = new ObjectMapper().readValue(in, ConfigModels.class);
                if (!kafkaVersion.equals(configModels.getVersion())) {
                    throw new RuntimeException("Incorrect version");
                }
                return configModels.getConfigs();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading from classpath resource " + name, e);
        }
    }

    /**
     * Return dynamic Kafka configs supported by the the given version of Kafka.
     * @param kafkaVersion specific kafka version
     * @return all dynamic properties for specific kafka version
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:BooleanExpressionComplexity", "unchecked"})
    public static Map<String, ConfigModel> getDynamicConfigurationProperties(String kafkaVersion)  {

        Map<String, ConfigModel> configs = KafkaUtils.readConfigModel(kafkaVersion);

        LOGGER.info("Kafka config {}", configs.toString());

        LOGGER.info("Number of all kafka configs {}", configs.size());

        Map<String, ConfigModel> dynamicConfigs = configs
            .entrySet()
            .stream()
            .filter(a -> {
                String[] prefixKey = a.getKey().split("\\.");

                // filter all which is Scope = ClusterWide or PerBroker
                boolean isClusterWideOrPerBroker = a.getValue().getScope() == Scope.CLUSTER_WIDE || a.getValue().getScope() == Scope.PER_BROKER;

                if (prefixKey[0].equals("ssl") || prefixKey[0].equals("sasl") || prefixKey[0].equals("advertised") ||
                    prefixKey[0].equals("listeners") || prefixKey[0].equals("listener")) {
                    return isClusterWideOrPerBroker && !FORBIDDEN_PREFIXES.contains(prefixKey[0]);
                }

                return isClusterWideOrPerBroker;
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        LOGGER.info("Number of dynamic-configs {}", dynamicConfigs.size());

        Map<String, ConfigModel> forbiddenExceptionsConfigs = configs
            .entrySet()
            .stream()
            .filter(a -> FORBIDDEN_PREFIX_EXCEPTIONS.contains(a.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        LOGGER.info("Number of forbidden-exception-configs {}", forbiddenExceptionsConfigs.size());

        Map<String, ConfigModel> dynamicConfigsWithExceptions = new HashMap<>();

        dynamicConfigsWithExceptions.putAll(dynamicConfigs);
        dynamicConfigsWithExceptions.putAll(forbiddenExceptionsConfigs);

        LOGGER.info("Size of dynamic-configs with forbidden-exception-configs {}", dynamicConfigsWithExceptions.size());

        dynamicConfigsWithExceptions.forEach((key, value) -> LOGGER.info(key + " -> "  + value.getScope() + ":" + value.getType()));

        return dynamicConfigsWithExceptions;
    }

    /**
     * Generated random name for the Kafka resource based on prefix
     * @param clusterName name prefix
     * @return name with prefix and random salt
     */
    public static String generateRandomNameOfKafka(String clusterName) {
        return clusterName + "-" + new Random().nextInt(Integer.MAX_VALUE);
    }

    public static String getVersionFromKafkaPodLibs(String kafkaPodName) {
        String command = "ls libs | grep -Po 'kafka_\\d+.\\d+-\\K(\\d+.\\d+.\\d+)(?=.*jar)' | head -1 | cut -d \"-\" -f2";
        return cmdKubeClient().execInPodContainer(
            kafkaPodName,
            "kafka",
            "/bin/bash",
            "-c",
            command
        ).out().trim();
    }

    public static void waitForKafkaDeletion(String namespaceName, String kafkaClusterName) {
        LOGGER.info("Waiting for deletion of Kafka:{}", kafkaClusterName);
        TestUtils.waitFor("Kafka deletion " + kafkaClusterName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, DELETION_TIMEOUT,
            () -> {
                if (KafkaResource.kafkaClient().inNamespace(namespaceName).withName(kafkaClusterName).get() == null &&
                    kubeClient(namespaceName).getStatefulSet(namespaceName, KafkaResources.kafkaStatefulSetName(kafkaClusterName)) == null &&
                    kubeClient(namespaceName).getStatefulSet(namespaceName, KafkaResources.zookeeperStatefulSetName(kafkaClusterName)) == null &&
                    kubeClient(namespaceName).getDeployment(namespaceName, KafkaResources.entityOperatorDeploymentName(kafkaClusterName)) == null) {
                    return true;
                } else {
                    cmdKubeClient(namespaceName).deleteByName(Kafka.RESOURCE_KIND, kafkaClusterName);
                    return false;
                }
            },
            () -> LOGGER.info(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(kafkaClusterName).get()));
    }

    public static String getKafkaTlsListenerCaCertName(String namespace, String clusterName, String listenerName) {
        List<GenericKafkaListener> listeners = KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get().getSpec().getKafka().getListeners();

        GenericKafkaListener tlsListener = listenerName == null || listenerName.isEmpty() ?
            listeners.stream().filter(listener -> Constants.TLS_LISTENER_DEFAULT_NAME.equals(listener.getName())).findFirst().orElseThrow(RuntimeException::new) :
            listeners.stream().filter(listener -> listenerName.equals(listener.getName())).findFirst().orElseThrow(RuntimeException::new);
        return tlsListener.getConfiguration() == null ?
            KafkaResources.clusterCaCertificateSecretName(clusterName) : tlsListener.getConfiguration().getBrokerCertChainAndKey().getSecretName();
    }

    public static String getKafkaExternalListenerCaCertName(String namespace, String clusterName, String listenerName) {
        List<GenericKafkaListener> listeners = KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get().getSpec().getKafka().getListeners();

        GenericKafkaListener external = listenerName == null || listenerName.isEmpty() ?
            listeners.stream().filter(listener -> Constants.EXTERNAL_LISTENER_DEFAULT_NAME.equals(listener.getName())).findFirst().orElseThrow(RuntimeException::new) :
            listeners.stream().filter(listener -> listenerName.equals(listener.getName())).findFirst().orElseThrow(RuntimeException::new);

        if (external.getConfiguration() == null) {
            return KafkaResources.clusterCaCertificateSecretName(clusterName);
        } else {
            if (external.getConfiguration().getBrokerCertChainAndKey() != null) {
                return external.getConfiguration().getBrokerCertChainAndKey().getSecretName();
            } else {
                return KafkaResources.clusterCaCertificateSecretName(clusterName);
            }
        }
    }

    public static String changeOrRemoveKafkaVersion(File file, String version) {
        return changeOrRemoveKafkaConfiguration(file, version, null, null);
    }

    public static String changeOrRemoveKafkaConfiguration(File file, String version, String logMessageFormat, String interBrokerProtocol) {
        YAMLMapper mapper = new YAMLMapper();
        try {
            JsonNode node = mapper.readTree(file);
            ObjectNode kafkaNode = (ObjectNode) node.at("/spec/kafka");
            if (version == null) {
                kafkaNode.remove("version");
                ((ObjectNode) kafkaNode.get("config")).remove("log.message.format.version");
                ((ObjectNode) kafkaNode.get("config")).remove("inter.broker.protocol.version");
            } else if (!version.equals("")) {
                kafkaNode.put("version", version);
                ((ObjectNode) kafkaNode.get("config")).put("log.message.format.version", TestKafkaVersion.getSpecificVersion(version).messageVersion());
                ((ObjectNode) kafkaNode.get("config")).put("inter.broker.protocol.version", TestKafkaVersion.getSpecificVersion(version).protocolVersion());
            }
            if (logMessageFormat != null) {
                ((ObjectNode) kafkaNode.get("config")).put("log.message.format.version", logMessageFormat);
            }
            if (interBrokerProtocol != null) {
                ((ObjectNode) kafkaNode.get("config")).put("inter.broker.protocol.version", interBrokerProtocol);
            }
            return mapper.writeValueAsString(node);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
