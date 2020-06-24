/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationOAuth;
import io.strimzi.api.kafka.model.listener.NodeAddressType;
import io.strimzi.api.kafka.model.listener.v2.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.v2.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.listener.v2.KafkaListenerType;
import io.strimzi.api.kafka.model.template.ExternalTrafficPolicy;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Util methods for working with Kafka listeners
 */
public class ListenersUtils {
    /*test*/ static final String BACKWARDS_COMPATIBLE_PLAIN_PORT_NAME = "tcp-clients";
    /*test*/ static final String BACKWARDS_COMPATIBLE_TLS_PORT_NAME = "tcp-clientstls";
    /*test*/ static final String BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME = "tcp-external";

    /**
     * Finds out if any of the listeners has OAuth authentication enabled
     *
     * @param listeners List of all listeners
     * @return          List of used names
     */
    public static boolean hasListenerWithOAuth(List<GenericKafkaListener> listeners)    {
        return listeners.stream()
                .filter(listener -> listener.getAuth() != null && KafkaListenerAuthenticationOAuth.TYPE_OAUTH.equals(listener.getAuth().getType()))
                .map(listener -> true)
                .findFirst()
                .orElse(false);
    }

    /**
     * Returns list of all internal listeners
     *
     * @param listeners List of all listeners
     * @return          List of internal listeners
     */
    public static List<GenericKafkaListener> internalListeners(List<GenericKafkaListener> listeners)    {
        return listeners.stream()
                .filter(listener -> KafkaListenerType.INTERNAL == listener.getType())
                .collect(Collectors.toList());
    }

    /**
     * Returns list of all external listeners (i.e. not internal)
     *
     * @param listeners List of all listeners
     * @return          List of external listeners
     */
    public static List<GenericKafkaListener> externalListeners(List<GenericKafkaListener> listeners)    {
        return listeners.stream()
                .filter(listener -> KafkaListenerType.INTERNAL != listener.getType())
                .collect(Collectors.toList());
    }

    /**
     * Returns list of all Route type listeners
     *
     * @param listeners List of all listeners
     * @return          List of route listeners
     */
    public static List<GenericKafkaListener> routeListeners(List<GenericKafkaListener> listeners)    {
        return listeners.stream()
                .filter(listener -> KafkaListenerType.ROUTE == listener.getType())
                .collect(Collectors.toList());
    }

    /**
     * Returns list of all LoadBalancer type listeners
     *
     * @param listeners List of all listeners
     * @return          List of load balancer listeners
     */
    public static List<GenericKafkaListener> loadBalancerListeners(List<GenericKafkaListener> listeners)    {
        return listeners.stream()
                .filter(listener -> KafkaListenerType.LOADBALANCER == listener.getType())
                .collect(Collectors.toList());
    }

    /**
     * Returns list of all NodePort type listeners
     *
     * @param listeners List of all listeners
     * @return          List of node port listeners
     */
    public static List<GenericKafkaListener> nodePortListeners(List<GenericKafkaListener> listeners)    {
        return listeners.stream()
                .filter(listener -> KafkaListenerType.NODEPORT == listener.getType())
                .collect(Collectors.toList());
    }

    /**
     * Returns list of all Ingress type listeners
     *
     * @param listeners List of all listeners
     * @return          List of ingress listeners
     */
    public static List<GenericKafkaListener> ingressListeners(List<GenericKafkaListener> listeners)    {
        return listeners.stream()
                .filter(listener -> KafkaListenerType.INGRESS == listener.getType())
                .collect(Collectors.toList());
    }

    /**
     * Returns list of all additional DNS addresses for certificates
     *
     * @param listeners List of all listeners
     * @return          List of external listeners
     */
    public static List<String> alternativeNames(List<GenericKafkaListener> listeners)    {
        return listeners.stream()
                .filter(listener -> listener.getConfiguration() != null
                        && listener.getConfiguration().getBootstrap() != null
                        && listener.getConfiguration().getBootstrap().getAlternativeNames() != null)
                .map(listener -> listener.getConfiguration().getBootstrap().getAlternativeNames())
                .flatMap(List::stream)
                .distinct()
                .collect(Collectors.toList());
    }

    /**
     * Generates a listener identifier which is used to name the related volumes, volume mounts, etc.
     *
     * @param listener  Listener for which the name should be generated
     * @return          Identifier string
     */
    public static String identifier(GenericKafkaListener listener) {
        return listener.getName() + "-" + listener.getPort();
    }

    /**
     * Generates a listener identifier which can be used in environment variables
     *
     * @param listener  Listener for which the name should be generated
     * @return          Identifier string
     */
    public static String envVarIdentifier(GenericKafkaListener listener) {
        return listener.getName().toUpperCase(Locale.ENGLISH) + "_" + listener.getPort();
    }

    /**
     * Generates port names which are backwards compatible with the previous Strimzi versions
     *
     * @param listener  Listener for which the name should be generated
     * @return          Name of the port
     */
    public static String backwardsCompatiblePortName(GenericKafkaListener listener) {
        if (listener.getPort() == 9092 && "plain".equals(listener.getName()) && KafkaListenerType.INTERNAL == listener.getType())   {
            return BACKWARDS_COMPATIBLE_PLAIN_PORT_NAME;
        } else if (listener.getPort() == 9093 && "tls".equals(listener.getName()) && KafkaListenerType.INTERNAL == listener.getType())   {
            return BACKWARDS_COMPATIBLE_TLS_PORT_NAME;
        } else if (listener.getPort() == 9094 && "external".equals(listener.getName()))   {
            return BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME;
        } else {
            return "tcp-" + listener.getName();
        }
    }

    /**
     * Generates bootstrap service names which are backwards compatible with the previous Strimzi versions
     *
     * @param clusterName Name of the cluster to which this service belongs
     * @param listener  Listener for which the name should be generated
     * @return          Name of the bootstrap service
     */
    public static String backwardsCompatibleBootstrapServiceName(String clusterName, GenericKafkaListener listener) {
        if (listener.getPort() == 9092 && "plain".equals(listener.getName()) && KafkaListenerType.INTERNAL == listener.getType())   {
            return clusterName + "-kafka-bootstrap";
        } else if (listener.getPort() == 9093 && "tls".equals(listener.getName()) && KafkaListenerType.INTERNAL == listener.getType())   {
            return clusterName + "-kafka-bootstrap";
        } else if (listener.getPort() == 9094 && "external".equals(listener.getName()))   {
            return clusterName + "-kafka-external-bootstrap";
        } else if (KafkaListenerType.INTERNAL == listener.getType()) {
            return clusterName + "-kafka-bootstrap";
        } else {
            return clusterName + "-kafka-" + listener.getName() + "-bootstrap";
        }
    }

    /**
     * Generates bootstrap route or ingress names which are backwards compatible with the previous Strimzi versions
     *
     * @param clusterName Name of the cluster to which this service belongs
     * @param listener  Listener for which the name should be generated
     * @return          Name of the bootstrap service
     */
    public static String backwardsCompatibleBootstrapRouteOrIngressName(String clusterName, GenericKafkaListener listener) {
        if (listener.getPort() == 9092 && "plain".equals(listener.getName()) && KafkaListenerType.INTERNAL == listener.getType())   {
            throw new UnsupportedOperationException("Bootstrap routes or ingresses are not used for internal listeners");
        } else if (listener.getPort() == 9093 && "tls".equals(listener.getName()) && KafkaListenerType.INTERNAL == listener.getType())   {
            throw new UnsupportedOperationException("Bootstrap routes or ingresses are not used for internal listeners");
        } else if (listener.getPort() == 9094 && "external".equals(listener.getName()))   {
            return clusterName + "-kafka-bootstrap";
        } else if (KafkaListenerType.INTERNAL == listener.getType()) {
            throw new UnsupportedOperationException("Bootstrap routes or ingresses are not used for internal listeners");
        } else {
            return clusterName + "-kafka-" + listener.getName() + "-bootstrap";
        }
    }

    /**
     * Generates broker service names which are backwards compatible with the previous Strimzi versions.
     *
     * @throws  UnsupportedOperationException Throws UnsupportedOperationException if called for internal service
     *                                          which does not have per-pod services
     *
     * @param clusterName Name of the cluster to which this service belongs
     * @param pod         Number of the pod for which this service will be used
     * @param listener  Listener for which the name should be generated
     * @return          Name of the bootstrap service
     */
    public static String backwardsCompatibleBrokerServiceName(String clusterName, int pod, GenericKafkaListener listener) {
        if (listener.getPort() == 9092 && "plain".equals(listener.getName()) && KafkaListenerType.INTERNAL == listener.getType())   {
            throw new UnsupportedOperationException("Per-broker services are not used for internal listener");
        } else if (listener.getPort() == 9093 && "tls".equals(listener.getName()) && KafkaListenerType.INTERNAL == listener.getType())   {
            throw new UnsupportedOperationException("Per-broker services are not used for internal listener");
        } else if (listener.getPort() == 9094 && "external".equals(listener.getName()))   {
            return clusterName + "-kafka-" + pod;
        } else if (KafkaListenerType.INTERNAL == listener.getType()) {
            throw new UnsupportedOperationException("Per-broker services are not used for internal listener");
        } else {
            return clusterName + "-kafka-" + listener.getName() + "-" + pod;
        }
    }

    /**
     * Finds bootstrap service node port
     *
     * @param listener  Listener for which the port should be found
     * @return          Node port or null if not specified
     */
    public static Integer bootstrapNodePort(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBootstrap() != null) {
            return listener.getConfiguration().getBootstrap().getNodePort();
        } else {
            return null;
        }
    }

    /**
     * Finds bootstrap service load balancer IP
     *
     * @param listener  Listener for which the load balancer IP should be found
     * @return          Load Balancer or null if not specified
     */
    public static String bootstrapLoadBalancerIP(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBootstrap() != null) {
            return listener.getConfiguration().getBootstrap().getLoadBalancerIP();
        } else {
            return null;
        }
    }

    /**
     * Finds bootstrap service DNS annotations
     *
     * @param listener  Listener for which the load balancer IP should be found
     * @return          Map with DNS annotations or empty map if not specified
     */
    public static Map<String, String> bootstrapDnsAnnotations(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBootstrap() != null
                && listener.getConfiguration().getBootstrap().getDnsAnnotations() != null) {
            return listener.getConfiguration().getBootstrap().getDnsAnnotations();
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Finds bootstrap host
     *
     * @param listener  Listener for which the host should be found
     * @return          Host name or null if not specified
     */
    public static String bootstrapHost(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBootstrap() != null) {
            return listener.getConfiguration().getBootstrap().getHost();
        } else {
            return null;
        }
    }

    /**
     * Finds broker service node port
     *
     * @param listener  Listener for which the port should be found
     * @param pod       Pod ID for which we should get the configuration option
     * @return          Node port or null if not specified
     */
    public static Integer brokerNodePort(GenericKafkaListener listener, int pod)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBrokers() != null) {
            return listener.getConfiguration().getBrokers().stream()
                    .filter(broker -> broker != null && broker.getBroker() != null && broker.getBroker() == pod && broker.getNodePort() != null)
                    .map(GenericKafkaListenerConfigurationBroker::getNodePort)
                    .findAny()
                    .orElse(null);
        } else {
            return null;
        }
    }

    /**
     * Finds broker service load balancer IP
     *
     * @param listener  Listener for which the load balancer IP should be found
     * @param pod       Pod ID for which we should get the configuration option
     * @return          Load Balancer or null if not specified
     */
    public static String brokerLoadBalancerIP(GenericKafkaListener listener, int pod)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBrokers() != null) {
            return listener.getConfiguration().getBrokers().stream()
                    .filter(broker -> broker != null && broker.getBroker() != null && broker.getBroker() == pod && broker.getLoadBalancerIP() != null)
                    .map(GenericKafkaListenerConfigurationBroker::getLoadBalancerIP)
                    .findAny()
                    .orElse(null);
        } else {
            return null;
        }
    }

    /**
     * Finds broker service DNS annotations
     *
     * @param listener  Listener for which the load balancer IP should be found
     * @param pod       Pod ID for which we should get the configuration option
     * @return          Map with DNS annotations or empty map if not specified
     */
    public static Map<String, String> brokerDnsAnnotations(GenericKafkaListener listener, int pod)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBrokers() != null) {
            return listener.getConfiguration().getBrokers().stream()
                    .filter(broker -> broker != null && broker.getBroker() != null && broker.getBroker() == pod && broker.getDnsAnnotations() != null)
                    .map(GenericKafkaListenerConfigurationBroker::getDnsAnnotations)
                    .findAny()
                    .orElse(Collections.emptyMap());
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Finds broker host configuration
     *
     * @param listener  Listener for which the host should be found
     * @param pod       Pod ID for which we should get the configuration option
     * @return          Host or null if not specified
     */
    public static String brokerHost(GenericKafkaListener listener, int pod)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBrokers() != null) {
            return listener.getConfiguration().getBrokers().stream()
                    .filter(broker -> broker != null && broker.getBroker() != null && broker.getBroker() == pod && broker.getHost() != null)
                    .map(GenericKafkaListenerConfigurationBroker::getHost)
                    .findAny()
                    .orElse(null);
        } else {
            return null;
        }
    }

    /**
     * Finds broker advertised host configuration
     *
     * @param listener  Listener for which the advertised host should be found
     * @param pod       Pod ID for which we should get the configuration option
     * @return          Advertised Host or null if not specified
     */
    public static String brokerAdvertisedHost(GenericKafkaListener listener, int pod)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBrokers() != null) {
            return listener.getConfiguration().getBrokers().stream()
                    .filter(broker -> broker != null && broker.getBroker() != null && broker.getBroker() == pod && broker.getAdvertisedHost() != null)
                    .map(GenericKafkaListenerConfigurationBroker::getAdvertisedHost)
                    .findAny()
                    .orElse(null);
        } else {
            return null;
        }
    }

    /**
     * Finds broker advertised port configuration
     *
     * @param listener  Listener for which the advertised port should be found
     * @param pod       Pod ID for which we should get the configuration option
     * @return          Advertised port or null if not specified
     */
    public static Integer brokerAdvertisedPort(GenericKafkaListener listener, int pod)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBrokers() != null) {
            return listener.getConfiguration().getBrokers().stream()
                    .filter(broker -> broker != null && broker.getBroker() != null && broker.getBroker() == pod && broker.getAdvertisedPort() != null)
                    .map(GenericKafkaListenerConfigurationBroker::getAdvertisedPort)
                    .findAny()
                    .orElse(null);
        } else {
            return null;
        }
    }

    /**
     * Finds load balancer source ranges
     *
     * @param listener  Listener for which the load balancer source ranges should be found
     * @return          Load Balancer source ranges or null if not specified
     */
    public static List<String> loadBalancerSourceRanges(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null) {
            return listener.getConfiguration().getLoadBalancerSourceRanges();
        } else {
            return null;
        }
    }

    /**
     * Finds external traffic policy
     *
     * @param listener  Listener for which the external traffic policy should be found
     * @return          External traffic policy or null if not specified
     */
    public static ExternalTrafficPolicy externalTrafficPolicy(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null) {
            return listener.getConfiguration().getExternalTrafficPolicy();
        } else {
            return null;
        }
    }

    /**
     * Finds preferred node address type
     *
     * @param listener  Listener for which the preferred node address type should be found
     * @return          Preferred node address type or null if not specified
     */
    public static NodeAddressType preferredNodeAddressType(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null) {
            return listener.getConfiguration().getPreferredAddressType();
        } else {
            return null;
        }
    }

    /**
     * Finds ingress class
     *
     * @param listener  Listener for which the ingress class should be found
     * @return          Ingress class or null if not specified
     */
    public static String ingressClass(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null) {
            return listener.getConfiguration().getIngressClass();
        } else {
            return null;
        }
    }

    /**
     * Utility function to help to determine the type of service based on external listener configuration
     *
     * @param listener Kafka listener for which we want to get the service type
     *
     * @return Service type
     */
    public static String getServiceType(GenericKafkaListener listener) {
        if (listener.getType() == KafkaListenerType.NODEPORT) {
            return "NodePort";
        } else if (listener.getType() == KafkaListenerType.LOADBALANCER) {
            return "LoadBalancer";
        } else {
            return "ClusterIP";
        }
    }
}
