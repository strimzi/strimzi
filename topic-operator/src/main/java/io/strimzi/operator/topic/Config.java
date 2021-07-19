/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class Config {

    private static abstract class Type<T> {

        abstract T parse(String s);
    }

    /** A java string */
    private static final Type<? extends String> STRING = new Type<String>() {
        @Override
        public String parse(String s) {
            return s;
        }
    };

    /** A java Long */
    private static final Type<? extends Long> LONG = new Type<Long>() {
        @Override
        public Long parse(String s) {
            return Long.parseLong(s);
        }
    };

    /** A Java Integer */
    private static final Type<? extends Integer> POSITIVE_INTEGER = new Type<Integer>() {
        @Override
        Integer parse(String s) {
            int value = Integer.parseInt(s);
            if (value <= 0) {
                throw new IllegalArgumentException("The value must be greater than zero");
            }
            return value;
        }
    };

    /** A Java Boolean */
    private static final Type<? extends Boolean> BOOLEAN = new Type<Boolean>() {
        @Override
        Boolean parse(String s) {
            return Boolean.parseBoolean(s);
        }
    };

    /**
     * A time duration.
     */
    private static final Type<? extends Long> DURATION = new Type<Long>() {

        @Override
        public Long parse(String s) {
            return Long.parseLong(s);
        }
    };

    /**
     * A kubernetes selector.
     */
    private static final Type<? extends Labels> LABEL_PREDICATE = new Type<Labels>() {
        @Override
        public Labels parse(String s) {
            return Labels.fromString(s);
        }
    };

    static class Value<T> {
        public final String key;
        public final String defaultValue;
        public final boolean required;
        private final Type<? extends T> type;
        private Value(String key, Type<? extends T> type, String defaultValue) {
            this.key = key;
            this.type = type;
            if (defaultValue != null) {
                type.parse(defaultValue);
            }
            this.defaultValue = defaultValue;
            this.required = false;
        }
        private Value(String key, Type<? extends T> type, boolean required) {
            this.key = key;
            this.type = type;
            this.defaultValue = null;
            this.required = required;
        }
    }

    public static final String TC_RESOURCE_LABELS = "STRIMZI_RESOURCE_LABELS";
    public static final String TC_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    public static final String TC_NAMESPACE = "STRIMZI_NAMESPACE";
    public static final String TC_CLIENT_ID = "STRIMZI_CLIENT_ID";
    public static final String TC_ZK_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";
    public static final String TC_ZK_SESSION_TIMEOUT_MS = "STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS";
    public static final String TC_ZK_CONNECTION_TIMEOUT_MS = "TC_ZK_CONNECTION_TIMEOUT_MS";
    public static final String TC_PERIODIC_INTERVAL_MS = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    public static final String TC_REASSIGN_THROTTLE = "STRIMZI_REASSIGN_THROTTLE";
    public static final String TC_REASSIGN_VERIFY_INTERVAL_MS = "STRIMZI_REASSIGN_VERIFY_INTERVAL_MS";
    public static final String TC_TOPIC_METADATA_MAX_ATTEMPTS = "STRIMZI_TOPIC_METADATA_MAX_ATTEMPTS";
    public static final String TC_TOPICS_PATH = "STRIMZI_TOPICS_PATH";

    public static final String TC_TLS_ENABLED = "STRIMZI_TLS_ENABLED";
    public static final String TC_TLS_TRUSTSTORE_LOCATION = "STRIMZI_TRUSTSTORE_LOCATION";
    public static final String TC_TLS_TRUSTSTORE_PASSWORD = "STRIMZI_TRUSTSTORE_PASSWORD";
    public static final String TC_TLS_KEYSTORE_LOCATION = "STRIMZI_KEYSTORE_LOCATION";
    public static final String TC_TLS_KEYSTORE_PASSWORD = "STRIMZI_KEYSTORE_PASSWORD";
    public static final String TC_TLS_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = "STRIMZI_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM";

    public static final String TC_SASL_ENABLED = "STRIMZI_SASL_ENABLED";
    public static final String TC_SASL_MECHANISM = "STRIMZI_SASL_MECHANISM";
    public static final String TC_SASL_USERNAME = "STRIMZI_SASL_USERNAME";
    public static final String TC_SASL_PASSWORD = "STRIMZI_SASL_PASSWORD";

    public static final String TC_SECURITY_PROTOCOL = "STRIMZI_SECURITY_PROTOCOL";

    public static final String TC_STORE_TOPIC = "STRIMZI_STORE_TOPIC";
    public static final String TC_STORE_NAME = "STRIMZI_STORE_NAME";
    public static final String TC_APPLICATION_ID = "STRIMZI_APPLICATION_ID";
    public static final String TC_APPLICATION_SERVER = "STRIMZI_APPLICATION_SERVER";
    public static final String TC_STALE_RESULT_TIMEOUT_MS = "STRIMZI_STALE_RESULT_TIMEOUT_MS";

    public static final String TC_USE_ZOOKEEPER_TOPIC_STORE = "STRIMZI_USE_ZOOKEEPER_TOPIC_STORE";

    private static final Map<String, Value<?>> CONFIG_VALUES = new HashMap<>();

    /** A comma-separated list of key=value pairs for selecting Resources that describe topics. */
    public static final Value<Labels> LABELS = new Value<>(TC_RESOURCE_LABELS, LABEL_PREDICATE, "");

    /** A comma-separated list of kafka bootstrap servers. */
    public static final Value<String> KAFKA_BOOTSTRAP_SERVERS = new Value<>(TC_KAFKA_BOOTSTRAP_SERVERS, STRING, true);

    /** The kubernetes namespace in which to operate. */
    public static final Value<String> NAMESPACE = new Value<>(TC_NAMESPACE, STRING, true);

    /** Client-ID. */
    public static final Value<String> CLIENT_ID = new Value<>(TC_CLIENT_ID, STRING, "strimzi-topic-operator-" + UUID.randomUUID());

    /** The zookeeper connection string. */
    public static final Value<String> ZOOKEEPER_CONNECT = new Value<>(TC_ZK_CONNECT, STRING, true);

    /** The zookeeper session timeout. */
    public static final Value<Long> ZOOKEEPER_SESSION_TIMEOUT_MS = new Value<>(TC_ZK_SESSION_TIMEOUT_MS, DURATION, "18000");

    /** The zookeeper connection timeout. */
    public static final Value<Long> ZOOKEEPER_CONNECTION_TIMEOUT_MS = new Value<>(TC_ZK_CONNECTION_TIMEOUT_MS, DURATION, "18000");

    /** The period between full reconciliations. */
    public static final Value<Long> FULL_RECONCILIATION_INTERVAL_MS = new Value<>(TC_PERIODIC_INTERVAL_MS, DURATION, "120000");

    /** The interbroker throttled rate to use when a topic change requires partition reassignment. */
    public static final Value<Long> REASSIGN_THROTTLE = new Value<>(TC_REASSIGN_THROTTLE, LONG, Long.toString(Long.MAX_VALUE));

    /**
     * The interval between verification executions (as in {@code kafka-reassign-partitions.sh --verify ...})
     * when a topic change requires partition reassignment.
     */
    public static final Value<Long> REASSIGN_VERIFY_INTERVAL_MS = new Value<>(TC_REASSIGN_VERIFY_INTERVAL_MS, DURATION, "120000");

    /** The maximum number of retries for getting topic metadata from the Kafka cluster */
    public static final Value<Integer> TOPIC_METADATA_MAX_ATTEMPTS = new Value<>(TC_TOPIC_METADATA_MAX_ATTEMPTS, POSITIVE_INTEGER, "6");

    /** The path to the Zookeeper node that stores the topic state in ZooKeeper. */
    public static final Value<String> TOPICS_PATH = new Value<>(TC_TOPICS_PATH, STRING, "/strimzi/topics");

    /** If the connection with Kafka has to be encrypted by TLS protocol */
    public static final Value<String> TLS_ENABLED = new Value<>(TC_TLS_ENABLED, STRING, "false");
    /** The truststore with CA certificate for Kafka broker/server authentication */
    public static final Value<String> TLS_TRUSTSTORE_LOCATION = new Value<>(TC_TLS_TRUSTSTORE_LOCATION, STRING, "");
    /** The password for the truststore with CA certificate for Kafka broker/server authentication */
    public static final Value<String> TLS_TRUSTSTORE_PASSWORD = new Value<>(TC_TLS_TRUSTSTORE_PASSWORD, STRING, "");
    /** The keystore with private key and certificate for client authentication against Kafka broker */
    public static final Value<String> TLS_KEYSTORE_LOCATION = new Value<>(TC_TLS_KEYSTORE_LOCATION, STRING, "");
    /** The password for keystore with private key and certificate for client authentication against Kafka broker */
    public static final Value<String> TLS_KEYSTORE_PASSWORD = new Value<>(TC_TLS_KEYSTORE_PASSWORD, STRING, "");
    /** The endpoint identification algorithm used by clients to validate server host name. The default value is https. Clients including client connections created by the broker for inter-broker communication verify that the broker host name matches the host name in the broker’s certificate. Disable server host name verification by setting to an empty string.**/
    public static final Value<String> TLS_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = new Value<>(TC_TLS_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, STRING, "HTTPS");
    /** If SASL should be used to authenticate with */
    public static final Value<String> SASL_ENABLED = new Value<>(TC_SASL_ENABLED, STRING, "false");
    /** The SASL mechanism to be used */
    public static final Value<String> SASL_MECHANISM = new Value<>(TC_SASL_MECHANISM, STRING, "");
    /** The SASL username to be used for authentication */
    public static final Value<String> SASL_USERNAME = new Value<>(TC_SASL_USERNAME, STRING, "");
    /** The SASL password to be used for authentication */
    public static final Value<String> SASL_PASSWORD = new Value<>(TC_SASL_PASSWORD, STRING, "");
    /** The security protocol to be used */
    public static final Value<String> SECURITY_PROTOCOL = new Value<>(TC_SECURITY_PROTOCOL, STRING, "");

    /**
     * The store topic for the Kafka Streams based TopicStore
     */
    public static final Value<String> STORE_TOPIC = new Value<>(TC_STORE_TOPIC, STRING, "__strimzi_store_topic");
    /** The store name for the Kafka Streams based TopicStore */
    public static final Value<String> STORE_NAME = new Value<>(TC_STORE_NAME, STRING, "topic-store");
    /** The application id for the Kafka Streams based TopicStore */
    public static final Value<String> APPLICATION_ID = new Value<>(TC_APPLICATION_ID, STRING, "__strimzi-topic-operator-kstreams");
    /** The (gRPC) application server for the Kafka Streams based TopicStore */
    public static final Value<String> APPLICATION_SERVER = new Value<>(TC_APPLICATION_SERVER, STRING, "localhost:9000");
    /** The stale timeout for the Kafka Streams based TopicStore */
    public static final Value<Long> STALE_RESULT_TIMEOUT_MS = new Value<>(TC_STALE_RESULT_TIMEOUT_MS, DURATION, "5000");

    /** Do we use old ZooKeeper based TopicStore */
    public static final Value<Boolean> USE_ZOOKEEPER_TOPIC_STORE = new Value<>(TC_USE_ZOOKEEPER_TOPIC_STORE, BOOLEAN, "false");

    static {
        Map<String, Value<?>> configValues = CONFIG_VALUES;
        addConfigValue(configValues, LABELS);
        addConfigValue(configValues, KAFKA_BOOTSTRAP_SERVERS);
        addConfigValue(configValues, NAMESPACE);
        addConfigValue(configValues, CLIENT_ID);
        addConfigValue(configValues, ZOOKEEPER_CONNECT);
        addConfigValue(configValues, ZOOKEEPER_SESSION_TIMEOUT_MS);
        addConfigValue(configValues, ZOOKEEPER_CONNECTION_TIMEOUT_MS);
        addConfigValue(configValues, FULL_RECONCILIATION_INTERVAL_MS);
        addConfigValue(configValues, REASSIGN_THROTTLE);
        addConfigValue(configValues, REASSIGN_VERIFY_INTERVAL_MS);
        addConfigValue(configValues, TOPIC_METADATA_MAX_ATTEMPTS);
        addConfigValue(configValues, TOPICS_PATH);
        addConfigValue(configValues, TLS_ENABLED);
        addConfigValue(configValues, SECURITY_PROTOCOL);
        addConfigValue(configValues, TLS_TRUSTSTORE_LOCATION);
        addConfigValue(configValues, TLS_TRUSTSTORE_PASSWORD);
        addConfigValue(configValues, TLS_KEYSTORE_LOCATION);
        addConfigValue(configValues, TLS_KEYSTORE_PASSWORD);
        addConfigValue(configValues, SASL_ENABLED);
        addConfigValue(configValues, SASL_MECHANISM);
        addConfigValue(configValues, SASL_USERNAME);
        addConfigValue(configValues, SASL_PASSWORD);
        addConfigValue(configValues, TLS_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM);
        addConfigValue(configValues, STORE_TOPIC);
        addConfigValue(configValues, STORE_NAME);
        addConfigValue(configValues, APPLICATION_ID);
        addConfigValue(configValues, APPLICATION_SERVER);
        addConfigValue(configValues, STALE_RESULT_TIMEOUT_MS);
        addConfigValue(configValues, USE_ZOOKEEPER_TOPIC_STORE);
    }

    static void addConfigValue(Map<String, Value<?>> configValues, Value<?> cv) {
        if (configValues.put(cv.key, cv) != null) {
            throw new RuntimeException();
        }
    }

    private final Map<String, Object> map;

    public Config(Map<String, String> map) {
        this.map = new HashMap<>(map.size());
        for (Map.Entry<String, String> entry : map.entrySet()) {
            final Value<?> configValue = CONFIG_VALUES.get(entry.getKey());
            if (configValue == null) {
                throw new IllegalArgumentException("Unknown config key " + entry.getKey());
            }
            this.map.put(configValue.key, get(map, configValue));
        }
        // now add all those config (with default value) that weren't in the given map
        Map<String, Value> x = new HashMap<>(CONFIG_VALUES);
        x.keySet().removeAll(map.keySet());
        for (Value<?> value : x.values()) {
            this.map.put(value.key, get(map, value));
        }
    }

    public static Collection<Value<?>> keys() {
        return Collections.unmodifiableCollection(CONFIG_VALUES.values());
    }

    public static Set<String> keyNames() {
        return Collections.unmodifiableSet(CONFIG_VALUES.keySet());
    }

    private <T> T get(Map<String, String> map, Value<T> value) {
        if (!CONFIG_VALUES.containsKey(value.key)) {
            throw new IllegalArgumentException("Unknown config value: " + value.key + " probably needs to be added to Config.CONFIG_VALUES");
        }
        final String s = map.getOrDefault(value.key, value.defaultValue);
        if (s != null) {
            return (T) value.type.parse(s);
        } else {
            if (value.required) {
                throw new IllegalArgumentException("Config value: " + value.key + " is mandatory");
            }
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T get(Value<T> value, T defaultValue) {
        return (T) this.map.getOrDefault(value.key, defaultValue);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(Value<T> value) {
        return (T) this.map.get(value.key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Config config = (Config) o;
        return Objects.equals(map, config.map);
    }

    @Override
    public int hashCode() {
        return Objects.hash(map);
    }

    // TODO Generate documentation about the env vars
}
