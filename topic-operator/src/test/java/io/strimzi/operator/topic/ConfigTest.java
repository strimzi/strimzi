/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.strimzi.test.mockkube.MockKube;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.SaslConfigs;

import io.strimzi.operator.common.InvalidConfigurationException;

public class ConfigTest {

    private static final Map<String, String> MANDATORY = new HashMap<>();

    static {
        MANDATORY.put(Config.ZOOKEEPER_CONNECT.key, "localhost:2181");
        MANDATORY.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, "localhost:9092");
        MANDATORY.put(Config.NAMESPACE.key, "default");
        MANDATORY.put(Config.CLIENT_ID.key, "default-client-id");
    }

    @Test
    public void testUnknownKeyThrows() {
        assertThrows(IllegalArgumentException.class, () -> {
            new Config(Collections.singletonMap("foo", "bar"));
        });
    }

    @Test
    public void testEmptyMapThrows() {
        assertThrows(IllegalArgumentException.class, () -> {
            Config c = new Config(Collections.emptyMap());
        });
    }

    @Test
    public void testDefaultInput() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        Config c = new Config(map);
        assertThat(c.get(Config.ZOOKEEPER_SESSION_TIMEOUT_MS).intValue(), is(18_000));
    }

    @Test
    public void testOverrideZookeeperSessionTimeout() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.ZOOKEEPER_SESSION_TIMEOUT_MS.key, "13000");

        Config c = new Config(map);
        assertThat(c.get(Config.ZOOKEEPER_SESSION_TIMEOUT_MS).intValue(), is(13_000));
    }

    @Test
    public void testNewInvalidTimeout() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.ZOOKEEPER_SESSION_TIMEOUT_MS.key, "13000");

        assertDoesNotThrow(() -> new Config(map));

        map.put(Config.ZOOKEEPER_SESSION_TIMEOUT_MS.key, "foos");
        assertThrows(IllegalArgumentException.class, () -> new Config(map));
    }

    @Test
    public void testTopicMetadataMaxAttemptsIsSetCorrectly() {

        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.TC_TOPIC_METADATA_MAX_ATTEMPTS, "3");

        Config c = new Config(map);
        assertThat(c.get(Config.TOPIC_METADATA_MAX_ATTEMPTS).intValue(), is(3));
    }

    @Test
    public void testDefaultConfig() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        MockKube mockKube = new MockKube();
        KubernetesClient kubeClient = mockKube.build();

        Config config = new Config(map);
        Session session = new Session(kubeClient, config);
        Properties adminClientProps = session.adminClientProperties();

        assertThat(adminClientProps.getProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG), is("PLAINTEXT"));
        assertNull(adminClientProps.getProperty(SaslConfigs.SASL_MECHANISM));
        assertNull(adminClientProps.getProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG));
    }

    @Test
    public void testTlsEnabledConfig() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.TLS_ENABLED.key, "true");
        MockKube mockKube = new MockKube();
        KubernetesClient kubeClient = mockKube.build();

        Config config = new Config(map);
        Session session = new Session(kubeClient, config);
        Properties adminClientProps = session.adminClientProperties();

        assertThat(adminClientProps.getProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG), is("HTTPS"));
        assertThat(adminClientProps.getProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG), is("SSL"));
    }

    @Test
    public void testSecurityProtocolConfig() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.SECURITY_PROTOCOL.key, "SSL");
        MockKube mockKube = new MockKube();
        KubernetesClient kubeClient = mockKube.build();

        Config config = new Config(map);
        Session session = new Session(kubeClient, config);
        Properties adminClientProps = session.adminClientProperties();

        assertThat(adminClientProps.getProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG), is("HTTPS"));
        assertThat(adminClientProps.getProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG), is("SSL"));
    }

    @Test
    public void testInvalidSaslConfig() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.SASL_ENABLED.key, "true");
        MockKube mockKube = new MockKube();
        KubernetesClient kubeClient = mockKube.build();

        Config config = new Config(map);
        Session session = new Session(kubeClient, config);
        assertThrows(InvalidConfigurationException.class, () -> session.adminClientProperties());

        String username = "admin";
        String password = "password";
        map.put(Config.SASL_USERNAME.key, username);
        map.put(Config.SASL_PASSWORD.key, password);
        Config configWithCredentials = new Config(map);
        Session sessionWithCredentials = new Session(kubeClient, configWithCredentials);
        assertThrows(IllegalArgumentException.class, () -> sessionWithCredentials.adminClientProperties());
    }

    @Test
    public void testInvalidTlsSecurityProtocolConfig() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.SECURITY_PROTOCOL.key, "PLAINTEXT");
        map.put(Config.TLS_ENABLED.key, "true");

        MockKube mockKube = new MockKube();
        KubernetesClient kubeClient = mockKube.build();

        Config config = new Config(map);
        Session session = new Session(kubeClient, config);
        assertThrows(InvalidConfigurationException.class, () -> session.adminClientProperties());
    }

    @Test
    public void testInvalidKeystoreConfig() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.TLS_ENABLED.key, "true");
        map.put(Config.TLS_TRUSTSTORE_PASSWORD.key, "password");

        MockKube mockKube = new MockKube();
        KubernetesClient kubeClient = mockKube.build();

        Config config = new Config(map);
        Session session = new Session(kubeClient, config);
        assertThrows(InvalidConfigurationException.class, () -> session.adminClientProperties());
    }

    @Test
    public void testScramSaslConfig() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.SASL_ENABLED.key, "true");

        String username = "admin";
        String password = "password";
        map.put(Config.SASL_USERNAME.key, username);
        map.put(Config.SASL_PASSWORD.key, password);
        String scramJaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";";
        String plainJaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";";

        MockKube mockKube = new MockKube();
        KubernetesClient kubeClient = mockKube.build();

        map.put(Config.SASL_MECHANISM.key, "scram-sha-256");
        Config configSHA256 = new Config(map);
        Session sessionSHA256 = new Session(kubeClient, configSHA256);

        Properties adminClientPropsSHA256 = sessionSHA256.adminClientProperties();
        assertThat(adminClientPropsSHA256.getProperty(SaslConfigs.SASL_MECHANISM), is("SCRAM-SHA-256"));
        assertThat(adminClientPropsSHA256.getProperty(SaslConfigs.SASL_JAAS_CONFIG), is(scramJaasConfig));

        map.put(Config.SASL_MECHANISM.key, "scram-sha-512");
        Config configSHA512 = new Config(map);
        Session sessionSHA512 = new Session(kubeClient, configSHA512);

        Properties adminClientPropsSHA512 = sessionSHA512.adminClientProperties();
        assertThat(adminClientPropsSHA512.getProperty(SaslConfigs.SASL_MECHANISM), is("SCRAM-SHA-512"));
        assertThat(adminClientPropsSHA512.getProperty(SaslConfigs.SASL_JAAS_CONFIG), is(scramJaasConfig));

        map.put(Config.SASL_MECHANISM.key, "plain");
        Config configPlain = new Config(map);
        Session sessionPlain = new Session(kubeClient, configPlain);

        Properties adminClientPropsPlain = sessionPlain.adminClientProperties();
        assertThat(adminClientPropsPlain.getProperty(SaslConfigs.SASL_MECHANISM), is("PLAIN"));
        assertThat(adminClientPropsPlain.getProperty(SaslConfigs.SASL_JAAS_CONFIG), is(plainJaasConfig));
    }
}
