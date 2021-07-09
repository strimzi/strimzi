/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TestKafkaVersion implements Comparable<TestKafkaVersion> {

    private static final Logger LOGGER = LogManager.getLogger(TestKafkaVersion.class);
    private static List<TestKafkaVersion> kafkaVersions;
    private static List<TestKafkaVersion> supportedKafkaVersions;

    static {
        try {
            kafkaVersions = parseKafkaVersions(TestUtils.USER_PATH + "/../kafka-versions.yaml");
            supportedKafkaVersions = kafkaVersions.stream().filter(TestKafkaVersion::isSupported).collect(Collectors.toList());
            Collections.sort(kafkaVersions);
            Collections.sort(supportedKafkaVersions);

            if (supportedKafkaVersions == null || supportedKafkaVersions.size() == 0) {
                throw new Exception("There is no one Kafka version supported inside " + TestUtils.USER_PATH + "/../kafka-versions.yaml file");
            }

            LOGGER.debug("These are following Kafka versions:\n{}", kafkaVersions.toString());
            LOGGER.debug("These are following supported Kafka versions:\n{}", supportedKafkaVersions.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<TestKafkaVersion> parseKafkaVersionsFromUrl(String url) throws IOException {
        File kafkaVersions = FileUtils.downloadYaml(url);
        return parseKafkaVersions(kafkaVersions.getAbsolutePath());
    }

    @JsonProperty("version")
    String version;

    @JsonProperty("protocol")
    String protocolVersion;

    @JsonProperty("format")
    String messageVersion;

    @JsonProperty("zookeeper")
    String zookeeperVersion;

    @JsonProperty("default")
    boolean isDefault;

    @JsonProperty("supported")
    boolean isSupported;

    @Override
    public String toString() {
        return "KafkaVersion{" +
                "version='" + version + '\'' +
                ", protocolVersion='" + protocolVersion + '\'' +
                ", messageVersion='" + messageVersion + '\'' +
                ", zookeeperVersion='" + zookeeperVersion + '\'' +
                ", isDefault=" + isDefault +
                ", isSupported=" + isSupported +
                '}';
    }

    public String version() {
        return version;
    }

    public String protocolVersion() {
        return protocolVersion;
    }

    public String messageVersion() {
        return messageVersion;
    }

    public String zookeeperVersion() {
        return zookeeperVersion;
    }

    public boolean isDefault() {
        return isDefault;
    }

    public boolean isSupported() {
        return isSupported;
    }

    @Override
    public int compareTo(TestKafkaVersion o) {
        return compareDottedVersions(this.version, o.version);
    }

    /**
     * Compare two decimal version strings, e.g. 1.10.1 &gt; 1.9.2
     *
     * @param version1 The first version.
     * @param version2 The second version.
     * @return Zero if version1 == version2;
     * -1 if version1 &lt; version2;
     * 1 if version1 &gt; version2.
     */
    public static int compareDottedVersions(String version1, String version2) {
        String[] components = version1.split("\\.");
        String[] otherComponents = version2.split("\\.");
        for (int i = 0; i < Math.min(components.length, otherComponents.length); i++) {
            int x = Integer.parseInt(components[i]);
            int y = Integer.parseInt(otherComponents[i]);
            if (x == y) {
                continue;
            } else if (x < y) {
                return -1;
            } else {
                return 1;
            }
        }
        return components.length - otherComponents.length;
    }

    public boolean isUpgrade(TestKafkaVersion version) {
        return compareTo(version) < 0;
    }

    @Override
    public int hashCode() {
        return version.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestKafkaVersion that = (TestKafkaVersion) o;
        return version.equals(that.version);
    }

    /**
     * Parse the version information present in the {@code /kafka-versions} classpath resource and return a sorted list
     * from earliest to latest kafka version.
     * @param versionsFilePath path to versions file, use path to root or download new one and pass it to there
     *
     * @return A list of the kafka versions listed in the kafka-versions.yaml file
     */
    private static List<TestKafkaVersion> parseKafkaVersions(String versionsFilePath) throws IOException {

        YAMLMapper mapper = new YAMLMapper();

        Reader versionsFileReader = new InputStreamReader(
                new FileInputStream(versionsFilePath),
                StandardCharsets.UTF_8);

        List<TestKafkaVersion> kafkaVersions = mapper.readValue(versionsFileReader, new TypeReference<>() {
        });

        return kafkaVersions;
    }

    public static List<TestKafkaVersion> getSupportedKafkaVersions() {
        return supportedKafkaVersions;
    }

    /**
     * Parse the version information present in the {@code /kafka-versions} classpath resource and return a map
     * of kafka versions data with a version as key
     *
     * @return A map of the kafka versions listed in the kafka-versions.yaml file where key is specific version
     */
    public static Map<String, TestKafkaVersion> getKafkaVersionsInMap() {
        return kafkaVersions.stream().collect(Collectors.toMap(TestKafkaVersion::version, i -> i));
    }

    public static boolean containsVersion(String kafkaVersion) {
        return kafkaVersions.stream().map(item -> item.version()).collect(Collectors.toList()).contains(kafkaVersion);
    }

    public static TestKafkaVersion getSpecificVersion(String kafkaVersion) {
        // One specific version will always be only once in the list
        return kafkaVersions.stream().filter(it -> it.version.equals(kafkaVersion)).collect(Collectors.toList()).get(0);
    }
}
