/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

/**
 * Enum with argument for external clients
 */
public enum ClientArgument {
    // Common
    TOPIC("--topic"),
    VERBOSE("--verbose"),
    GROUP("--group"),
    DESCRIBE("--describe"),

    // Consumer
    CONSUMER_CONFIG("--consumer.config"),
    MAX_MESSAGES("--max-messages"),
    GROUP_ID("--group-id"),
    GROUP_INSTANCE_ID("--group-instance-id"),
    SESSION_TIMEOUT("--session-timeout"),
    ENABLE_AUTOCOMMIT("--enable-autocommit"),
    RESET_POLICY("--reset-policy"),
    ASSIGMENT_STRATEGY("--assignment-strategy"),

    // Producer
    BOOTSTRAP_SERVER("--bootstrap-server"),
    BROKER_LIST("--broker-list"),
    PRODUCER_CONFIG("--producer.config"),
    ACKS("--acks"),
    TIMEOUT("--timeout"),
    THROUGHPUT("--throughput"),
    MESSAGE_CREATE_TIME("--message-create-time"),
    VALUE_PREFIX("--value-prefix"),
    REPEATING_KEYS("--repeating-keys"),

    // Special argument for verifiable consumer in case of tls is needed
    USER("USER");

    private String command;

    ClientArgument(String command) {
        this.command = command;
    }

    /**
     * Gets command for external webClient
     *
     * @return string command
     */
    public String command() {
        return command;
    }
}
