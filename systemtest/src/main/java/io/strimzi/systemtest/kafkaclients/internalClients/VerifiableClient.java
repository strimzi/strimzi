/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.strimzi.test.executor.Exec;

/**
 * Class represent verifiable kafka client which keeps common features of kafka clients
 */
public class VerifiableClient {

    private static final Logger LOGGER = LogManager.getLogger(VerifiableClient.class);
    private List<ClientArgument> allowedArguments = new ArrayList<>();
    private final Object lock = new Object();
    private List<String> messages = new ArrayList<>();
    private List<String> arguments = new ArrayList<>();
    private String executable;
    private Exec executor;

    private ClientType clientType;
    private String podName;
    private String podNamespace;
    private String bootstrapServer;
    private String topicName;
    private int maxMessages;
    private String kafkaUsername;
    private String consumerGroupName;
    private String consumerInstanceId;
    private ClientArgumentMap clientArgumentMap;

    public static class VerifiableClientBuilder {
        private ClientType clientType;
        private String podName;
        private String podNamespace;
        private String bootstrapServer;
        private String topicName;
        private int maxMessages;
        private String kafkaUsername;
        private String consumerGroupName;
        private String consumerInstanceId;

        public VerifiableClientBuilder withClientType(ClientType clientType) {
            this.clientType = clientType;
            return this;
        }

        public VerifiableClientBuilder withUsingPodName(String podName) {
            this.podName = podName;
            return this;
        }

        public VerifiableClientBuilder withPodNamespace(String podNamespace) {
            this.podNamespace = podNamespace;
            return this;
        }

        public VerifiableClientBuilder withBootstrapServer(String bootstrapServer) {
            this.bootstrapServer = bootstrapServer;
            return this;
        }

        public VerifiableClientBuilder withTopicName(String topicName) {
            this.topicName = topicName;
            return this;
        }

        public VerifiableClientBuilder withMaxMessages(int maxMessages) {
            this.maxMessages = maxMessages;
            return this;
        }

        public VerifiableClientBuilder withKafkaUsername(String kafkaUsername) {
            this.kafkaUsername = kafkaUsername;
            return this;
        }

        public VerifiableClientBuilder withConsumerGroupName(String consumerGroupName) {
            this.consumerGroupName = consumerGroupName;
            return this;
        }

        public VerifiableClientBuilder withConsumerInstanceId(String consumerInstanceId) {
            this.consumerInstanceId = consumerInstanceId;
            return this;
        }

        protected VerifiableClient build() {
            return new VerifiableClient(this);
        }
    }

    public VerifiableClient(VerifiableClientBuilder verifiableClientBuilder) {
        this.clientType = verifiableClientBuilder.clientType;
        this.podName = verifiableClientBuilder.podName;
        this.podNamespace = verifiableClientBuilder.podNamespace;
        this.bootstrapServer = verifiableClientBuilder.bootstrapServer;
        this.topicName = verifiableClientBuilder.topicName;
        this.maxMessages = verifiableClientBuilder.maxMessages;
        this.kafkaUsername = verifiableClientBuilder.kafkaUsername;

        this.setAllowedArguments(this.clientType);
        this.clientArgumentMap = new ClientArgumentMap();
        this.clientArgumentMap.put(ClientArgument.TOPIC, topicName);
        this.clientArgumentMap.put(ClientArgument.MAX_MESSAGES, Integer.toString(maxMessages));
        if (kafkaUsername != null) this.clientArgumentMap.put(ClientArgument.USER, kafkaUsername.replace("-", "_"));

        String image = kubeClient().namespace(podNamespace).getPod(podNamespace, this.podName).getSpec().getContainers().get(0).getImage();
        String clientVersion = image.substring(image.length() - 5);

        this.clientArgumentMap.put(allowParameter("2.5.0", clientVersion) ? ClientArgument.BOOTSTRAP_SERVER : ClientArgument.BROKER_LIST, bootstrapServer);

        if (clientType == ClientType.CLI_KAFKA_VERIFIABLE_CONSUMER) {
            this.consumerGroupName = verifiableClientBuilder.consumerGroupName;
            this.clientArgumentMap.put(ClientArgument.GROUP_ID, consumerGroupName);

            if (allowParameter("2.3.0", clientVersion)) {
                this.consumerInstanceId = verifiableClientBuilder.consumerInstanceId;
                this.clientArgumentMap.put(ClientArgument.GROUP_INSTANCE_ID, this.consumerInstanceId);
            }
        }

        if (clientType == ClientType.CLI_KAFKA_CONSUMER_GROUPS) {
            this.consumerGroupName = verifiableClientBuilder.consumerGroupName;
            this.clientArgumentMap.put(ClientArgument.GROUP, consumerGroupName);
            this.clientArgumentMap.put(ClientArgument.DESCRIBE, "");
        }

        LOGGER.debug("This is all args {}, which are set.", this.clientArgumentMap);

        this.setArguments(this.clientArgumentMap);
        this.executable = ClientType.getCommand(clientType);
    }

    /**
     * Get of messages
     *
     * @return Json array of messages;
     */
    public List<String> getMessages() {
        return messages;
    }

    /**
     * Set arguments of kafka client
     *
     * @param args string array of arguments
     */
    public void setArguments(ClientArgumentMap args) {
        arguments.clear();
        String argument;
        for (ClientArgument arg : args.getArguments()) {
            if (validateArgument(arg)) {
                for (String value : args.getValues(arg)) {
                    if (arg.equals(ClientArgument.USER)) {
                        argument = String.format("%s=%s", arg.command(), value);
                        arguments.add(argument);
                    } else {
                        arguments.add(arg.command());
                        if (!value.isEmpty()) {
                            arguments.add(value);
                        }
                    }
                }
            } else {
                LOGGER.warn(String.format("Argument '%s' is not allowed for '%s'",
                        arg.command(),
                        this.getClass().getSimpleName()));
            }
        }
    }

    /**
     * Run clients
     *
     * @param timeoutMs kill timeout in ms
     * @return true if command end with exit code 0
     */
    private boolean runClient(long timeoutMs, boolean logToOutput) {
        messages.clear();
        try {
            executor = new Exec();
            ArrayList<String> command = prepareCommand();
            LOGGER.info("Client command: {}", String.join(" ", command));
            int ret = executor.execute(null, command, timeoutMs);
            synchronized (lock) {
                if (logToOutput) {
                    if (ret == 0) {
                        parseToList(executor.out());
                    } else {
                        LOGGER.info("{} RETURN code: {}", clientType,  ret);
                        if (!executor.out().isEmpty()) {
                            LOGGER.info("======STDOUT START=======");
                            LOGGER.info("{}", Exec.cutExecutorLog(executor.out()));
                            LOGGER.info("======STDOUT END======");
                        }
                        if (!executor.err().isEmpty()) {
                            LOGGER.info("======STDERR START=======");
                            LOGGER.info("{}", Exec.cutExecutorLog(executor.err()));
                            LOGGER.info("======STDERR END======");
                        }
                    }
                }
            }
            return ret == 0;
        } catch (InterruptedException | ExecutionException | IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Method for parse string output to List<string>
     *
     * @param data string data output
     */
    private void parseToList(String data) {
        if (data != null) {
            for (String line : data.split(System.getProperty("line.separator"))) {
                if (!Objects.equals(line, "") && !line.trim().isEmpty()) {
                    try {
                        messages.add(line);
                    } catch (Exception ignored) {
                        LOGGER.warn("{} - Failed to parse client output '{}' as JSON", clientType, line);
                    }
                }
            }
        }
    }

    /**
     * Merge command and arguments
     *
     * @return merged array of command and args
     */
    private ArrayList<String> prepareCommand() {
        ArrayList<String> command = new ArrayList<>(arguments);
        ArrayList<String> executableCommand = new ArrayList<>();
        executableCommand.addAll(Arrays.asList(cmdKubeClient().toString(), "exec", podName, "-n", podNamespace, "--"));
        executableCommand.add(executable);
        executableCommand.addAll(command);
        return executableCommand;
    }

    /**
     * Run client in sync mode
     *
     * @return exit status of client
     */
    public boolean run(long timeoutMs) {
        return runClient(timeoutMs, true);
    }

    /**
     * Validates that kafka client support this arg
     *
     * @param arg argument to validate
     * @return true if argument is supported
     */
    private boolean validateArgument(ClientArgument arg) {
        return this.allowedArguments.contains(arg);
    }

    /**
     * Set allowed args for verifiable clients
     * @param clientType client type
     */
    protected void setAllowedArguments(ClientType clientType) {
        switch (clientType) {
            case CLI_KAFKA_VERIFIABLE_PRODUCER:
                allowedArguments.add(ClientArgument.TOPIC);
                allowedArguments.add(ClientArgument.BOOTSTRAP_SERVER);
                allowedArguments.add(ClientArgument.BROKER_LIST);
                allowedArguments.add(ClientArgument.MAX_MESSAGES);
                allowedArguments.add(ClientArgument.THROUGHPUT);
                allowedArguments.add(ClientArgument.ACKS);
                allowedArguments.add(ClientArgument.PRODUCER_CONFIG);
                allowedArguments.add(ClientArgument.MESSAGE_CREATE_TIME);
                allowedArguments.add(ClientArgument.VALUE_PREFIX);
                allowedArguments.add(ClientArgument.REPEATING_KEYS);
                allowedArguments.add(ClientArgument.USER);
                break;
            case CLI_KAFKA_VERIFIABLE_CONSUMER:
                allowedArguments.add(ClientArgument.BOOTSTRAP_SERVER);
                allowedArguments.add(ClientArgument.BROKER_LIST);
                allowedArguments.add(ClientArgument.TOPIC);
                allowedArguments.add(ClientArgument.GROUP_ID);
                allowedArguments.add(ClientArgument.MAX_MESSAGES);
                allowedArguments.add(ClientArgument.SESSION_TIMEOUT);
                allowedArguments.add(ClientArgument.VERBOSE);
                allowedArguments.add(ClientArgument.ENABLE_AUTOCOMMIT);
                allowedArguments.add(ClientArgument.RESET_POLICY);
                allowedArguments.add(ClientArgument.ASSIGMENT_STRATEGY);
                allowedArguments.add(ClientArgument.CONSUMER_CONFIG);
                allowedArguments.add(ClientArgument.USER);
                allowedArguments.add(ClientArgument.GROUP_INSTANCE_ID);
                break;
            case CLI_KAFKA_CONSUMER_GROUPS:
                allowedArguments.add(ClientArgument.BOOTSTRAP_SERVER);
                allowedArguments.add(ClientArgument.GROUP);
                allowedArguments.add(ClientArgument.DESCRIBE);
                break;
            default:
                throw new IllegalArgumentException("Unexpected client type!");
        }
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    private boolean allowParameter(String minimalVersion, String clientVersion) {
        Pattern pattern = Pattern.compile("(?<major>[0-9]).(?<minor>[0-9]).(?<micro>[0-9])");
        Matcher current = pattern.matcher(clientVersion);
        Matcher minimal = pattern.matcher(minimalVersion);
        if (current.find() && minimal.find()) {
            return Integer.parseInt(current.group("major")) >= Integer.parseInt(minimal.group("major"))
                    && Integer.parseInt(current.group("minor")) >= Integer.parseInt(minimal.group("minor"))
                    && Integer.parseInt(current.group("micro")) >= Integer.parseInt(minimal.group("micro"));
        }
        return false;
    }

    @Override
    public String toString() {
        return "VerifiableClient{" +
            "allowedArguments=" + allowedArguments +
            ", lock=" + lock +
            ", messages=" + messages +
            ", arguments=" + arguments +
            ", executable='" + executable + '\'' +
            ", executor=" + executor +
            ", clientType=" + clientType +
            ", podName='" + podName + '\'' +
            ", podNamespace='" + podNamespace + '\'' +
            ", bootstrapServer='" + bootstrapServer + '\'' +
            ", topicName='" + topicName + '\'' +
            ", maxMessages=" + maxMessages +
            ", kafkaUsername='" + kafkaUsername + '\'' +
            ", consumerGroupName='" + consumerGroupName + '\'' +
            ", consumerInstanceId='" + consumerInstanceId + '\'' +
            ", clientArgumentMap=" + clientArgumentMap +
            '}';
    }
}
