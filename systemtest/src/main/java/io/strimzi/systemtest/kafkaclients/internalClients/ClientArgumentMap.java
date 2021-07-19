/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Class represents Map of arguments (allow duplicate argument)
 */
public class ClientArgumentMap {
    private final Map<ClientArgument, ArrayList<String>> mappings = new HashMap<>();

    /**
     * Returns set of values for argument
     *
     * @param arg argument
     * @return Set of values
     */
    public ArrayList<String> getValues(ClientArgument arg) {
        return mappings.get(arg);
    }

    /**
     * Returns set of arguments
     *
     * @return set of arguments
     */
    public Set<ClientArgument> getArguments() {
        return mappings.keySet();
    }

    /**
     * Add argument and his values
     *
     * @param key   arguments
     * @param value value
     * @return true if operation is completed
     */
    public Boolean put(ClientArgument key, String value) {
        ArrayList<String> target = mappings.computeIfAbsent(key, k -> new ArrayList<>());

        return target.add(value);
    }
}
