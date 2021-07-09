/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.matchers;

import io.fabric8.kubernetes.api.model.Event;
import io.strimzi.systemtest.k8s.Events;
import org.hamcrest.Matcher;

import java.util.List;

public class Matchers {

    private Matchers() {
    }

    /**
     * A matcher checks that examined object has a full match of reasons for actual events.
     * @param eventReasons - expected events for resource
     * @return The matcher.
     */
    public static Matcher<List<Event>> hasAllOfReasons(Events... eventReasons) {
        return new HasAllOfReasons(eventReasons);
    }

    /**
     * A matcher checks that log doesn't have unexpected errors
     * @return The matcher.
     */
    public static Matcher<String> logHasNoUnexpectedErrors() {
        return new LogHasNoUnexpectedErrors();
    }
}
