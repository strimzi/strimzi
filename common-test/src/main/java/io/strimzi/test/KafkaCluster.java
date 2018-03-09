/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for test classes or methods run via {@code @RunWith(StrimziRunner.class)}
 * which causes that runner to create kafka clusters before, and delete kafka clusters after,
 * the tests.
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(KafkaCluster.Container.class)
public @interface KafkaCluster {

    String name();
    int kafkaNodes() default 3;
    int zkNodes() default 1;

    @Target({ElementType.METHOD, ElementType.TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @interface Container {
        KafkaCluster[] value();
    }
}
