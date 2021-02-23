/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.cli;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

public abstract class AbstractCommand implements Runnable {
    protected Logger log = LogManager.getLogger(getClass().getName());

    @CommandLine.Option(names = {"-d", "--debug"}, description = "Use debug?")
    boolean debug;

    @CommandLine.Option(names = {"-ll", "--log-level"}, description = "Set log level to enable logging")
    Level level;

    protected void println(Object value) {
        if (level != null) {
            log.log(level, String.valueOf(value));
        } else {
            System.out.println(value);
        }
    }
}
