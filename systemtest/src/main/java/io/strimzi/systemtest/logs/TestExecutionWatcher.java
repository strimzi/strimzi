/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.logs;

import io.strimzi.systemtest.Environment;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.LifecycleMethodExecutionExceptionHandler;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.opentest4j.TestAbortedException;

import java.io.IOException;
import java.util.Random;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class TestExecutionWatcher implements TestExecutionExceptionHandler, LifecycleMethodExecutionExceptionHandler {

    @Override
    public void handleTestExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        if (!(throwable instanceof TestAbortedException)) {
            final String testClass = extensionContext.getRequiredTestClass().getName();
            final String testMethod = extensionContext.getRequiredTestMethod().getName();
            collectLogs(testClass, testMethod);
        }
        throw throwable;
    }

    @Override
    public void handleBeforeAllMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        if (!(throwable instanceof TestAbortedException)) {
            String testClass = extensionContext.getRequiredTestClass().getName();
            collectLogs(testClass, testClass);
        }
        throw throwable;
    }

    @Override
    public void handleBeforeEachMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        if (!(throwable instanceof TestAbortedException)) {
            final String testClass = extensionContext.getRequiredTestClass().getName();
            final String testMethod = extensionContext.getRequiredTestMethod().getName();
            collectLogs(testClass, testMethod);
        }
        throw throwable;
    }

    @Override
    public void handleAfterEachMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        final String testClass = extensionContext.getRequiredTestClass().getName();
        final String testMethod = extensionContext.getRequiredTestMethod().getName();
        collectLogs(testClass, testMethod);
        throw throwable;
    }

    @Override
    public void handleAfterAllMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        final String testClass = extensionContext.getRequiredTestClass().getName();
        collectLogs(testClass, "");
        throw throwable;
    }

    public synchronized static void collectLogs(String testClass, String testMethod) throws IOException {
        // Stop test execution time counter in case of failures
        TimeMeasuringSystem.getInstance().stopOperation(Operation.TEST_EXECUTION, testClass, testMethod);

        testMethod = testMethod.isEmpty() ? "class-context-" + new Random().nextInt(Integer.MAX_VALUE) : testMethod;
        final LogCollector logCollector = new LogCollector(testClass, testMethod, kubeClient(), Environment.TEST_LOG_DIR);
        // collecting logs for all resources inside Kubernetes cluster
        logCollector.collect();
    }
}
