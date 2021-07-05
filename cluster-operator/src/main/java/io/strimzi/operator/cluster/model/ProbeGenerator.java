/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.HTTPHeader;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.TlsSidecar;

import java.util.Arrays;
import java.util.List;

public class ProbeGenerator {

    private ProbeGenerator() { }

    /**
     * probeBuilder returns a ProbeBuilder pre-configured with the supplied properties
     *
     * @param probeConfig the initial config for the ProbeBuilder
     * @return ProbeBuilder
     */
    public static ProbeBuilder defaultBuilder(Probe probeConfig) {
        if (probeConfig == null) {
            throw new IllegalArgumentException();
        }


        ProbeBuilder pb =  new ProbeBuilder()
                .withTimeoutSeconds(probeConfig.getTimeoutSeconds())
                .withPeriodSeconds(probeConfig.getPeriodSeconds())
                .withSuccessThreshold(probeConfig.getSuccessThreshold())
                .withFailureThreshold(probeConfig.getFailureThreshold());

        if (probeConfig.getInitialDelaySeconds() > 0)   {
            pb = pb.withInitialDelaySeconds(probeConfig.getInitialDelaySeconds());
        }

        return pb;
    }

    public static io.fabric8.kubernetes.api.model.Probe httpProbe(Probe probeConfig, String path, String port) {
        if (path == null || path.isEmpty() || port == null || port.isEmpty()) {
            throw new IllegalArgumentException();
        }
        io.fabric8.kubernetes.api.model.Probe probe = defaultBuilder(probeConfig)
                .withNewHttpGet()
                    .withPath(path)
                    .withNewPort(port)
                .endHttpGet()
                .build();
        return probe;
    }

    protected static io.fabric8.kubernetes.api.model.Probe httpProbe(Probe probeConfig, String path, String port, String scheme, List<HTTPHeader> headers) {
        if (path == null || path.isEmpty() || port == null || port.isEmpty()) {
            throw new IllegalArgumentException();
        }
        io.fabric8.kubernetes.api.model.Probe probe = defaultBuilder(probeConfig)
                .withNewHttpGet()
                  .withPath(path)
                  .withNewPort(port)
                  .withHttpHeaders(headers)
                  .withScheme(scheme)
                .endHttpGet()
                .build();
        return probe;
    }

    public static io.fabric8.kubernetes.api.model.Probe execProbe(Probe probeConfig, List<String> command) {
        if (command == null || command.isEmpty()) {
            throw new IllegalArgumentException();
        }
        io.fabric8.kubernetes.api.model.Probe probe = defaultBuilder(probeConfig)
                .withNewExec()
                    .withCommand(command)
                .endExec()
                .build();
        return probe;
    }

    public static final io.strimzi.api.kafka.model.Probe DEFAULT_TLS_SIDECAR_PROBE = new io.strimzi.api.kafka.model.ProbeBuilder()
            .withInitialDelaySeconds(TlsSidecar.DEFAULT_HEALTHCHECK_DELAY)
            .withTimeoutSeconds(TlsSidecar.DEFAULT_HEALTHCHECK_TIMEOUT)
            .build();

    protected static io.fabric8.kubernetes.api.model.Probe tlsSidecarReadinessProbe(TlsSidecar tlsSidecar) {
        Probe tlsSidecarReadinessProbe = DEFAULT_TLS_SIDECAR_PROBE;
        if (tlsSidecar != null && tlsSidecar.getReadinessProbe() != null) {
            tlsSidecarReadinessProbe = tlsSidecar.getReadinessProbe();
        }
        return ProbeGenerator.execProbe(tlsSidecarReadinessProbe, Arrays.asList("/opt/stunnel/stunnel_healthcheck.sh", "2181"));
    }

    protected static io.fabric8.kubernetes.api.model.Probe tlsSidecarLivenessProbe(TlsSidecar tlsSidecar) {
        Probe tlsSidecarLivenessProbe = DEFAULT_TLS_SIDECAR_PROBE;
        if (tlsSidecar != null && tlsSidecar.getLivenessProbe() != null) {
            tlsSidecarLivenessProbe = tlsSidecar.getLivenessProbe();
        }
        return ProbeGenerator.execProbe(tlsSidecarLivenessProbe, Arrays.asList("/opt/stunnel/stunnel_healthcheck.sh", "2181"));
    }
}
