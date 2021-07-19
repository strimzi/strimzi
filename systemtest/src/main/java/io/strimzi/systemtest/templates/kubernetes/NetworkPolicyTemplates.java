/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.kubernetes;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.enums.DefaultNetworkPolicy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NetworkPolicyTemplates {

    private static final Logger LOGGER = LogManager.getLogger(NetworkPolicyTemplates.class);

    public static NetworkPolicyBuilder networkPolicyBuilder(String namespace, String name, LabelSelector labelSelector) {
        return new NetworkPolicyBuilder()
            .withApiVersion("networking.k8s.io/v1")
                .withKind(Constants.NETWORK_POLICY)
                    .withNewMetadata()
                        .withName(name + "-allow")
                        .withNamespace(namespace)
                    .endMetadata()
                    .withNewSpec()
                        .addNewIngress()
                            .addNewFrom()
                                .withPodSelector(labelSelector)
                            .endFrom()
                        .endIngress()
                        .withPolicyTypes("Ingress")
                    .endSpec();
    }

    public static NetworkPolicy applyDefaultNetworkPolicy(String namespace, DefaultNetworkPolicy policy) {
        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
            .withApiVersion("networking.k8s.io/v1")
            .withKind(Constants.NETWORK_POLICY)
            .withNewMetadata()
                .withName("global-network-policy")
                .withNamespace(namespace)
            .endMetadata()
            .withNewSpec()
                .withNewPodSelector()
                .endPodSelector()
                .withPolicyTypes("Ingress")
            .endSpec()
            .build();

        if (policy.equals(DefaultNetworkPolicy.DEFAULT_TO_ALLOW)) {
            networkPolicy = new NetworkPolicyBuilder(networkPolicy)
                .editSpec()
                    .addNewIngress()
                    .endIngress()
                .endSpec()
                .build();
        }

        LOGGER.debug("Going to apply the following NetworkPolicy: {}", networkPolicy.toString());

        return networkPolicy;
    }

}
