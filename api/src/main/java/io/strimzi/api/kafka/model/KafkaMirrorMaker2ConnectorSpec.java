/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.KUBERNETES_API_BUILDER
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"tasksMax", "config"})
@EqualsAndHashCode(callSuper = true)
public class KafkaMirrorMaker2ConnectorSpec extends AbstractConnectorSpec {
    private static final long serialVersionUID = 1L;
}
