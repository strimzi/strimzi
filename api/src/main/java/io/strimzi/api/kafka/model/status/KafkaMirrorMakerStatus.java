/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Represents a status of the Kafka MirrorMaker resource
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.KUBERNETES_API_BUILDER
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"conditions", "observedGeneration"})
@EqualsAndHashCode
@ToString(callSuper = true)
public class KafkaMirrorMakerStatus extends Status {
    private static final long serialVersionUID = 1L;
}
