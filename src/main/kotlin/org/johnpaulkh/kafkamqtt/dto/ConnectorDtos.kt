package org.johnpaulkh.kafkamqtt.dto

import org.johnpaulkh.kafkamqtt.entity.Connector
import org.johnpaulkh.kafkamqtt.entity.Connector.TopicDescriptor

data class ConnectorCreateRequest(
    val name: String,
    val descriptor: TopicDescriptor,
    val transformer: Map<String, String>? = null,
) {
    fun toEntity() =
        Connector(
            name = name,
            descriptor = descriptor,
            transformer = transformer,
        )
}

data class ConnectorUpdateRequest(
    val descriptor: TopicDescriptor? = null,
    val transformer: Map<String, String>? = null,
)
