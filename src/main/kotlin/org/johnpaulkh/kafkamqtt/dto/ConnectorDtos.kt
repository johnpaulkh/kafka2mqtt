package org.johnpaulkh.kafkamqtt.dto

import org.johnpaulkh.kafkamqtt.entity.Connector
import org.johnpaulkh.kafkamqtt.entity.Connector.TopicDescriptor

data class ConnectorCreateRequest(
    val name: String,
    val descriptor: TopicDescriptor,
) {
    fun toEntity() =
        Connector(
            name = name,
            descriptor = descriptor,
        )
}

data class ConnectorUpdateRequest(
    val descriptor: TopicDescriptor,
)
