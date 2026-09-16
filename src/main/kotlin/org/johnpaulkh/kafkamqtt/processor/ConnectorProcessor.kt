package org.johnpaulkh.kafkamqtt.processor

import com.fasterxml.jackson.databind.ObjectMapper

interface ConnectorProcessor {
    val objectMapper: ObjectMapper

    fun getMqttTopic(message: String): String
}
