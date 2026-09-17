package org.johnpaulkh.kafkamqtt.processor

interface ConnectorProcessor {
    fun getMqttTopic(message: String): String

    fun transformMessage(message: String): String
}
