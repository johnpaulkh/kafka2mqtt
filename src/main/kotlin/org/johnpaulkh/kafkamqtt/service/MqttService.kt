package org.johnpaulkh.kafkamqtt.service

import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.springframework.stereotype.Service

@Service
class MqttService(
    private val mqttClient: MqttClient,
) {
    fun publish(
        topic: String,
        message: String,
        qos: Int = 1,
        retain: Boolean = false,
    ) {
        val mqttMessage =
            MqttMessage(message.toByteArray(Charsets.UTF_8)).apply {
                this.qos = qos
                this.isRetained = retain
            }
        mqttClient.publish(topic, mqttMessage)
    }
}
