package org.johnpaulkh.kafkamqtt.service

import org.johnpaulkh.kafkamqtt.entity.Connector
import org.springframework.stereotype.Service

@Service
class ConnectorRegistrationService(
    private val kafkaService: KafkaService,
    private val kafkaToMqttService: KafkaToMqttService,
) {
    fun register(connector: Connector) {
        kafkaService.startListenerForConnector(connector)
        kafkaToMqttService.registerProcessor(connector)
    }
}
