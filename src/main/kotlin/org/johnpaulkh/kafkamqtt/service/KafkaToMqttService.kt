package org.johnpaulkh.kafkamqtt.service

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import org.johnpaulkh.kafkamqtt.entity.Connector
import org.johnpaulkh.kafkamqtt.processor.ConnectorProcessor
import org.johnpaulkh.kafkamqtt.processor.ConnectorProcessorFactory
import org.springframework.stereotype.Service

@Service
class KafkaToMqttService(
    private val connectorProcessorFactory: ConnectorProcessorFactory,
    private val objectMapper: ObjectMapper,
    private val mqttService: MqttService,
) {
    private val log = KotlinLogging.logger {}
    private val processorMapByTopic = mutableMapOf<String, ConnectorProcessor>()

    fun registerProcessor(connector: Connector) {
        processorMapByTopic[connector.descriptor.kafkaTopic] =
            connectorProcessorFactory.createProcessor(connector, objectMapper)
    }

    fun process(
        topic: String,
        kafkaEventBody: String,
    ) {
        val processor = processorMapByTopic[topic]
        if (processor == null) {
            log.warn { "processor $topic does not exist." }
            return
        }
        val mqttTopic = processor.getMqttTopic(kafkaEventBody)
        log.info { "mqtt message for kafka topic $topic and mqtt topic: $mqttTopic" }
        mqttService.publish(mqttTopic, kafkaEventBody)
    }
}
