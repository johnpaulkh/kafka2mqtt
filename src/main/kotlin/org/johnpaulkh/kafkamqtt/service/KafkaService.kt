@file:Suppress("SpringJavaInjectionPointsAutowiringInspection")

package org.johnpaulkh.kafkamqtt.service

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.johnpaulkh.kafkamqtt.entity.Connector
import org.springframework.kafka.config.KafkaListenerContainerFactory
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.MessageListener
import org.springframework.stereotype.Service

@Service
class KafkaService(
    private val containerFactory: KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>>,
    private val kafkaToMqttService: KafkaToMqttService,
) {
    private val log = KotlinLogging.logger {}
    private val runningContainers = mutableMapOf<String, ConcurrentMessageListenerContainer<String, String>>()

    fun startListenerForConnector(connector: Connector) {
        val kafkaTopic = connector.descriptor.kafkaTopic
        val containerKey = connector.id ?: connector.name

        if (runningContainers.containsKey(containerKey)) {
            log.warn { "Listener container for connector [{}] is already running. ${connector.name}" }
            return
        }

        val container =
            containerFactory.createContainer(kafkaTopic).apply {
                containerProperties.apply {
                    groupId = "kafka-mqtt-connector-${connector.name}"
                    messageListener =
                        MessageListener<String, String> { record: ConsumerRecord<String, String> ->
                            handleKafkaMessage(kafkaTopic, record)
                        }
                }
                beanName = "kafka-container-${connector.name}"
                start()
            }

        runningContainers[containerKey] = container
        log.info { "Successfully started Kafka listener for topic [{}] (Connector: {}) $kafkaTopic ${connector.name}" }
    }

    private fun handleKafkaMessage(
        topic: String,
        record: ConsumerRecord<String, String>,
    ) {
        kafkaToMqttService.process(topic, record.value())
    }

    fun stopListenerForConnector(connectorId: String) {
        runningContainers.remove(connectorId)?.apply {
            stop()
            log.info { "Stopped Kafka listener container for connector ID [{}] $connectorId" }
        }
    }
}
