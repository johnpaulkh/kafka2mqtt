package org.johnpaulkh.kafkamqtt.service

import io.github.oshai.kotlinlogging.KotlinLogging
import org.johnpaulkh.kafkamqtt.entity.Connector
import org.johnpaulkh.kafkamqtt.entity.ConnectorRepository
import org.springframework.boot.context.event.ApplicationStartedEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Service

@Service
class ConnectorLoaderService(
    private val connectorRepository: ConnectorRepository,
    private val kafkaService: KafkaService,
    private val kafkaToMqttService: KafkaToMqttService,
) {
    private val log = KotlinLogging.logger {}

    @EventListener(ApplicationStartedEvent::class)
    fun onApplicationReady() {
        log.info { "Fetching connectors from MongoDB to initialize Kafka listeners..." }
        val connectors = connectorRepository.findAll()

        if (connectors.isEmpty()) {
            log.info { "No connector definitions found in database." }
            return
        }

        connectors.forEach { connector ->
            register(connector)
        }
    }

    fun register(connector: Connector) {
        kafkaService.startListenerForConnector(connector)
        kafkaToMqttService.registerProcessor(connector)
    }
}
