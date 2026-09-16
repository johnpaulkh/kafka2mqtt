package org.johnpaulkh.kafkamqtt.config

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.config.EnableMongoAuditing

@Configuration
@EnableMongoAuditing
class MongoConfig

@Configuration
class ObjectMapperConfig {
    @Bean
    fun objectMapper() =
        jacksonObjectMapper().apply {
            registerModule(JavaTimeModule())
        }
}

@Configuration
class MqttConfig(
    @param:Value("\${mqtt.broker-url:tcp://localhost:1883}") private val brokerUrl: String,
    @param:Value("\${mqtt.client-id:kafka-mqtt-publisher}") private val clientId: String,
) {
    private val log = KotlinLogging.logger {}

    @Bean
    fun mqttClient(): MqttClient {
        val uniqueClientId = "$clientId-${System.currentTimeMillis()}"
        val client = MqttClient(brokerUrl, uniqueClientId, MemoryPersistence())

        val options =
            MqttConnectOptions().apply {
                isCleanSession = true
                connectionTimeout = 10
                keepAliveInterval = 30
                isAutomaticReconnect = true
            }

        log.info { "Connecting to MQTT Broker at [{}] with Client ID [{}]... $brokerUrl $uniqueClientId" }
        client.connect(options)
        log.info { "Successfully connected to MQTT Broker." }

        return client
    }
}
