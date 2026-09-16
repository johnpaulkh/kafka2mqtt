package org.johnpaulkh.kafkamqtt.integration

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.johnpaulkh.kafkamqtt.dto.ConnectorCreateRequest
import org.johnpaulkh.kafkamqtt.entity.Connector
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.MediaType
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.post
import java.util.UUID
import java.util.concurrent.TimeUnit

@EmbeddedKafka(partitions = 2, topics = ["order.created"])
@ServiceIntegrationTest
class KafkaMqttIntegrationTest {
    @Autowired
    private lateinit var client: MockMvc

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @Value("\${mqtt.broker-url:tcp://localhost:1883}")
    private lateinit var brokerUrl: String

    private lateinit var testMqttClient: MqttClient

    private val kafkaTopic = "order.created"
    private val mqttTopic = "customers/\$customerId/orders"
    private val customerId = "CUST-001"
    private val kafkaEvent =
        """
        {
            "customerId": "$customerId",
            "status": "CREATED"
        }
        """.trimIndent()
    private val receivedPayloads = mutableListOf<String>()

    @BeforeEach
    fun setUp() {
        val uniqueClientId = "test-subscriber-${UUID.randomUUID()}"
        testMqttClient = MqttClient(brokerUrl, uniqueClientId, MemoryPersistence())

        val options =
            MqttConnectOptions().apply {
                isCleanSession = true
                connectionTimeout = 5
            }
        testMqttClient.connect(options)
    }

    @AfterEach
    fun tearDown() {
        if (::testMqttClient.isInitialized && testMqttClient.isConnected) {
            testMqttClient.disconnect()
            testMqttClient.close()
        }
    }

    @Nested
    inner class KafkaToMqtt {
        @Test
        fun `given a connector is registered when kafka event published should publish to mqtt`() {
            // Given
            connectorRegistered()
            mqttTopicSubscribed(customerId)

            // When
            kafkaTemplate.send(kafkaTopic, kafkaEvent)

            // Then
            await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted {
                    assertThat(receivedPayloads)
                        .isNotEmpty
                        .contains(kafkaEvent)
                }
        }
    }

    fun connectorRegistered() {
        val connector =
            ConnectorCreateRequest(
                name = "connector-test-001",
                descriptor =
                    Connector.TopicDescriptor(
                        kafkaTopic = kafkaTopic,
                        mqttTopic = mqttTopic,
                    ),
            )
        val request = objectMapper.writeValueAsString(connector)

        // When
        client.post("/api/v1/connectors") {
            contentType = MediaType.APPLICATION_JSON
            content = request
        }
    }

    fun mqttTopicSubscribed(customerId: String) {
        testMqttClient.subscribe("customers/$customerId/orders", 1) { _, message ->
            val payload = String(message.payload)
            receivedPayloads.add(payload)
        }
    }
}
