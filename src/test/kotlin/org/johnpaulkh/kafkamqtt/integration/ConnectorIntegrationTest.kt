@file:Suppress("SpringJavaInjectionPointsAutowiringInspection")

package org.johnpaulkh.kafkamqtt.integration

import com.fasterxml.jackson.databind.ObjectMapper
import org.johnpaulkh.kafkamqtt.dto.ConnectorCreateRequest
import org.johnpaulkh.kafkamqtt.dto.ConnectorUpdateRequest
import org.johnpaulkh.kafkamqtt.entity.Connector
import org.johnpaulkh.kafkamqtt.entity.ConnectorRepository
import org.johnpaulkh.kafkamqtt.utils.next
import org.johnpaulkh.kafkamqtt.utils.random
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.delete
import org.springframework.test.web.servlet.patch
import org.springframework.test.web.servlet.post

@ServiceIntegrationTest
class ConnectorIntegrationTest {
    @Autowired
    private lateinit var client: MockMvc

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @Autowired
    private lateinit var connectorRepository: ConnectorRepository

    @Nested
    inner class RegisterConnector {
        @Test
        fun `given a new connector registered should be persisted in mongodb`() {
            // Given
            val connector =
                ConnectorCreateRequest(
                    name = "connector-test-001",
                    descriptor =
                        Connector.TopicDescriptor(
                            kafkaTopic = "order.created",
                            mqttTopic = "customers/\$customerId/orders",
                        ),
                )
            val request = objectMapper.writeValueAsString(connector)

            // When
            val post =
                client.post("/api/v1/connectors") {
                    contentType = MediaType.APPLICATION_JSON
                    content = request
                }

            // Then
            post.andExpect {
                status { isOk() }
                jsonPath("$.name") { value("connector-test-001") }
                jsonPath("$.descriptor.kafkaTopic") { value("order.created") }
                jsonPath("$.descriptor.mqttTopic") { value("customers/\$customerId/orders") }
            }
        }
    }

    @Nested
    inner class DeleteConnector {
        @Test
        fun `given a connector exists should be deleted`() {
            // Given
            val connector =
                random.next<Connector>()
                    .copy(id = null)
                    .let {
                        connectorRepository.save(it)
                    }

            // When
            val delete = client.delete("/api/v1/connectors/${connector.id}")

            // Then
            delete.andExpect { status { isOk() } }
            assertEquals(0, connectorRepository.count())
        }
    }

    @Nested
    inner class UpdateConnector {
        @Test
        fun `given a connector exists should be updated`() {
            // Given
            val connector =
                random.next<Connector>()
                    .copy(id = null)
                    .let {
                        connectorRepository.save(it)
                    }
            val request =
                ConnectorUpdateRequest(
                    descriptor =
                        Connector.TopicDescriptor(
                            kafkaTopic = "order.created",
                            mqttTopic = "customers/\$customerId/orders",
                        ),
                )

            // When
            val patch =
                client.patch("/api/v1/connectors/${connector.id}") {
                    contentType = MediaType.APPLICATION_JSON
                    content = objectMapper.writeValueAsString(request)
                }

            // Then
            patch
                .andExpect {
                    status { isOk() }
                    jsonPath("$.descriptor.kafkaTopic") { value("order.created") }
                    jsonPath("$.descriptor.mqttTopic") { value("customers/\$customerId/orders") }
                }
        }

        @Test
        fun `given a connector does not should return 404`() {
            // Given
            val request =
                ConnectorUpdateRequest(
                    descriptor =
                        Connector.TopicDescriptor(
                            kafkaTopic = "order.created",
                            mqttTopic = "customers/\$customerId/orders",
                        ),
                )

            // When
            val patch =
                client.patch("/api/v1/connectors/NOT_EXISTS") {
                    contentType = MediaType.APPLICATION_JSON
                    content = objectMapper.writeValueAsString(request)
                }

            // Then
            patch
                .andExpect {
                    status { isNotFound() }
                }
        }
    }
}
