package org.johnpaulkh.kafkamqtt.processor

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.mockk.junit5.MockKExtension
import org.johnpaulkh.kafkamqtt.entity.Connector
import org.johnpaulkh.kafkamqtt.utils.next
import org.johnpaulkh.kafkamqtt.utils.random
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(MockKExtension::class)
class ConnectorProcessorTest {
    private val factory = ConnectorProcessorFactory()
    private val objectMapper = jacksonObjectMapper()

    private val message =
        """
        {
          "orderId": "ORD-20260915-3012",
          "customerId": "CUST-88319",
          "createdAt": "2026-09-15T15:42:10Z",
          "products": [
            {
              "productId": "PROD-602",
              "productName": "Smart Monitor 27 Inch 4K",
              "price": 4800000,
              "quantity": 1
            },
            {
              "productId": "PROD-109",
              "productName": "HD Webcam 1080p with Mic",
              "price": 650000,
              "quantity": 1
            }
          ],
          "payment": {
            "paymentMethod": "BRI_VIRTUAL_ACCOUNT",
            "paymentNumber": "262150883190012",
            "amountPaid": 5450000,
            "currency": "IDR",
            "status": "COMPLETED"
          }
        }
        """.trimIndent()

    @Nested
    inner class GetMqttTopic {
        @Test
        fun `given customers-$customerId-orders should return list of $customerId`() {
            // Given
            val mqttTopic = "/customers/\$customerId/orders"
            val connector =
                Connector(
                    name = random.next(),
                    descriptor =
                        Connector.TopicDescriptor(
                            kafkaTopic = "order.created",
                            mqttTopic = mqttTopic,
                        ),
                )
            val processor = factory.createProcessor(connector, objectMapper)

            // When
            val result = processor.getMqttTopic(message)

            // Then
            assertEquals("/customers/CUST-88319/orders", result)
        }
    }
}
