package org.johnpaulkh.kafkamqtt.integration

import org.johnpaulkh.kafkamqtt.utils.MongoContainer
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration

@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext
@EmbeddedKafka(
    partitions = 2,
    brokerProperties = [
        "offsets.topic.replication.factor=1",
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1",
    ],
)
@ContextConfiguration(initializers = [MongoContainer.Initializer::class])
@AutoConfigureMockMvc
annotation class ServiceIntegrationTest()
