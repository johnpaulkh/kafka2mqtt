package org.johnpaulkh.kafkamqtt.entity

import org.springframework.data.annotation.CreatedDate
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.LastModifiedDate
import org.springframework.data.mongodb.core.index.Indexed
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.repository.MongoRepository
import java.time.Instant

@Document(collection = "connectors")
data class Connector(
    @Id
    val id: String? = null,
    @Indexed(unique = true)
    val name: String,
    val descriptor: TopicDescriptor,
    val transformer: Map<String, String>? = null,
    @CreatedDate
    val createdAt: Instant = Instant.now(),
    @LastModifiedDate
    val updatedAt: Instant = Instant.now(),
) {
    data class TopicDescriptor(
        val kafkaTopic: String,
        val mqttTopic: String,
    )
}

interface ConnectorRepository : MongoRepository<Connector, String> {
    fun findByName(name: String): Connector?
}
