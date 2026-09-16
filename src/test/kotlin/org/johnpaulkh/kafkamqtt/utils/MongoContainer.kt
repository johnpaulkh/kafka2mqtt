package org.johnpaulkh.kafkamqtt.utils

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.boot.test.util.TestPropertyValues
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.testcontainers.containers.MongoDBContainer

private val logger = KotlinLogging.logger {}

class MongoContainer {
    companion object {
        val mongoContainer: MongoDBContainer = MongoDBContainer("mongo:5.0.2").withReuse(true)
    }

    internal class Initializer : ApplicationContextInitializer<ConfigurableApplicationContext> {
        override fun initialize(configurableApplicationContext: ConfigurableApplicationContext) {
            mongoContainer.start()
            logger.info { "Mongo started at ${mongoContainer.replicaSetUrl}" }

            TestPropertyValues
                .of(
                    "spring.data.mongodb.uri=${mongoContainer.replicaSetUrl}",
                    "spring.data.mongodb.auto-index-creation=true",
                ).applyTo(configurableApplicationContext.environment)
        }
    }
}
