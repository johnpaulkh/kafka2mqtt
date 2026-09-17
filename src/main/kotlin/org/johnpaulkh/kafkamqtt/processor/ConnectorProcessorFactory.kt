package org.johnpaulkh.kafkamqtt.processor

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.jayway.jsonpath.JsonPath
import io.github.oshai.kotlinlogging.KotlinLogging
import org.johnpaulkh.kafkamqtt.entity.Connector
import org.springframework.stereotype.Service

@Service
class ConnectorProcessorFactory {
    fun createProcessor(
        connector: Connector,
        objectMapper: ObjectMapper,
    ) = object : ConnectorProcessor {
        override val objectMapper = objectMapper

        val log = KotlinLogging.logger {}
        val mqttTopicVariables = extractTokens(connector.descriptor.mqttTopic)
        val mqttTopicTemplate = connector.descriptor.mqttTopic
        val transformer = connector.transformer

        override fun getMqttTopic(message: String): String {
            val rootNode: JsonNode = objectMapper.readTree(message)
            val variableMap =
                mqttTopicVariables.associateWith { token ->
                    val fieldName = token.replace("$", "")
                    val targetNode = rootNode.get(fieldName)
                    when {
                        targetNode.isValueNode -> targetNode.asText()
                        else -> targetNode.toString()
                    }
                }
            return mqttTopicTemplate.resolveTemplate(variableMap)
        }

        override fun transformMessage(message: String): String {
            val documentContext = JsonPath.parse(message)
            return transformer
                .let { it ?: return message }
                .entries
                .fold(mutableMapOf<String, Any?>()) { resultMap, entry ->
                    val path = entry.value
                    val targetKey = entry.key
                    val extractedValue = runCatching { documentContext.read(path) as Any? }.getOrElse {
                        log.warn { "Fail to parse $path of message $message" }
                        null
                    }
                    resultMap[targetKey] = extractedValue
                    resultMap
                }
                .let { objectMapper.writeValueAsString(it) }
        }

        fun String.resolveTemplate(values: Map<String, String>): String {
            val regex = Regex("""\$[a-zA-Z0-9_]+""")

            return regex.replace(this) { matchResult ->
                val key = matchResult.value
                values[key] ?: key
            }
        }

        fun extractTokens(descriptor: String): List<String> {
            val regex = Regex("""\$[a-zA-Z0-9_]+""")
            return regex.findAll(descriptor).map { it.value }.toList()
        }
    }
}
