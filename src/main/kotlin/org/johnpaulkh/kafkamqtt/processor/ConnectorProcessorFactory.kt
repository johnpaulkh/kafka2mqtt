package org.johnpaulkh.kafkamqtt.processor

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.jayway.jsonpath.JsonPath
import io.github.oshai.kotlinlogging.KotlinLogging
import org.johnpaulkh.kafkamqtt.entity.Connector
import org.springframework.stereotype.Service

@Service
class ConnectorProcessorFactory {
    val log = KotlinLogging.logger {}

    companion object {
        private val PLACEHOLDER_REGEX = Regex("""\$[a-zA-Z0-9_]+""")
    }

    fun createProcessor(
        connector: Connector,
        objectMapper: ObjectMapper,
    ) = object : ConnectorProcessor {
        val getMqttTopicFunc = generateGetMqttTopic(connector, objectMapper)
        val transformMessageFunc = generateTransformMessageFunc(connector, objectMapper)

        override fun getMqttTopic(message: String): String = getMqttTopicFunc(message)

        override fun transformMessage(message: String): String = transformMessageFunc(message)
    }

    fun generateGetMqttTopic(
        connector: Connector,
        objectMapper: ObjectMapper,
    ): (message: String) -> String =
        fun(message: String): String {
            val mqttTopicTemplate = connector.descriptor.mqttTopic
            val mqttTopicVariables = extractTokens(connector.descriptor.mqttTopic)
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

    fun generateTransformMessageFunc(
        connector: Connector,
        objectMapper: ObjectMapper,
    ): (message: String) -> String =
        fun(message: String): String {
            val documentContext = JsonPath.parse(message)
            return connector.transformer
                .let { it ?: return message }
                .entries
                .fold(mutableMapOf<String, Any?>()) { resultMap, entry ->
                    val path = entry.value
                    val targetKey = entry.key
                    val extractedValue =
                        runCatching { documentContext.read(path) as Any? }.getOrElse {
                            log.warn { "Fail to parse $path of message $message" }
                            null
                        }
                    resultMap[targetKey] = extractedValue
                    resultMap
                }
                .let { objectMapper.writeValueAsString(it) }
        }

    fun String.resolveTemplate(values: Map<String, String>): String =
        PLACEHOLDER_REGEX.replace(this) { matchResult ->
            val key = matchResult.value
            values[key] ?: key
        }

    fun extractTokens(descriptor: String): List<String> = PLACEHOLDER_REGEX.findAll(descriptor).map { it.value }.toList()
}
