package org.johnpaulkh.kafkamqtt

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaMqttConnector

fun main(args: Array<String>) {
    runApplication<KafkaMqttConnector>(*args)
}
