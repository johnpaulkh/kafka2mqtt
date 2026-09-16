package org.johnpaulkh.kafkamqtt.web

import org.johnpaulkh.kafkamqtt.dto.ConnectorCreateRequest
import org.johnpaulkh.kafkamqtt.dto.ConnectorUpdateRequest
import org.johnpaulkh.kafkamqtt.entity.Connector
import org.johnpaulkh.kafkamqtt.service.ConnectorService
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("api/v1/connectors")
class ConnectorController(
    private val connectorService: ConnectorService,
) {
    @PostMapping
    fun create(
        @RequestBody request: ConnectorCreateRequest,
    ) = connectorService.create(request)

    @PatchMapping("/{id}")
    fun update(
        @PathVariable id: String,
        @RequestBody request: ConnectorUpdateRequest,
    ) = connectorService.update(id, request)

    @GetMapping
    fun list(): List<Connector> = connectorService.list()

    @DeleteMapping("/{id}")
    fun delete(
        @PathVariable id: String,
    ) = connectorService.delete(id)
}
