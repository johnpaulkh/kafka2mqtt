package org.johnpaulkh.kafkamqtt.service

import org.johnpaulkh.kafkamqtt.dto.ConnectorCreateRequest
import org.johnpaulkh.kafkamqtt.dto.ConnectorUpdateRequest
import org.johnpaulkh.kafkamqtt.entity.Connector
import org.johnpaulkh.kafkamqtt.entity.ConnectorRepository
import org.johnpaulkh.kafkamqtt.exception.ErrorCode
import org.johnpaulkh.kafkamqtt.exception.ServiceException
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service

@Service
class ConnectorService(
    private val connectorRepository: ConnectorRepository,
    private val connectorRegistrationService: ConnectorRegistrationService,
) {
    fun create(request: ConnectorCreateRequest): Connector =
        request
            .toEntity()
            .let { connectorRepository.save(it) }
            .also {
                connectorRegistrationService.register(it)
            }

    fun list(): List<Connector> = connectorRepository.findAll()

    fun delete(id: String) = connectorRepository.deleteById(id)

    fun update(
        id: String,
        request: ConnectorUpdateRequest,
    ): Connector =
        connectorRepository.findByIdOrNull(id)
            .let {
                it ?: throw ServiceException(errorCode = ErrorCode.CONNECTOR_NOT_FOUND)
            }
            .let { connector ->
                connector.copy(
                    descriptor = request.descriptor ?: connector.descriptor,
                    transformer = request.transformer ?: request.transformer,
                )
            }
            .let { connectorRepository.save(it) }
            .also { connectorRegistrationService.register(it) }!!
}
