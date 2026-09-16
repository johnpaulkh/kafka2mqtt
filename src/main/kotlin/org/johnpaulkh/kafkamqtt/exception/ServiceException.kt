package org.johnpaulkh.kafkamqtt.exception

import org.springframework.http.HttpStatus

class ServiceException(
    val errorCode: ErrorCode,
    message: String? = null,
) : RuntimeException(
        when (message) {
            null -> errorCode.message
            else -> "${errorCode.message}: $message"
        },
    )

enum class ErrorCode(
    val code: String,
    val message: String,
    val httpStatus: HttpStatus? = null,
) {
    UNSPECIFIED("UNSPECIFIED", "Unspecified error"),
    CONNECTOR_NOT_FOUND("KMC-001", "connection not found", HttpStatus.NOT_FOUND),
}
