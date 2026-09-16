package org.johnpaulkh.kafkamqtt.web

import org.johnpaulkh.kafkamqtt.dto.ErrorDto
import org.johnpaulkh.kafkamqtt.exception.ErrorCode
import org.johnpaulkh.kafkamqtt.exception.ServiceException
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler

@ControllerAdvice
class ConnectorControllerAdvice {
    @ExceptionHandler(ServiceException::class)
    fun handleServiceException(ex: ServiceException): ResponseEntity<ErrorDto> =
        ResponseEntity
            .status(ex.errorCode.httpStatus?.value() ?: HttpStatus.UNPROCESSABLE_ENTITY.value())
            .body(
                ErrorDto(
                    message = ex.message ?: ex.errorCode.message,
                    code = ex.errorCode.code,
                ),
            )

    @ExceptionHandler
    fun handleException(ex: Exception): ResponseEntity<ErrorDto> =
        ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR.value())
            .body(
                ErrorDto(
                    message = ex.message ?: "Unknown error",
                    code = ErrorCode.UNSPECIFIED.code,
                ),
            )
}
