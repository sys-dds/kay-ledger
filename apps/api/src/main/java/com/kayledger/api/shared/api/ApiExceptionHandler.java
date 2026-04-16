package com.kayledger.api.shared.api;

import java.time.Instant;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(BadRequestException.class)
    ResponseEntity<ApiError> badRequest(BadRequestException exception) {
        return error(HttpStatus.BAD_REQUEST, exception.getMessage());
    }

    @ExceptionHandler(NotFoundException.class)
    ResponseEntity<ApiError> notFound(NotFoundException exception) {
        return error(HttpStatus.NOT_FOUND, exception.getMessage());
    }

    @ExceptionHandler(ForbiddenException.class)
    ResponseEntity<ApiError> forbidden(ForbiddenException exception) {
        return error(HttpStatus.FORBIDDEN, exception.getMessage());
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    ResponseEntity<ApiError> conflict(DataIntegrityViolationException exception) {
        return error(HttpStatus.CONFLICT, "Request conflicts with existing data or tenant boundaries.");
    }

    private static ResponseEntity<ApiError> error(HttpStatus status, String message) {
        return ResponseEntity.status(status).body(new ApiError(status.value(), message, Instant.now()));
    }

    public record ApiError(int status, String message, Instant timestamp) {
    }
}
