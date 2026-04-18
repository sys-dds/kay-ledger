package com.kayledger.api.shared.api;

public class InternalFailureException extends RuntimeException {

    public InternalFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}
