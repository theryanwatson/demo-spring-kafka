package org.watson.demos.exceptions;

public class DoNotRetryException extends RuntimeException {
    public DoNotRetryException(String message) {
        super(message);
    }
}
