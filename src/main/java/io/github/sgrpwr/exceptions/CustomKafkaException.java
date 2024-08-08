package io.github.sgrpwr.exceptions;

public class CustomKafkaException extends RuntimeException {
    private final ErrorType errorType;

    public enum ErrorType {
        TEMPLATE_NULL,
        TOPIC_NOT_FOUND,
        MESSAGE_EMPTY
    }

    public CustomKafkaException(String message, ErrorType errorType) {
        super(message);
        this.errorType = errorType;
    }

    public ErrorType getErrorType() {
        return errorType;
    }
}
