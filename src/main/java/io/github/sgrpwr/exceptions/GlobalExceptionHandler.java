package io.github.sgrpwr.exceptions;

import io.github.sgrpwr.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(CustomKafkaException.class)
    public ResponseEntity<String> handleCustomKafkaException(CustomKafkaException e) {
        switch (e.getErrorType()) {
            case TEMPLATE_NULL:
                logger.error(Constants.TEMPLATE_NOT_FOUNT + ": ", e);
                return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
            case TOPIC_NOT_FOUND:
                logger.error(Constants.TOPIC_NOT_FOUNT + ": ", e);
                return new ResponseEntity<>(e.getMessage(), HttpStatus.NOT_FOUND);
            case MESSAGE_EMPTY:
                logger.error(Constants.REQUEST_NOT_FOUNT + ": ", e);
                return new ResponseEntity<>(e.getMessage(), HttpStatus.BAD_REQUEST);
            default:
                logger.error("Unknown CustomKafkaException: ", e);
                return new ResponseEntity<>("Internal Server Error", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

}
