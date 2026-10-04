package com.salesforce.einstein.tagging.api;

import com.salesforce.einstein.tagging.domain.DuplicateTagNameException;
import com.salesforce.einstein.tagging.domain.IdempotencyConflictException;
import com.salesforce.einstein.tagging.domain.InvalidRequestException;
import com.salesforce.einstein.tagging.domain.LimitExceededException;
import com.salesforce.einstein.tagging.domain.NotFoundException;
import com.salesforce.einstein.tagging.domain.PreconditionRequiredException;
import com.salesforce.einstein.tagging.domain.RateLimitedException;
import com.salesforce.einstein.tagging.domain.StaleVersionException;
import com.salesforce.einstein.tagging.domain.TaggingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.net.URI;

/** Maps every failure to an RFC 7807 problem. Internals never leak: 5xx bodies are generic. */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(TaggingException.class)
    ResponseEntity<ProblemDetail> business(TaggingException e) {
        HttpStatus status = status(e);
        ProblemDetail p = ProblemDetail.forStatusAndDetail(status, e.getMessage());
        p.setType(URI.create("https://tagging.example/problems/" + e.getClass().getSimpleName()
                .replace("Exception", "").replaceAll("([a-z])([A-Z])", "$1-$2").toLowerCase()));
        ResponseEntity.BodyBuilder b = ResponseEntity.status(status);
        if (e instanceof RateLimitedException rl) {
            b.header(HttpHeaders.RETRY_AFTER, Long.toString(Math.max(1, rl.retryAfter().toSeconds())));
        }
        return b.body(p);
    }

    /** Deadlock victim, serialization failure, lock timeout: safe for the client to retry. */
    @ExceptionHandler({ConcurrencyFailureException.class, TransientDataAccessException.class})
    ResponseEntity<ProblemDetail> transientFailure(RuntimeException e) {
        log.warn("transient data access failure", e);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).header(HttpHeaders.RETRY_AFTER, "1")
                .body(ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, "temporarily unavailable, retry"));
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ProblemDetail> unexpected(Exception e) {
        log.error("unhandled error", e);
        return ResponseEntity.internalServerError()
                .body(ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, "internal error"));
    }

    static HttpStatus status(TaggingException e) {
        if (e instanceof InvalidRequestException) {
            return HttpStatus.BAD_REQUEST;
        } else if (e instanceof NotFoundException) {
            return HttpStatus.NOT_FOUND;
        } else if (e instanceof DuplicateTagNameException) {
            return HttpStatus.CONFLICT;
        } else if (e instanceof StaleVersionException) {
            return HttpStatus.PRECONDITION_FAILED;
        } else if (e instanceof PreconditionRequiredException) {
            return HttpStatus.PRECONDITION_REQUIRED;
        } else if (e instanceof LimitExceededException || e instanceof IdempotencyConflictException) {
            return HttpStatus.UNPROCESSABLE_ENTITY;
        } else if (e instanceof RateLimitedException) {
            return HttpStatus.TOO_MANY_REQUESTS;
        }
        return HttpStatus.BAD_REQUEST;
    }
}
