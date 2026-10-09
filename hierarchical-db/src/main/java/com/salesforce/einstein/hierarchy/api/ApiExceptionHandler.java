package com.salesforce.einstein.hierarchy.api;

import com.salesforce.einstein.hierarchy.domain.ConflictException;
import com.salesforce.einstein.hierarchy.domain.ForbiddenException;
import com.salesforce.einstein.hierarchy.domain.HierarchyException;
import com.salesforce.einstein.hierarchy.domain.IdempotencyConflictException;
import com.salesforce.einstein.hierarchy.domain.InvalidRequestException;
import com.salesforce.einstein.hierarchy.domain.NotFoundException;
import com.salesforce.einstein.hierarchy.domain.PreconditionRequiredException;
import com.salesforce.einstein.hierarchy.domain.StaleVersionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.CannotAcquireLockException;
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

    @ExceptionHandler(HierarchyException.class)
    ResponseEntity<ProblemDetail> business(HierarchyException e) {
        HttpStatus status = status(e);
        ProblemDetail p = ProblemDetail.forStatusAndDetail(status, e.getMessage());
        p.setType(URI.create("https://hierarchy.example/problems/" + e.getClass().getSimpleName()
                .replace("Exception", "").replaceAll("([a-z])([A-Z])", "$1-$2").toLowerCase()));
        ResponseEntity.BodyBuilder b = ResponseEntity.status(status);
        if (e instanceof ConflictException c && c.retryable()) {
            b.header(HttpHeaders.RETRY_AFTER, "1");
        }
        return b.body(p);
    }

    /** The space lock wasn't free within {@code lock-timeout}: a move holds it. Retry shortly. */
    @ExceptionHandler(CannotAcquireLockException.class)
    ResponseEntity<ProblemDetail> lockTimeout(CannotAcquireLockException e) {
        log.debug("space lock busy", e);
        return ResponseEntity.status(HttpStatus.CONFLICT).header(HttpHeaders.RETRY_AFTER, "1")
                .body(ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, "the space is busy (a move is running), retry"));
    }

    /** Deadlock victim, serialization failure, lost connection: safe for the client to retry. */
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

    static HttpStatus status(HierarchyException e) {
        if (e instanceof InvalidRequestException) {
            return HttpStatus.BAD_REQUEST;
        } else if (e instanceof NotFoundException) {
            return HttpStatus.NOT_FOUND;
        } else if (e instanceof ForbiddenException) {
            return HttpStatus.FORBIDDEN;
        } else if (e instanceof ConflictException) {
            return HttpStatus.CONFLICT;
        } else if (e instanceof StaleVersionException) {
            return HttpStatus.PRECONDITION_FAILED;
        } else if (e instanceof PreconditionRequiredException) {
            return HttpStatus.PRECONDITION_REQUIRED;
        } else if (e instanceof IdempotencyConflictException) {
            return HttpStatus.UNPROCESSABLE_ENTITY;
        }
        return HttpStatus.BAD_REQUEST;
    }
}
