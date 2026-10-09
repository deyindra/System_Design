package com.salesforce.einstein.hierarchy.api;

import com.salesforce.einstein.hierarchy.domain.InvalidRequestException;
import com.salesforce.einstein.hierarchy.domain.Node;
import com.salesforce.einstein.hierarchy.domain.PreconditionRequiredException;
import com.salesforce.einstein.hierarchy.domain.ReadToken;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** HTTP conventions shared by the controllers. */
final class Http {
    static final String TOKEN_HEADER = "X-Read-Token";
    static final String IDEMPOTENCY_HEADER = "Idempotency-Key";
    private static final Pattern ETAG = Pattern.compile("(?:W/)?\"v(\\d{1,18})\"");
    private static final Pattern IDEMPOTENCY_KEY = Pattern.compile("[A-Za-z0-9._:-]{8,128}");

    private Http() {
    }

    static String etag(Node n) {
        return "\"v" + n.version() + "\"";
    }

    /** Parses {@code If-Match: "v<version>"}. Returns null when absent or {@code *} (any current version). */
    @Nullable
    static Long ifMatch(@Nullable String header) {
        if (header == null || header.isBlank() || header.trim().equals("*")) {
            return null;
        }
        Matcher m = ETAG.matcher(header.trim());
        if (!m.matches()) {
            throw new InvalidRequestException("If-Match must be an ETag returned by this API");
        }
        return Long.parseLong(m.group(1));
    }

    static long requireIfMatch(@Nullable String header) {
        Long v = ifMatch(header);
        if (v == null) {
            throw new PreconditionRequiredException(
                    "If-Match with the ETag you read is required (\"*\" would allow lost updates)");
        }
        return v;
    }

    @Nullable
    static String idempotencyKey(@Nullable String header) {
        if (header == null) {
            return null;
        }
        if (!IDEMPOTENCY_KEY.matcher(header).matches()) {
            throw new InvalidRequestException(IDEMPOTENCY_HEADER + " must match " + IDEMPOTENCY_KEY.pattern());
        }
        return header;
    }

    static ReadToken token(@Nullable String header) {
        return ReadToken.parse(header);
    }

    static ResponseEntity.BodyBuilder withToken(ResponseEntity.BodyBuilder b, ReadToken token) {
        return token.isNone() ? b : b.header(TOKEN_HEADER, token.format());
    }
}
