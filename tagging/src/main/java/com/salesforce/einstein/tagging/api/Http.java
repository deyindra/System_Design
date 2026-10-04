package com.salesforce.einstein.tagging.api;

import com.salesforce.einstein.tagging.domain.ConsistencyToken;
import com.salesforce.einstein.tagging.domain.InvalidRequestException;
import com.salesforce.einstein.tagging.domain.PreconditionRequiredException;
import com.salesforce.einstein.tagging.domain.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** HTTP conventions shared by the controllers. */
final class Http {
    @SuppressWarnings("IncorrectHttpHeaderInspection")   // our own header, not an IANA-registered one
    static final String TOKEN_HEADER = "X-Consistency-Token";
    static final String IDEMPOTENCY_HEADER = "Idempotency-Key";
    private static final Pattern ETAG = Pattern.compile("(?:W/)?\"v(\\d{1,18})\"");

    private Http() {
    }

    static String etag(Tag t) {
        return "\"v" + t.version() + "\"";
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
            throw new PreconditionRequiredException("If-Match with the ETag you read is required (\"*\" would allow lost updates)");
        }
        return v;
    }

    static ConsistencyToken token(@Nullable String header) {
        return ConsistencyToken.parse(header);
    }

    static ResponseEntity.BodyBuilder withToken(ResponseEntity.BodyBuilder b, ConsistencyToken token) {
        return token.isNone() ? b : b.header(TOKEN_HEADER, token.format());
    }
}
