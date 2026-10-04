package com.salesforce.einstein.tagging.domain;

import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A taggable object owned by some product, addressed ARI-style as {@code type/id}, for example
 * {@code jira:issue/10042} or {@code confluence:page/98765}.
 *
 * <p>The tagging service never interprets the id. It only needs a stable, bounded string. Both parts are
 * restricted to ASCII so keys hash and compare identically in every store and every language.
 */
public record EntityRef(String type, String id) {
    private static final Pattern TYPE = Pattern.compile("[a-z][a-z0-9-]{0,15}(:[a-z][a-z0-9-]{0,15})?");
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9._-]{1,128}");

    public EntityRef {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(id, "id");
        if (!TYPE.matcher(type).matches()) {
            throw new InvalidRequestException("entity type must match " + TYPE.pattern() + ": " + type);
        }
        if (!ID.matcher(id).matches()) {
            throw new InvalidRequestException("entity id must match " + ID.pattern());
        }
    }

    /** Validates an optional entity-type filter: null means "any type", anything else must be a valid type. */
    public static void checkTypeFilter(@Nullable String type) {
        if (type != null && !TYPE.matcher(type).matches()) {
            throw new InvalidRequestException("invalid entity type: " + type);
        }
    }

    @Override
    @NonNull
    public String toString() {
        return type + "/" + id;
    }
}
