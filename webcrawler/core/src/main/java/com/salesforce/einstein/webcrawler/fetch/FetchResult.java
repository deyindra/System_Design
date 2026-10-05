package com.salesforce.einstein.webcrawler.fetch;

/**
 * One HTTP exchange, redirects <b>not</b> followed: the worker handles 3xx itself so the target goes
 * through normalization and the seen-test like any other link (which is what breaks redirect loops).
 *
 * @param status    HTTP status, 0 for a network error / timeout, or {@link #BLOCKED} when the egress policy refused
 *                  the destination ({@code error} says why)
 * @param location  {@code Location} header for 3xx
 * @param truncated the body exceeded {@link FetchRequest#maxBytes()}
 */
public record FetchResult(int status, String contentType, byte[] body, String location,
                          String etag, String lastModified, boolean truncated, String error) {

    /** Not a network outcome: the request was never sent. Final, never retried. */
    public static final int BLOCKED = -1;

    public static FetchResult ok(String contentType, byte[] body) {
        return new FetchResult(200, contentType, body, null, null, null, false, null);
    }

    public static FetchResult redirect(int status, String location) {
        return new FetchResult(status, null, new byte[0], location, null, null, false, null);
    }

    public static FetchResult status(int status) {
        return new FetchResult(status, null, new byte[0], null, null, null, false, null);
    }

    public static FetchResult networkError(String error) {
        return new FetchResult(0, null, new byte[0], null, null, null, false, error);
    }

    public static FetchResult blocked(String reason) {
        return new FetchResult(BLOCKED, null, new byte[0], null, null, null, false, "blocked by egress policy: " + reason);
    }

    public boolean isRedirect() { return status >= 300 && status < 400 && status != 304 && location != null; }

    /** Worth retrying with backoff: timeouts, connection errors, 429 and 5xx. 4xx is final. */
    public boolean isRetryable() { return status == 0 || status == 429 || status >= 500; }
}
