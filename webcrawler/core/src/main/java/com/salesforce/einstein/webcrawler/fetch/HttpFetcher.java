package com.salesforce.einstein.webcrawler.fetch;

import java.io.ByteArrayOutputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link Fetcher} over the JDK HTTP client (HTTP/2, connection pooling per host). Bodies are cut at {@code maxBytes},
 * so a 4 GB video linked as an "image" cannot exhaust a worker's memory, and the whole exchange (headers <i>and</i>
 * body) must finish within {@code timeout}, so a server that drips one byte a second cannot hold a worker forever.
 * In production large assets stream straight to the blob store with a multipart upload instead.
 */
public final class HttpFetcher implements Fetcher {

    public static final String USER_AGENT = "EinsteinCrawler/1.0 (+https://example.com/crawler)";

    private final HttpClient client;
    private final Duration timeout;

    public HttpFetcher(Duration connectTimeout, Duration timeout) {
        this.client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(connectTimeout)
                .build();
        this.timeout = timeout;
    }

    @Override public FetchResult fetch(FetchRequest req) {
        HttpRequest.Builder b = HttpRequest.newBuilder(req.url()).timeout(timeout).GET()
                .header("User-Agent", USER_AGENT)
                .header("Accept", "text/html,application/xhtml+xml,*/*;q=0.8");
        if (req.etag() != null) b.header("If-None-Match", req.etag());
        if (req.lastModified() != null) b.header("If-Modified-Since", req.lastModified());
        CompletableFuture<HttpResponse<CappedBody.Bytes>> f =
                client.sendAsync(b.build(), info -> new CappedBody(req.maxBytes()));
        try {
            HttpResponse<CappedBody.Bytes> r = f.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return new FetchResult(r.statusCode(), r.headers().firstValue("Content-Type").orElse(null),
                    r.body().bytes(), r.headers().firstValue("Location").orElse(null),
                    r.headers().firstValue("ETag").orElse(null), r.headers().firstValue("Last-Modified").orElse(null),
                    r.body().truncated(), null);
        } catch (TimeoutException e) {
            f.cancel(true);                                              // aborts the exchange and frees the connection
            return FetchResult.networkError("deadline of " + timeout.toMillis() + " ms exceeded");
        } catch (ExecutionException e) {
            Throwable c = e.getCause() == null ? e : e.getCause();
            return FetchResult.networkError(c.getClass().getSimpleName() + ": " + c.getMessage());
        } catch (InterruptedException e) {
            f.cancel(true);
            Thread.currentThread().interrupt();
            return FetchResult.networkError("interrupted");
        }
    }

    /** Collects at most {@code max} bytes, then cancels the stream instead of reading (and discarding) the rest. */
    private static final class CappedBody implements HttpResponse.BodySubscriber<CappedBody.Bytes> {

        record Bytes(byte[] bytes, boolean truncated) { }

        private final long max;
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private final CompletableFuture<Bytes> done = new CompletableFuture<>();
        private Flow.Subscription subscription;

        CappedBody(long max) { this.max = max; }

        @Override public CompletionStage<Bytes> getBody() { return done; }

        @Override public void onSubscribe(Flow.Subscription s) {
            subscription = s;
            s.request(1);
        }

        @Override public void onNext(List<ByteBuffer> buffers) {
            boolean truncated = false;
            for (ByteBuffer buf : buffers) {
                int take = (int) Math.min(buf.remaining(), max - out.size());
                byte[] chunk = new byte[take];
                buf.get(chunk);
                out.write(chunk, 0, take);
                truncated |= buf.hasRemaining();
            }
            if (truncated) {
                subscription.cancel();
                done.complete(new Bytes(out.toByteArray(), true));
            } else {
                subscription.request(1);
            }
        }

        @Override public void onError(Throwable t) { done.completeExceptionally(t); }

        @Override public void onComplete() { done.complete(new Bytes(out.toByteArray(), false)); }
    }
}
