package com.salesforce.einstein.webcrawler.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.einstein.webcrawler.fetch.EgressPolicy;
import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.store.JobStore;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.function.Consumer;

/**
 * POSTs the final {@link ApiModels.JobView} to the job's {@code callbackUrl}, fire-and-forget.
 * Production: an outbox row per finished job, retried with backoff, signed with an HMAC header
 * ({@code X-Crawler-Signature}) so the receiver can verify it. The callback URL is tenant input, so it passes the same
 * {@link EgressPolicy} as every fetch, and redirects are not followed.
 */
public final class WebhookNotifier implements Consumer<CrawlJob> {

    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ObjectMapper json;
    private final JobStore jobs;
    private final EgressPolicy egress;

    public WebhookNotifier(ObjectMapper json, JobStore jobs, EgressPolicy egress) {
        this.json = json;
        this.jobs = jobs;
        this.egress = egress;
    }

    @Override public void accept(CrawlJob job) {
        String url = job.request().callbackUrl();
        if (url == null) return;
        try {
            URI target = URI.create(url);
            if (egress.check(target).isPresent()) return;          // production: record the refusal on the outbox row
            byte[] body = json.writeValueAsBytes(ApiModels.JobView.of(job, jobs.stats(job.jobId())));
            client.sendAsync(HttpRequest.newBuilder(target).timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(), HttpResponse.BodyHandlers.discarding());
        } catch (Exception ignored) {
            // best effort in this build
        }
    }
}
