package com.salesforce.einstein.webcrawler.fetch;

import java.net.UnknownHostException;
import java.util.Optional;

/**
 * Applies the {@link EgressPolicy} to every request before it reaches the network. A decorator, so the engine and
 * {@link RobotsCache} are unaware of it and tests can use a fake web without it. Because the engine follows redirects
 * itself, each hop of a redirect chain is checked as its own request.
 */
public final class EgressFilteringFetcher implements Fetcher {

    private final Fetcher delegate;
    private final EgressPolicy policy;

    public EgressFilteringFetcher(Fetcher delegate, EgressPolicy policy) {
        this.delegate = delegate;
        this.policy = policy;
    }

    @Override public FetchResult fetch(FetchRequest request) {
        Optional<String> refused;
        try {
            refused = policy.check(request.url());
        } catch (UnknownHostException e) {
            return FetchResult.networkError("UnknownHostException: " + request.url().getHost());
        }
        return refused.map(FetchResult::blocked).orElseGet(() -> delegate.fetch(request));
    }
}
