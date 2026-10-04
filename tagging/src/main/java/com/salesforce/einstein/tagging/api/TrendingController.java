package com.salesforce.einstein.tagging.api;

import com.salesforce.einstein.tagging.api.dto.Dtos.TrendingResponse;
import com.salesforce.einstein.tagging.domain.TrendRank;
import com.salesforce.einstein.tagging.domain.TrendWindow;
import com.salesforce.einstein.tagging.service.TrendingService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ConditionalOnProperty(name = "tagging.trending.enabled", havingValue = "true", matchIfMissing = true)
@io.swagger.v3.oas.annotations.tags.Tag(name = "Trending", description = "Popular and rising tags (eventual)")
public class TrendingController {
    private final TrendingService trending;

    public TrendingController(TrendingService trending) {
        this.trending = trending;
    }

    @GetMapping("/v1/tags:trending")
    @Operation(summary = "Top tags by attaches in the last 24h or 7d. Always eventual (seconds behind writes)")
    public TrendingResponse trending(Caller caller,
                                     @RequestParam(defaultValue = "24h") TrendWindow window,
                                     @RequestParam(defaultValue = "popular") TrendRank rank,
                                     @RequestParam(required = false) @Nullable Integer limit) {
        return TrendingResponse.of(trending.top(caller.tenantId(), window, rank, limit));
    }
}
