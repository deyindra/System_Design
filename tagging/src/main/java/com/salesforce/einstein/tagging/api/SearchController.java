package com.salesforce.einstein.tagging.api;

import com.salesforce.einstein.tagging.api.dto.Dtos.EntityPageResponse;
import com.salesforce.einstein.tagging.api.dto.Dtos.EntityRefDto;
import com.salesforce.einstein.tagging.api.dto.Dtos.SearchRequest;
import com.salesforce.einstein.tagging.domain.SearchResult;
import com.salesforce.einstein.tagging.service.TagSearchService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Tag(name = "Search", description = "Boolean tag search across entities")
public class SearchController {
    static final String SERVED_BY_HEADER = "X-Served-By";
    private final TagSearchService search;

    public SearchController(TagSearchService search) {
        this.search = search;
    }

    @PostMapping("/v1/search")
    @Operation(summary = "Entities with ALL of `all`, at least one of `any`, NONE of `none`")
    public ResponseEntity<EntityPageResponse> search(Caller caller, @Valid @RequestBody SearchRequest body,
                                                     @RequestHeader(value = Http.TOKEN_HEADER, required = false) @Nullable String token) {
        SearchResult r = search.search(caller.tenantId(), body.all(), body.any(), body.none(), body.entityType(),
                body.cursor(), body.limit(), body.consistency(), Http.token(token));
        return ResponseEntity.ok().header(SERVED_BY_HEADER, r.servedBy())
                .body(new EntityPageResponse(r.items().stream().map(EntityRefDto::of).toList(), r.nextCursor(),
                        r.total() < 0 ? null : r.total()));
    }
}
