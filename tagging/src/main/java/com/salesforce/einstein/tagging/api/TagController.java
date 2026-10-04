package com.salesforce.einstein.tagging.api;

import com.salesforce.einstein.tagging.api.dto.Dtos.BulkAttachRequest;
import com.salesforce.einstein.tagging.api.dto.Dtos.CreateTagRequest;
import com.salesforce.einstein.tagging.api.dto.Dtos.EntityPageResponse;
import com.salesforce.einstein.tagging.api.dto.Dtos.EntityRefDto;
import com.salesforce.einstein.tagging.api.dto.Dtos.TagPageResponse;
import com.salesforce.einstein.tagging.api.dto.Dtos.TagResponse;
import com.salesforce.einstein.tagging.api.dto.Dtos.UpdateTagRequest;
import com.salesforce.einstein.tagging.domain.Consistency;
import com.salesforce.einstein.tagging.domain.Page;
import com.salesforce.einstein.tagging.domain.SearchResult;
import com.salesforce.einstein.tagging.domain.Tag;
import com.salesforce.einstein.tagging.domain.WriteResult;
import com.salesforce.einstein.tagging.service.AssignmentService;
import com.salesforce.einstein.tagging.service.AssignmentService.BulkAttachResult;
import com.salesforce.einstein.tagging.service.TagSearchService;
import com.salesforce.einstein.tagging.service.TagService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag.*;
import jakarta.validation.Valid;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

@RestController
@RequestMapping("/v1")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Tags", description = "Tag metadata, reverse lookup and bulk tagging")
public class TagController {
    private final TagService tags;
    private final AssignmentService assignments;
    private final TagSearchService search;

    public TagController(TagService tags, AssignmentService assignments, TagSearchService search) {
        this.tags = tags;
        this.assignments = assignments;
        this.search = search;
    }

    @PostMapping("/tags")
    @Operation(summary = "Create a tag (409 if the name exists in the tenant)")
    public ResponseEntity<TagResponse> create(Caller caller, @Valid @RequestBody CreateTagRequest body) {
        WriteResult<Tag> r = tags.create(caller.tenantId(), caller.actorId(), body.name(), body.color());
        return Http.withToken(ResponseEntity.created(URI.create("/v1/tags/" + r.value().tagId())), r.token())
                .eTag(Http.etag(r.value()))
                .body(TagResponse.of(r.value(), 0L));
    }

    @GetMapping("/tags")
    @Operation(summary = "List / autocomplete tags by name prefix")
    public TagPageResponse list(Caller caller,
                                @RequestParam(required = false) @Nullable String prefix,
                                @RequestParam(required = false) @Nullable String cursor,
                                @RequestParam(required = false) @Nullable Integer limit,
                                @RequestParam(required = false) @Nullable Consistency consistency,
                                @RequestHeader(value = Http.TOKEN_HEADER, required = false) @Nullable String token) {
        Page<Tag> page = tags.list(caller.tenantId(), prefix, cursor, limit, consistency, Http.token(token));
        return new TagPageResponse(page.items().stream().map(t -> TagResponse.of(t, null)).toList(), page.nextCursor());
    }

    @GetMapping("/tags/{tagId}")
    public ResponseEntity<TagResponse> get(Caller caller, @PathVariable long tagId,
                                           @RequestParam(required = false) @Nullable Consistency consistency,
                                           @RequestHeader(value = Http.TOKEN_HEADER, required = false) @Nullable String token) {
        TagService.TagDetails d = tags.get(caller.tenantId(), tagId, consistency, Http.token(token));
        return ResponseEntity.ok().eTag(Http.etag(d.tag())).body(TagResponse.of(d.tag(), d.usage()));
    }

    @PatchMapping("/tags/{tagId}")
    @Operation(summary = "Rename / recolor. Requires If-Match (412 when stale, 428 when missing)")
    public ResponseEntity<TagResponse> update(Caller caller, @PathVariable long tagId,
                                              @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) @Nullable String ifMatch,
                                              @Valid @RequestBody UpdateTagRequest body) {
        WriteResult<Tag> r = tags.update(caller.tenantId(), caller.actorId(), tagId, Http.requireIfMatch(ifMatch),
                body.name(), body.color());
        return Http.withToken(ResponseEntity.ok(), r.token()).eTag(Http.etag(r.value()))
                .body(TagResponse.of(r.value(), null));
    }

    @DeleteMapping("/tags/{tagId}")
    @Operation(summary = "Soft delete; assignments are purged asynchronously")
    public ResponseEntity<Void> delete(Caller caller, @PathVariable long tagId,
                                       @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) @Nullable String ifMatch) {
        WriteResult<Void> r = tags.delete(caller.tenantId(), caller.actorId(), tagId, Http.ifMatch(ifMatch));
        return Http.withToken(ResponseEntity.status(HttpStatus.NO_CONTENT), r.token()).build();
    }

    @GetMapping("/tags/{tagId}/entities")
    @Operation(summary = "Entities carrying a tag (keyset paginated)")
    public EntityPageResponse entities(Caller caller, @PathVariable long tagId,
                                       @RequestParam(required = false) @Nullable String type,
                                       @RequestParam(required = false) @Nullable String cursor,
                                       @RequestParam(required = false) @Nullable Integer limit,
                                       @RequestParam(required = false) @Nullable Consistency consistency,
                                       @RequestHeader(value = Http.TOKEN_HEADER, required = false) @Nullable String token) {
        SearchResult r = search.entitiesOf(caller.tenantId(), tagId, type, cursor, limit, consistency, Http.token(token));
        return new EntityPageResponse(r.items().stream().map(EntityRefDto::of).toList(), r.nextCursor(), null);
    }

    @PostMapping("/tags:bulkAttach")
    @Operation(summary = "Attach tags to up to 500 entities atomically. Requires Idempotency-Key")
    public ResponseEntity<BulkAttachResult> bulkAttach(Caller caller,
                                                       @RequestHeader(value = Http.IDEMPOTENCY_HEADER, required = false) @Nullable String key,
                                                       @Valid @RequestBody BulkAttachRequest body) {
        WriteResult<BulkAttachResult> r = assignments.bulkAttach(caller.tenantId(), caller.actorId(), key,
                body.entities().stream().map(EntityRefDto::toRef).toList(), body.tagIds());
        return Http.withToken(ResponseEntity.ok(), r.token()).body(r.value());
    }
}
