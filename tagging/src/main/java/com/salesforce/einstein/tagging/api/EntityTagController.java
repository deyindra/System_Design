package com.salesforce.einstein.tagging.api;

import com.salesforce.einstein.tagging.api.dto.Dtos.AttachRequest;
import com.salesforce.einstein.tagging.api.dto.Dtos.EntityTagsResponse;
import com.salesforce.einstein.tagging.api.dto.Dtos.TagIdsRequest;
import com.salesforce.einstein.tagging.domain.Consistency;
import com.salesforce.einstein.tagging.domain.EntityRef;
import com.salesforce.einstein.tagging.domain.EntityTags;
import com.salesforce.einstein.tagging.domain.WriteResult;
import com.salesforce.einstein.tagging.service.AssignmentService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Tags of one entity, addressed {@code /v1/entities/{type}/{id}/tags} (e.g. {@code jira:issue/10042}). */
@RestController
@RequestMapping("/v1/entities/{type}/{id}")
@Tag(name = "Entity tags", description = "The hot path: read and change the tags of one entity")
public class EntityTagController {
    private final AssignmentService assignments;

    public EntityTagController(AssignmentService assignments) {
        this.assignments = assignments;
    }

    @GetMapping("/tags")
    @Operation(summary = "Tags of an entity. consistency=STRONG|SESSION|EVENTUAL (SESSION uses X-Consistency-Token)")
    public ResponseEntity<EntityTagsResponse> get(Caller caller, @PathVariable String type, @PathVariable String id,
                                                  @RequestParam(required = false) @Nullable Consistency consistency,
                                                  @RequestHeader(value = Http.TOKEN_HEADER, required = false) @Nullable String token) {
        EntityTags et = assignments.tagsOf(caller.tenantId(), new EntityRef(type, id), consistency, Http.token(token));
        // Echo the client's own token rather than the shard-wide relay position the read observed: that
        // position covers other tenants' writes, and a SESSION search holding it would wait on events
        // that never come for this tenant and fall back to the store.
        return Http.withToken(ResponseEntity.ok(), Http.token(token)).body(EntityTagsResponse.of(et));
    }

    @PutMapping("/tags")
    @Operation(summary = "Replace the entity's tags with exactly this set")
    public ResponseEntity<EntityTagsResponse> replace(Caller caller, @PathVariable String type, @PathVariable String id,
                                                      @Valid @RequestBody TagIdsRequest body) {
        return respond(assignments.replace(caller.tenantId(), caller.actorId(), new EntityRef(type, id), body.tagIds()));
    }

    @PostMapping("/tags:attach")
    @Operation(summary = "Idempotently add tags by id and/or name (unknown names are created)")
    public ResponseEntity<EntityTagsResponse> attach(Caller caller, @PathVariable String type, @PathVariable String id,
                                                     @Valid @RequestBody AttachRequest body) {
        return respond(assignments.attach(caller.tenantId(), caller.actorId(), new EntityRef(type, id),
                body.tagIds(), body.tagNames()));
    }

    @PostMapping("/tags:detach")
    @Operation(summary = "Idempotently remove tags")
    public ResponseEntity<EntityTagsResponse> detach(Caller caller, @PathVariable String type, @PathVariable String id,
                                                     @Valid @RequestBody TagIdsRequest body) {
        return respond(assignments.detach(caller.tenantId(), caller.actorId(), new EntityRef(type, id), body.tagIds()));
    }

    private static ResponseEntity<EntityTagsResponse> respond(WriteResult<EntityTags> r) {
        return Http.withToken(ResponseEntity.ok(), r.token()).body(EntityTagsResponse.of(r.value()));
    }
}
