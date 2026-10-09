package com.salesforce.einstein.hierarchy.api;

import com.salesforce.einstein.hierarchy.api.dto.Dtos.CreateNodeRequest;
import com.salesforce.einstein.hierarchy.api.dto.Dtos.CreateSpaceRequest;
import com.salesforce.einstein.hierarchy.api.dto.Dtos.NodeResponse;
import com.salesforce.einstein.hierarchy.api.dto.Dtos.SpaceResponse;
import com.salesforce.einstein.hierarchy.domain.Caller;
import com.salesforce.einstein.hierarchy.domain.CreateNode;
import com.salesforce.einstein.hierarchy.domain.Node;
import com.salesforce.einstein.hierarchy.domain.Space;
import com.salesforce.einstein.hierarchy.domain.Written;
import com.salesforce.einstein.hierarchy.service.TreeService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.List;

@RestController
@RequestMapping("/v1/spaces")
public class SpaceController {
    private final TreeService tree;

    public SpaceController(TreeService tree) {
        this.tree = tree;
    }

    @PostMapping
    public ResponseEntity<SpaceResponse> create(Caller caller, @Valid @RequestBody CreateSpaceRequest body) {
        Written<Space> r = tree.createSpace(caller, body.key(), body.title());
        return Http.withToken(ResponseEntity.created(URI.create("/v1/spaces/" + r.value().id())), r.token())
                .body(SpaceResponse.of(r.value()));
    }

    @GetMapping("/{spaceId}")
    public SpaceResponse get(Caller caller, @PathVariable long spaceId,
                             @RequestHeader(value = Http.TOKEN_HEADER, required = false) @Nullable String token) {
        return SpaceResponse.of(tree.space(caller, spaceId, Http.token(token)));
    }

    /** Creates a node; with {@code Idempotency-Key}, a retried request returns the node created the first time. */
    @PostMapping("/{spaceId}/nodes")
    public ResponseEntity<NodeResponse> createNode(
            Caller caller, @PathVariable long spaceId, @Valid @RequestBody CreateNodeRequest body,
            @RequestHeader(value = Http.IDEMPOTENCY_HEADER, required = false) @Nullable String idempotencyKey) {
        CreateNode req = new CreateNode(Long.parseLong(body.parentId()), body.title(), body.type(),
                id(body.afterId()), id(body.beforeId()), body.body());
        Written<Node> r = tree.create(caller, spaceId, req, Http.idempotencyKey(idempotencyKey));
        return Http.withToken(ResponseEntity.created(URI.create("/v1/nodes/" + r.value().id())), r.token())
                .eTag(Http.etag(r.value()))
                .body(NodeResponse.of(r.value()));
    }

    /** Trashed subtree roots of the space. */
    @GetMapping("/{spaceId}/trash")
    public List<NodeResponse> trash(Caller caller, @PathVariable long spaceId,
                                    @RequestHeader(value = Http.TOKEN_HEADER, required = false) @Nullable String token) {
        return tree.trash(caller, spaceId, Http.token(token)).stream().map(NodeResponse::of).toList();
    }

    @Nullable
    static Long id(@Nullable String s) {
        return s == null ? null : Long.valueOf(s);
    }
}
