package com.salesforce.einstein.hierarchy.api;

import com.salesforce.einstein.hierarchy.api.dto.Dtos.CrumbResponse;
import com.salesforce.einstein.hierarchy.api.dto.Dtos.MoveJobResponse;
import com.salesforce.einstein.hierarchy.api.dto.Dtos.MoveNodeRequest;
import com.salesforce.einstein.hierarchy.api.dto.Dtos.MoveResponse;
import com.salesforce.einstein.hierarchy.api.dto.Dtos.NodeResponse;
import com.salesforce.einstein.hierarchy.api.dto.Dtos.PageResponse;
import com.salesforce.einstein.hierarchy.api.dto.Dtos.PurgeResponse;
import com.salesforce.einstein.hierarchy.api.dto.Dtos.RestrictionsRequest;
import com.salesforce.einstein.hierarchy.api.dto.Dtos.RestrictionsResponse;
import com.salesforce.einstein.hierarchy.api.dto.Dtos.UpdateNodeRequest;
import com.salesforce.einstein.hierarchy.domain.Caller;
import com.salesforce.einstein.hierarchy.domain.MoveNode;
import com.salesforce.einstein.hierarchy.domain.MoveOutcome;
import com.salesforce.einstein.hierarchy.domain.Node;
import com.salesforce.einstein.hierarchy.domain.NodeDetail;
import com.salesforce.einstein.hierarchy.domain.RestrictionOp;
import com.salesforce.einstein.hierarchy.domain.Written;
import com.salesforce.einstein.hierarchy.service.TreeService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/v1/nodes")
public class NodeController {
    private final TreeService tree;

    public NodeController(TreeService tree) {
        this.tree = tree;
    }

    @GetMapping("/{id}")
    public ResponseEntity<NodeResponse> get(Caller caller, @PathVariable long id,
                                            @RequestParam(defaultValue = "false") boolean body,
                                            @RequestHeader(value = Http.TOKEN_HEADER, required = false)
                                            @Nullable String token) {
        NodeDetail d = tree.get(caller, id, body, Http.token(token));
        return ResponseEntity.ok().eTag(Http.etag(d.node())).body(NodeResponse.of(d.node(), d.inTrash(), d.body()));
    }

    @GetMapping("/{id}/children")
    public PageResponse<NodeResponse> children(Caller caller, @PathVariable long id,
                                               @RequestParam(defaultValue = "" + TreeService.DEFAULT_PAGE) int limit,
                                               @RequestParam(required = false) @Nullable String cursor,
                                               @RequestHeader(value = Http.TOKEN_HEADER, required = false)
                                               @Nullable String token) {
        return PageResponse.of(tree.children(caller, id, limit, cursor, Http.token(token)), NodeResponse::of);
    }

    /** Breadcrumbs: the node's ancestors, root first. */
    @GetMapping("/{id}/ancestors")
    public List<CrumbResponse> ancestors(Caller caller, @PathVariable long id,
                                         @RequestHeader(value = Http.TOKEN_HEADER, required = false)
                                         @Nullable String token) {
        return tree.ancestors(caller, id, Http.token(token)).stream().map(CrumbResponse::of).toList();
    }

    /** The subtree in pre-order, up to {@code depth} levels below the node. */
    @GetMapping("/{id}/descendants")
    public PageResponse<NodeResponse> descendants(Caller caller, @PathVariable long id,
                                                  @RequestParam(defaultValue = "1") int depth,
                                                  @RequestParam(defaultValue = "" + TreeService.DEFAULT_PAGE) int limit,
                                                  @RequestParam(required = false) @Nullable String cursor,
                                                  @RequestHeader(value = Http.TOKEN_HEADER, required = false)
                                                  @Nullable String token) {
        return PageResponse.of(tree.descendants(caller, id, depth, limit, cursor, Http.token(token)),
                NodeResponse::of);
    }

    @PatchMapping("/{id}")
    public ResponseEntity<NodeResponse> update(Caller caller, @PathVariable long id,
                                               @Valid @RequestBody UpdateNodeRequest body,
                                               @RequestHeader(value = "If-Match", required = false)
                                               @Nullable String ifMatch) {
        Written<Node> r = tree.update(caller, id, Http.requireIfMatch(ifMatch), body.title(), body.body());
        return written(ResponseEntity.ok(), r);
    }

    /**
     * Moves the node with its subtree, or reorders it among its siblings when the parent is unchanged. 200 when done;
     * 202 with a move job when the subtree is large (reads already show it moved).
     */
    @PostMapping("/{id}/move")
    public ResponseEntity<MoveResponse> move(Caller caller, @PathVariable long id,
                                             @Valid @RequestBody MoveNodeRequest body,
                                             @RequestHeader(value = "If-Match", required = false)
                                             @Nullable String ifMatch) {
        MoveNode req = new MoveNode(Long.parseLong(body.newParentId()), SpaceController.id(body.afterId()),
                SpaceController.id(body.beforeId()));
        Written<MoveOutcome> r = tree.move(caller, id, req, Http.ifMatch(ifMatch));
        Node n = r.value().node();
        if (r.value() instanceof MoveOutcome.Accepted a) {
            return Http.withToken(ResponseEntity.status(HttpStatus.ACCEPTED), r.token()).eTag(Http.etag(n))
                    .body(new MoveResponse(NodeResponse.of(n), MoveJobResponse.of(a.job())));
        }
        return Http.withToken(ResponseEntity.ok(), r.token()).eTag(Http.etag(n))
                .body(new MoveResponse(NodeResponse.of(n), null));
    }

    @PostMapping("/{id}/trash")
    public ResponseEntity<NodeResponse> trash(Caller caller, @PathVariable long id,
                                              @RequestHeader(value = "If-Match", required = false)
                                              @Nullable String ifMatch) {
        return written(ResponseEntity.ok(), tree.trash(caller, id, Http.requireIfMatch(ifMatch)));
    }

    @PostMapping("/{id}/restore")
    public ResponseEntity<NodeResponse> restore(Caller caller, @PathVariable long id,
                                                @RequestHeader(value = "If-Match", required = false)
                                                @Nullable String ifMatch) {
        return written(ResponseEntity.ok(), tree.restore(caller, id, Http.requireIfMatch(ifMatch)));
    }

    /** Permanently deletes a trashed node and its subtree. */
    @PostMapping("/{id}/purge")
    public ResponseEntity<PurgeResponse> purge(Caller caller, @PathVariable long id,
                                               @RequestHeader(value = "If-Match", required = false)
                                               @Nullable String ifMatch) {
        Written<Long> r = tree.purge(caller, id, Http.requireIfMatch(ifMatch));
        return Http.withToken(ResponseEntity.ok(), r.token()).body(new PurgeResponse(r.value()));
    }

    @GetMapping("/{id}/restrictions")
    public RestrictionsResponse restrictions(Caller caller, @PathVariable long id,
                                             @RequestHeader(value = Http.TOKEN_HEADER, required = false)
                                             @Nullable String token) {
        return RestrictionsResponse.of(id, tree.restrictions(caller, id, Http.token(token)));
    }

    /** Replaces the node's {@code view} or {@code edit} restriction; an empty list removes it. */
    @PutMapping("/{id}/restrictions")
    public ResponseEntity<RestrictionsResponse> setRestrictions(Caller caller, @PathVariable long id,
                                                                @Valid @RequestBody RestrictionsRequest body) {
        Written<Map<RestrictionOp, List<String>>> r =
                tree.setRestrictions(caller, id, RestrictionOp.parse(body.op()), body.principals());
        return Http.withToken(ResponseEntity.ok(), r.token()).body(RestrictionsResponse.of(id, r.value()));
    }

    private static ResponseEntity<NodeResponse> written(ResponseEntity.BodyBuilder b, Written<Node> r) {
        return Http.withToken(b, r.token()).eTag(Http.etag(r.value())).body(NodeResponse.of(r.value()));
    }
}
