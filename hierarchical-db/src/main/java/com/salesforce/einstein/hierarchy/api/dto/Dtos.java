package com.salesforce.einstein.hierarchy.api.dto;

import com.salesforce.einstein.hierarchy.domain.Crumb;
import com.salesforce.einstein.hierarchy.domain.MoveJob;
import com.salesforce.einstein.hierarchy.domain.Node;
import com.salesforce.einstein.hierarchy.domain.Page;
import com.salesforce.einstein.hierarchy.domain.RestrictionOp;
import com.salesforce.einstein.hierarchy.domain.Space;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.lang.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Request and response bodies. Ids are strings on the wire: 64-bit Snowflake ids don't fit a JavaScript number.
 */
public final class Dtos {
    private static final String ID = "\\d{1,19}";

    private Dtos() {
    }

    // ------------------------------------------------------------------ requests

    public record CreateSpaceRequest(@NotBlank @Size(max = 16) String key, @NotBlank @Size(max = 255) String title) {
    }

    public record CreateNodeRequest(@NotNull @Pattern(regexp = ID) String parentId,
                                    @NotBlank @Size(max = 255) String title,
                                    @NotBlank @Size(max = 32) String type,
                                    @Nullable @Pattern(regexp = ID) String afterId,
                                    @Nullable @Pattern(regexp = ID) String beforeId,
                                    @Nullable @Size(max = 1_000_000) String body) {
    }

    public record UpdateNodeRequest(@Nullable @Size(max = 255) String title,
                                    @Nullable @Size(max = 1_000_000) String body) {
    }

    public record MoveNodeRequest(@NotNull @Pattern(regexp = ID) String newParentId,
                                  @Nullable @Pattern(regexp = ID) String afterId,
                                  @Nullable @Pattern(regexp = ID) String beforeId) {
    }

    public record RestrictionsRequest(@NotBlank String op, @NotNull @Size(max = 100) List<String> principals) {
    }

    // ------------------------------------------------------------------ responses

    public record SpaceResponse(String id, String key, String rootNodeId, long treeVersion) {
        public static SpaceResponse of(Space s) {
            return new SpaceResponse(Long.toString(s.id()), s.key(), Long.toString(s.rootNodeId()), s.treeVersion());
        }
    }

    public record NodeResponse(String id, String spaceId, @Nullable String parentId, String path, int depth,
                               String rank, String type, String title, String status, long version, String createdBy,
                               Instant createdAt, Instant updatedAt, @Nullable Boolean inTrash, @Nullable String body) {
        public static NodeResponse of(Node n) {
            return of(n, null, null);
        }

        public static NodeResponse of(Node n, @Nullable Boolean inTrash, @Nullable String body) {
            return new NodeResponse(Long.toString(n.id()), Long.toString(n.spaceId()),
                    n.parentId() == null ? null : Long.toString(n.parentId()), n.path(), n.depth(), n.rank(), n.type(),
                    n.title(), n.status().name(), n.version(), n.createdBy(), n.createdAt(), n.updatedAt(), inTrash,
                    body);
        }
    }

    public record CrumbResponse(String id, String title, String type, String status, int depth) {
        public static CrumbResponse of(Crumb c) {
            return new CrumbResponse(Long.toString(c.id()), c.title(), c.type(), c.status().name(), c.depth());
        }
    }

    public record PageResponse<T>(List<T> items, @Nullable String nextCursor) {
        public static <A, T> PageResponse<T> of(Page<A> page, Function<A, T> map) {
            return new PageResponse<>(page.items().stream().map(map).toList(), page.nextCursor());
        }
    }

    public record MoveJobResponse(String id, String spaceId, String rootNodeId, String oldPrefix, String newPrefix,
                                  String state, long rowsDone, Instant createdAt, @Nullable Instant finishedAt) {
        public static MoveJobResponse of(MoveJob j) {
            return new MoveJobResponse(Long.toString(j.id()), Long.toString(j.spaceId()), Long.toString(j.rootNodeId()),
                    j.oldPrefix(), j.newPrefix(), j.state().name(), j.rowsDone(), j.createdAt(), j.finishedAt());
        }
    }

    /** 200 with {@code job} null when the move is complete; 202 with the background job otherwise. */
    public record MoveResponse(NodeResponse node, @Nullable MoveJobResponse job) {
    }

    public record PurgeResponse(long purged) {
    }

    public record RestrictionsResponse(String nodeId, Map<String, List<String>> restrictions) {
        public static RestrictionsResponse of(long nodeId, Map<RestrictionOp, List<String>> r) {
            Map<String, List<String>> out = new TreeMap<>();
            r.forEach((op, ps) -> out.put(op.db(), ps));
            return new RestrictionsResponse(Long.toString(nodeId), out);
        }
    }
}
