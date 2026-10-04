package com.salesforce.einstein.tagging.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.salesforce.einstein.tagging.domain.Consistency;
import com.salesforce.einstein.tagging.domain.EntityRef;
import com.salesforce.einstein.tagging.domain.EntityTags;
import com.salesforce.einstein.tagging.domain.Tag;
import com.salesforce.einstein.tagging.domain.Trending;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.List;

/**
 * Wire types. Tag ids are 64-bit, beyond JavaScript's 2^53 safe-integer range, so responses send
 * them as <b>strings</b>. Requests accept either strings or numbers.
 */
public final class Dtos {
    private Dtos() {
    }

    public record CreateTagRequest(@NotBlank @Size(max = 256) String name, @Size(max = 32) String color) {
    }

    public record UpdateTagRequest(@Size(max = 256) String name, @Size(max = 32) String color) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record TagResponse(String id, String name, String color, long version, Long usage,
                              String createdBy, Instant createdAt, Instant updatedAt) {
        public static TagResponse of(Tag t, Long usage) {
            return new TagResponse(Long.toString(t.tagId()), t.name(), t.color(), t.version(), usage,
                    t.createdBy(), t.createdAt(), t.updatedAt());
        }
    }

    public record TagPageResponse(List<TagResponse> items, String nextCursor) {
    }

    public record EntityRefDto(@NotBlank String type, @NotBlank String id) {
        public EntityRef toRef() {
            return new EntityRef(type, id);
        }

        public static EntityRefDto of(EntityRef e) {
            return new EntityRefDto(e.type(), e.id());
        }
    }

    public record EntityTagsResponse(String entityType, String entityId, List<TagResponse> tags) {
        public static EntityTagsResponse of(EntityTags et) {
            return new EntityTagsResponse(et.entity().type(), et.entity().id(),
                    et.tags().stream().map(t -> TagResponse.of(t, null)).toList());
        }
    }

    public record AttachRequest(@Size(max = 100) List<@NotNull Long> tagIds,
                                @Size(max = 100) List<@NotBlank String> tagNames) {
    }

    public record TagIdsRequest(@NotNull @Size(max = 100) List<@NotNull Long> tagIds) {
    }

    public record BulkAttachRequest(@NotEmpty @Size(max = 500) List<@Valid @NotNull EntityRefDto> entities,
                                    @NotEmpty @Size(max = 50) List<@NotNull Long> tagIds) {
    }

    public record SearchRequest(@Size(max = 32) List<@NotNull Long> all,
                                @Size(max = 32) List<@NotNull Long> any,
                                @Size(max = 32) List<@NotNull Long> none,
                                String entityType, Integer limit, String cursor, Consistency consistency) {
    }

    /** Trending tags. {@code consistency} is always EVENTUAL: the counts trail writes by the pipeline lag. */
    public record TrendingResponse(String window, String rank, Consistency consistency, Instant asOf,
                                   List<TrendingItem> items) {
        public static TrendingResponse of(Trending t) {
            return new TrendingResponse(t.window().wire(), t.rank().name(), Consistency.EVENTUAL, t.asOf(),
                    t.items().stream().map(i -> new TrendingItem(TagResponse.of(i.tag(), null), i.count(),
                            i.previousCount())).toList());
        }
    }

    public record TrendingItem(TagResponse tag, long count, long previousCount) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record EntityPageResponse(List<EntityRefDto> items, String nextCursor, Long total) {
    }
}
