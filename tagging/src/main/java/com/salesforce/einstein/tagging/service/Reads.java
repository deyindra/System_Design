package com.salesforce.einstein.tagging.service;

import com.salesforce.einstein.tagging.config.TaggingProperties;
import com.salesforce.einstein.tagging.domain.Consistency;
import com.salesforce.einstein.tagging.domain.ConsistencyToken;
import com.salesforce.einstein.tagging.domain.InvalidRequestException;
import com.salesforce.einstein.tagging.domain.Tag;
import com.salesforce.einstein.tagging.domain.TenantInfo;
import com.salesforce.einstein.tagging.spi.ReadPreference;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Shared read-path rules. */
final class Reads {
    private Reads() {
    }

    /**
     * Maps the client's consistency choice to a store read preference.
     * <ul>
     *   <li>No level given: STRONG. Weaker levels are an explicit opt-in.</li>
     *   <li>SESSION without a token is EVENTUAL, because there's nothing to wait for.</li>
     *   <li>A token from another shard means the tenant moved since the write, so only the primary is safe.</li>
     * </ul>
     */
    static ReadPreference preference(TenantInfo t, Consistency c, ConsistencyToken token) {
        ConsistencyToken tok = token == null ? ConsistencyToken.NONE : token;
        Consistency level = c != null ? c : Consistency.STRONG;
        return switch (level) {
            case STRONG -> ReadPreference.strong();
            case EVENTUAL -> ReadPreference.eventual();
            case SESSION -> tok.isNone() ? ReadPreference.eventual()
                    : tok.shard().equals(t.shardId()) ? ReadPreference.session(tok.seq()) : ReadPreference.strong();
        };
    }

    static ConsistencyToken token(TenantInfo t, long seq) {
        return seq <= 0 ? ConsistencyToken.NONE : new ConsistencyToken(t.shardId(), seq);
    }

    static int pageSize(Integer requested, TaggingProperties.Limits limits) {
        if (requested == null) {
            return limits.defaultPageSize();
        }
        if (requested <= 0) {
            throw new InvalidRequestException("limit must be positive");
        }
        return Math.min(requested, limits.maxPageSize());
    }

    /** Live tags among {@code ids}, ordered by normalized name (display order). */
    static List<Tag> live(Collection<Long> ids, Map<Long, Tag> byId) {
        return ids.stream().map(byId::get).filter(Objects::nonNull).filter(t -> !t.deleted())
                .sorted(Comparator.comparing(Tag::nameNorm)).toList();
    }
}
