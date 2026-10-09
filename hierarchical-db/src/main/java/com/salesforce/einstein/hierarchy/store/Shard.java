package com.salesforce.einstein.hierarchy.store;

import com.salesforce.einstein.hierarchy.domain.ReadToken;

/** One Postgres cluster: a primary for writes and strict reads, replicas for the rest. */
public record Shard(String name, Db primary, ReplicaRouter replicas) {

    /** A token issued by another shard (the tenant was moved, or it was forged) is ignored. */
    public Db forRead(ReadToken token) {
        return replicas.forRead(token.shard().equals(name) ? token : ReadToken.NONE);
    }

    /** Call once a write has committed: its commit record is at or before this position. */
    public ReadToken tokenNow() {
        return new ReadToken(name, primary.currentLsn());
    }
}
