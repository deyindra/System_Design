package com.salesforce.einstein.tagging.spi;

import com.salesforce.einstein.tagging.domain.EntityRef;

/** An entity plus its store-assigned dense id, the keyset for pagination and the posting id in the index. */
public record EntityHit(long entitySeq, EntityRef entity) {
}
