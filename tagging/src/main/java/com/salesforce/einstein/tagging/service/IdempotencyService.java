package com.salesforce.einstein.tagging.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.salesforce.einstein.tagging.domain.ConsistencyToken;
import com.salesforce.einstein.tagging.domain.IdempotencyConflictException;
import com.salesforce.einstein.tagging.domain.InvalidRequestException;
import com.salesforce.einstein.tagging.domain.WriteResult;
import com.salesforce.einstein.tagging.spi.IdempotencyRecord;
import com.salesforce.einstein.tagging.spi.TagStore;
import com.salesforce.einstein.tagging.spi.UnitOfWork;
import org.springframework.dao.DuplicateKeyException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Exactly-once effect for retried requests ({@code Idempotency-Key}). The key is checked, the work is
 * done and the response is saved in <b>one</b> transaction, so a crash can't leave work done without
 * its record.
 *
 * <p>The key is claimed (inserted) <i>before</i> the work. A concurrent request with the same key blocks on
 * the unique key until the first commits, then fails and returns the winner's saved response. It never
 * gets as far as allocating outbox seqs, so it can't leave a gap that stalls the shard's relay. Reusing a key with a different body is
 * a client bug (422).
 */
public final class IdempotencyService {
    private static final Pattern KEY = Pattern.compile("[A-Za-z0-9._:-]{8,128}");

    private final ObjectMapper json;
    private final Clock clock;

    public IdempotencyService(ObjectMapper json, Clock clock) {
        this.json = json;
        this.clock = clock;
    }

    /**
     * @param request a canonical form of the request body; its hash detects key reuse with another body
     */
    public <T> WriteResult<T> run(TagStore store, String tenantId, String key, Object request, Class<T> type,
                                  Function<UnitOfWork, WriteResult<T>> work) {
        if (key == null || !KEY.matcher(key).matches()) {
            throw new InvalidRequestException("Idempotency-Key header must match " + KEY.pattern());
        }
        String hash = sha256(write(request));
        try {
            return store.write(tenantId, uow -> {
                Optional<IdempotencyRecord> prior = uow.findIdempotency(key);
                if (prior.isPresent()) {
                    return replay(prior.get(), hash, type);
                }
                uow.saveIdempotency(new IdempotencyRecord(tenantId, key, hash, "", clock.instant()));
                WriteResult<T> result = work.apply(uow);
                uow.completeIdempotency(key, serialize(result));
                return result;
            });
        } catch (DuplicateKeyException race) {
            Optional<IdempotencyRecord> winner = store.write(tenantId, uow -> uow.findIdempotency(key));
            if (winner.isEmpty()) {
                throw race;   // a different unique key failed; not an idempotency race
            }
            return replay(winner.get(), hash, type);
        }
    }

    private <T> WriteResult<T> replay(IdempotencyRecord r, String hash, Class<T> type) {
        if (!r.requestHash().equals(hash)) {
            throw new IdempotencyConflictException("Idempotency-Key was already used with a different request");
        }
        try {
            JsonNode node = json.readTree(r.response());
            return new WriteResult<>(json.treeToValue(node.get("value"), type),
                    ConsistencyToken.parse(node.path("token").asText("")));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("corrupt idempotency record", e);
        }
    }

    private String serialize(WriteResult<?> result) {
        ObjectNode node = json.createObjectNode();
        node.set("value", json.valueToTree(result.value()));
        node.put("token", result.token().format());
        return write(node);
    }

    private String write(Object o) {
        try {
            return json.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String sha256(String s) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
