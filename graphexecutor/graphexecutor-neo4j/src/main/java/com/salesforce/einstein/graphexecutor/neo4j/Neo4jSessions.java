package com.salesforce.einstein.graphexecutor.neo4j;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.TransactionContext;
import org.neo4j.driver.exceptions.ClientException;

import java.util.Map;
import java.util.Objects;

/**
 * One database of a {@link Driver}. The driver is thread-safe and pools connections; sessions are not, so
 * every call opens its own. Reads and writes are managed transactions: the driver retries them on transient
 * errors (a deadlock, a leader switch), so their work must be safe to repeat, which every store's is.
 */
public final class Neo4jSessions {

    private static final String EQUIVALENT_SCHEMA_RULE = "Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists";

    private final Driver driver;
    private final SessionConfig config;

    public Neo4jSessions(Driver driver, String database) {
        this.driver = Objects.requireNonNull(driver, "driver");
        this.config = SessionConfig.forDatabase(Objects.requireNonNull(database, "database"));
    }

    public <R> R read(TransactionCallback<R> work) {
        try (Session session = driver.session(config)) {
            return session.executeRead(work);
        }
    }

    public <R> R write(TransactionCallback<R> work) {
        try (Session session = driver.session(config)) {
            return session.executeWrite(work);
        }
    }

    /**
     * Runs schema statements ({@code CREATE CONSTRAINT … IF NOT EXISTS}), each in its own transaction. Two
     * workers starting at once race on them, and {@code IF NOT EXISTS} does not stop the loser failing with
     * {@value #EQUIVALENT_SCHEMA_RULE}; the rule it wanted exists then, so that failure is success.
     */
    public void schema(String... statements) {
        for (String statement : statements) {
            try {
                write(tx -> tx.run(statement).consume());
            } catch (ClientException e) {
                if (!EQUIVALENT_SCHEMA_RULE.equals(e.code())) {
                    throw e;
                }
            }
        }
    }

    /**
     * Takes the write lock of the nodes {@code match} finds (bound to {@code n}), until the transaction ends.
     * Neo4j reads without locks, so a check-then-write (is the lease still mine? then arrive) must lock first,
     * or a concurrent transaction can change what was checked before the write commits.
     */
    static void lock(TransactionContext tx, String match, Map<String, Object> params) {
        tx.run(match + " SET n._lock = true REMOVE n._lock", params).consume();
    }
}
