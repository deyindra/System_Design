package com.salesforce.einstein.graphexecutor.neo4j;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.TransactionContext;
import org.neo4j.driver.exceptions.ClientException;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Against a fake driver, whose transactions record the queries they run and fail the ones they are told to. */
class Neo4jSessionsTest {

    private static final String EQUIVALENT = "Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists";

    /** What the fake saw: the queries run, the database of each session, and sessions still open. */
    private static final class FakeDriver {
        final List<String> queries = new ArrayList<>();
        final List<Optional<String>> databases = new ArrayList<>();
        final Map<String, ClientException> failures;
        int open;

        FakeDriver(Map<String, ClientException> failures) {
            this.failures = failures;
        }

        Driver driver() {
            return proxy(Driver.class, (name, args) -> switch (name) {
                case "session" -> {
                    databases.add(((SessionConfig) args[0]).database());
                    open++;
                    yield session();
                }
                case "close" -> null;
                default -> throw new UnsupportedOperationException(name);
            });
        }

        private Session session() {
            return proxy(Session.class, (name, args) -> switch (name) {
                case "executeRead", "executeWrite" -> ((TransactionCallback<?>) args[0]).execute(transaction());
                case "close" -> {
                    open--;
                    yield null;
                }
                default -> throw new UnsupportedOperationException(name);
            });
        }

        private TransactionContext transaction() {
            return proxy(TransactionContext.class, (name, args) -> {
                if (!name.equals("run")) {
                    throw new UnsupportedOperationException(name);
                }
                String query = (String) args[0];
                queries.add(query);
                ClientException failure = failures.get(query);
                if (failure != null) {
                    throw failure;
                }
                return proxy(Result.class, (method, ignored) -> null);   // consume(): its summary is not read
            });
        }
    }

    @FunctionalInterface
    private interface Handler {
        Object invoke(String method, Object[] args);
    }

    private static <T> T proxy(Class<T> type, Handler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (self, method, args) -> handler.invoke(method.getName(), args == null ? new Object[0] : args)));
    }

    @Test
    void everyCallOpensAndClosesASessionOnTheDatabase() {
        FakeDriver fake = new FakeDriver(Map.of());
        Neo4jSessions sessions = new Neo4jSessions(fake.driver(), "graphs");

        assertEquals("read", sessions.read(tx -> "read"));
        assertEquals("written", sessions.write(tx -> "written"));
        assertEquals(List.of(Optional.of("graphs"), Optional.of("graphs")), fake.databases);
        assertEquals(0, fake.open);
    }

    @Test
    void schemaTreatsAnEquivalentRuleAsCreated() {
        FakeDriver fake = new FakeDriver(Map.of("CREATE A", new ClientException(EQUIVALENT, "raced")));
        new Neo4jSessions(fake.driver(), "neo4j").schema("CREATE A", "CREATE B");

        assertEquals(List.of("CREATE A", "CREATE B"), fake.queries, "the next statement still runs");
        assertEquals(0, fake.open);
    }

    @Test
    void schemaRethrowsAnyOtherError() {
        ClientException syntax = new ClientException("Neo.ClientError.Statement.SyntaxError", "bad");
        FakeDriver fake = new FakeDriver(Map.of("CREATE A", syntax));

        assertSame(syntax, assertThrows(ClientException.class,
                () -> new Neo4jSessions(fake.driver(), "neo4j").schema("CREATE A", "CREATE B")));
        assertEquals(List.of("CREATE A"), fake.queries, "stops at the failure");
        assertEquals(0, fake.open);
    }

    @Test
    void rejectsAMissingDriverOrDatabase() {
        assertThrows(NullPointerException.class, () -> new Neo4jSessions(null, "neo4j"));
        try (Driver driver = new FakeDriver(Map.of()).driver()) {
            assertThrows(NullPointerException.class, () -> new Neo4jSessions(driver, null));
        }
    }
}
