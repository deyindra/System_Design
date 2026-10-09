package com.salesforce.einstein.hierarchy.store;

import com.salesforce.einstein.hierarchy.domain.ReadToken;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.Objects;
import java.util.function.Function;

/**
 * One Postgres server (a shard primary or one of its replicas) with its two kinds of transaction.
 *
 * <ul>
 *   <li>{@link #write}: READ COMMITTED. Writers serialize on advisory locks and OCC, not on isolation level.</li>
 *   <li>{@link #read}: REPEATABLE READ, read only. A read runs several statements (overlay, node, ancestors) and they
 *       must all see one snapshot, or a move committing in between could show a half-moved tree.</li>
 * </ul>
 */
public final class Db {
    private final String name;
    private final DataSource dataSource;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate writeTx;
    private final TransactionTemplate readTx;

    public Db(String name, DataSource dataSource) {
        this.name = name;
        this.dataSource = dataSource;
        this.jdbc = new JdbcTemplate(dataSource);
        DataSourceTransactionManager tm = new DataSourceTransactionManager(dataSource);
        this.writeTx = new TransactionTemplate(tm);
        this.writeTx.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.readTx = new TransactionTemplate(tm);
        this.readTx.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        this.readTx.setReadOnly(true);
    }

    public String name() {
        return name;
    }

    public DataSource dataSource() {
        return dataSource;
    }

    public JdbcTemplate jdbc() {
        return jdbc;
    }

    public <T> T write(Function<JdbcTemplate, T> work) {
        return Objects.requireNonNull(writeTx.execute(status -> work.apply(jdbc)));
    }

    public <T> T read(Function<JdbcTemplate, T> work) {
        return Objects.requireNonNull(readTx.execute(status -> work.apply(jdbc)));
    }

    /** The WAL write position. Taken after a commit, it is at or past that commit's record. */
    public long currentLsn() {
        return ReadToken.parseLsn(Objects.requireNonNull(
                jdbc.queryForObject("SELECT pg_current_wal_lsn()::text", String.class)));
    }

    /** How far this server has replayed; a primary (no replay) reports its own write position. */
    public long replayLsn() {
        return ReadToken.parseLsn(Objects.requireNonNull(jdbc.queryForObject(
                "SELECT coalesce(pg_last_wal_replay_lsn(), pg_current_wal_lsn())::text", String.class)));
    }
}
