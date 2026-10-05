package com.salesforce.einstein.webcrawler.adapter.rediscql;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.List;

/**
 * {@code crawler.redis-cql.*}.
 *
 * @param redisUri            e.g. {@code redis://host:6379}; every key of a job shares the hash tag {@code {jobId}},
 *                            so the scripts also run on Redis Cluster
 * @param cassandraContactPoints {@code host:port} list
 * @param replication         CQL replication map for {@code CREATE KEYSPACE} when {@code createSchema}
 * @param createSchema        create the keyspace, tables and {@code crawl_jobs} if missing (off where schema is
 *                            managed by migrations)
 * @param hotStateTtl         how long a terminal job's Redis keys live; reads then fall back to Cassandra/Postgres
 * @param jobCacheTtl         how stale a running job's status may be on this node ({@code process()} reads it per task);
 *                            bounds how long a cancel takes to reach every worker
 */
@ConfigurationProperties("crawler.redis-cql")
public record RedisCqlSettings(
        @DefaultValue("redis://localhost:6379") String redisUri,
        @DefaultValue("localhost:9042") List<String> cassandraContactPoints,
        @DefaultValue("datacenter1") String cassandraLocalDatacenter,
        @DefaultValue("webcrawler") String keyspace,
        @DefaultValue("{'class': 'SimpleStrategy', 'replication_factor': 1}") String replication,
        @DefaultValue("jdbc:postgresql://localhost:5432/webcrawler") String jdbcUrl,
        @DefaultValue("webcrawler") String jdbcUser,
        @DefaultValue("") String jdbcPassword,
        @DefaultValue("10") int jdbcPoolSize,
        @DefaultValue("true") boolean createSchema,
        @DefaultValue("1d") Duration hotStateTtl,
        @DefaultValue("1s") Duration jobCacheTtl) {
}
