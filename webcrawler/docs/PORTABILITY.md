# Cloud portability: AWS, GCP, Azure or self-hosted

The crawler should move between AWS, GCP, Azure or a self-hosted Kubernetes cluster **without
changing the engine, the API or the design**. Two rules make that work:

1. **The design names capabilities, not products.** Every box in the architecture is "partitioned
   durable log", "object store", and so on, with a contract (below). Products appear in one place
   only: the mapping table in this document.
2. **The code depends on ports, not SDKs.** `CrawlEngine`, `CrawlResults` and the API see only Java
   interfaces. Each implementation lives in an adapter, chosen by configuration
   (`crawler.adapters.*`). Moving to another cloud means changing a dependency and a config file.

Contents: [Capabilities and contracts](#1-capabilities-and-contracts) ·
[Mapping](#2-mapping-to-providers) · [Where providers differ](#3-where-providers-really-differ) ·
[Ports in the code](#4-ports-in-the-code) · [Adapter modules](#5-adapter-modules) ·
[Switching provider](#6-switching-provider) · [Testing adapters](#7-testing-adapters) ·
[Rules](#8-rules-that-keep-it-portable)

---

## 1. Capabilities and contracts

| Capability | Used for | Contract the design relies on (an adapter must provide this) |
|---|---|---|
| **Partitioned durable log** | frontier (`crawl.frontier`; optionally one topic per priority band), `fetched` | partition by key (host); **exactly one consumer per partition** at a time; at-least-once delivery; offsets committed after processing; replay after a crash; pausing a partition |
| **Key-value cache with atomic scripts** | seen-sets, budgets, `pending` and task keys, scope keys, content claims, variant counts, robots/DNS cache, job-done signal | atomic multi-key script on keys sharing a hash tag; TTL; pub/sub (or polling as a fallback) |
| **Wide-column / KV store** | `job_pages`, `page_links`, `url_records`, `tenant_pages`, param rules, hosts | partition + clustering key range scans; TTL; **last-write-wins with a client-supplied timestamp** (or a conditional "newer than"): the CQL adapter uses it for the shortest depth and the newest content, so it needs no conditional write |
| **Relational database** | `crawl_jobs`, webhook outbox | unique constraints, `UPDATE … WHERE status = $from` (CAS), transactions |
| **Object store** | content-addressed blobs, exports | put/get/exists by key, strong read-after-write, multipart upload for large objects, **time-limited signed URLs**, lifecycle rules. *Not needed:* conditional put, listing, notifications |
| **CDN** | rewritten content, exports | cache by URL; origin = the results API; a separate sandbox domain |
| **Container platform** | API, fetchers, parsers, workers | Kubernetes: Deployments, HPA, PodDisruptionBudgets, NetworkPolicy |
| **Egress** | fetchers | NAT with stable public IPs (site owners allowlist us); no route to internal networks |
| **Secrets and workload identity** | DB passwords, webhook HMAC keys, cloud credentials | secrets mounted into pods; pods authenticate to cloud APIs without static keys |
| **Telemetry** | metrics, traces, logs | OpenTelemetry SDK → OTLP collector |

---

## 2. Mapping to providers

Pick one product per row. The **bold** choice speaks an open protocol, so its adapter is the same
on every cloud.

| Capability | Self-hosted (Kubernetes) | AWS | GCP | Azure |
|---|---|---|---|---|
| Partitioned durable log | **Apache Kafka** (Strimzi) / Redpanda | **Amazon MSK** | **Managed Service for Apache Kafka**, or Confluent Cloud | **Event Hubs (Kafka API)**, or Confluent Cloud |
| KV cache with scripts | **Redis / Valkey** | **ElastiCache / MemoryDB** | **Memorystore (Redis / Valkey)** | **Azure Cache for Redis** |
| Wide-column store | **Apache Cassandra / ScyllaDB** | **Amazon Keyspaces (CQL)**, or DynamoDB | **Cassandra/Scylla on GKE, Astra DB**, or Bigtable | **Cosmos DB for Apache Cassandra**, or Managed Instance for Cassandra |
| Relational | **PostgreSQL** | **RDS / Aurora PostgreSQL** | **Cloud SQL / AlloyDB for PostgreSQL** | **Azure Database for PostgreSQL** |
| Object store | MinIO / Ceph RGW | S3 | Cloud Storage | Blob Storage |
| CDN | Cloudflare / Fastly (cloud-neutral) | CloudFront | Cloud CDN | Front Door |
| Containers | Kubernetes | EKS | GKE | AKS |
| Egress NAT | gateway nodes | NAT Gateway + Elastic IPs | Cloud NAT + static IPs | NAT Gateway + public IP prefix |
| Secrets | Vault, External Secrets Operator | Secrets Manager | Secret Manager | Key Vault |
| Workload identity | Vault / SPIFFE | IRSA / EKS Pod Identity | GKE Workload Identity | Entra Workload ID |
| Telemetry | OTel Collector → Prometheus, Tempo, Loki | OTel → CloudWatch / AMP / X-Ray | OTel → Cloud Monitoring / Trace / Logging | OTel → Azure Monitor |
| Infrastructure as code | Terraform / OpenTofu + Helm | same, `aws` provider | same, `google` provider | same, `azurerm` provider |

**The result:** with the bold choices, the log, cache, wide-column and relational adapters are
*one* implementation each (Kafka client, Redis client, CQL driver, JDBC). Only the **object store**
(and signed URLs) needs one small adapter per cloud.

---

## 3. Where providers really differ

Mapping names is easy; semantics is where a port breaks. These are the differences that matter to
this design and how the design absorbs each one.

| If you choose… | What's different | How the design copes |
|---|---|---|
| **Pub/Sub, Kinesis or SQS** instead of a Kafka API for the frontier | Pub/Sub ordering keys give *ordering* but not *one consumer per key*. SQS FIFO message groups are close but cap throughput. Kinesis shards are exclusive but resharding is manual | Politeness assumes the log's exclusive ownership of a host. Without it, the `Frontier` adapter must add a **per-host lease** in the KV cache (`SET host-lease:{h} NX PX 30000`), and a worker that can't get the lease nacks the message with a delay. The design works either way; it's an adapter concern. Prefer a Kafka API, which every provider offers |
| **DynamoDB** instead of CQL | conditional writes (`attribute_not_exists`) instead of LWT; 400 KB item limit; query by partition + sort key; TTL is approximate (up to days late) | `JobStore` needs only "insert if absent" and "update if status = QUEUED", and both map directly. Bucketed partitions keep hot keys spread. TTL is for cleanup only, never correctness |
| **Bigtable** instead of CQL | single-row atomicity via `CheckAndMutateRow`; no secondary indexes; row key design is everything | row key `job_id#bucket#url_hash`; `job_pages_by_order` becomes a second table keyed `job_id#chunk#seq`; CAS uses check-and-mutate |
| **Cosmos DB** (Cassandra API) | request units; LWT supported with caveats; per-partition limit 20 GB | the same bucketing; budget RU for the write-heavy `page_links` |
| **Object stores** | all three are strongly consistent for read-after-write. Signed-URL mechanics differ (S3 presign, GCS V4 signed URL, Azure SAS). Multipart: S3 parts ≥ 5 MB; GCS resumable uploads; Azure block blobs | content addressing needs no conditional put, so the semantics are the same. Signed URLs go behind a port (`SignedUrls`, [§4](#4-ports-in-the-code)). Our API never returns raw bucket URLs, so clients don't see the provider |
| **Redis-compatible services** | cluster mode and `EVAL` are supported by ElastiCache, MemoryDB, Memorystore and Azure Cache, but some tiers restrict commands; pub/sub in cluster mode fans out to all shards | Lua only touches keys with the same `{job}` hash tag; `await` falls back to polling job status if pub/sub is unavailable |
| **Managed Kafka variants** | Event Hubs: no compaction on some tiers, a partition limit per namespace; MSK Serverless: partition and throughput caps | the frontier needs neither compaction nor more than ~100 partitions |
| **NAT and egress** | IP allocation, cost per GB and port exhaustion limits differ | fetcher connection pools per host keep the port count bounded; NAT IPs are published for allowlisting |

---

## 4. Ports in the code

| Port (interface) | Built in | Distributed adapter | Cloud-specific? |
|---|---|---|---|
| `Frontier` | `memory` (`InMemoryFrontier`) | **built** · `kafka` (`KafkaFrontier`): produce keyed by host; each consumer runs `InMemoryFrontier` over its partitions | no (Kafka API) |
| `JobStore` | `memory` | **built** · `redis-cql` (`RedisCqlJobStore`): admit/tasks/counters in Redis Lua; nodes, edges and `tenant_pages` in CQL; job rows in PostgreSQL | no |
| `HostSchedule` (politeness across a handover) | `InMemoryHostSchedule` | **built** · `RedisHostSchedule`, registered by the `redis-cql` module | no |
| `PageStore` | `memory` | *planned* `cql` (`url_records`) | no |
| `ContentStore` | `memory`, **`filesystem`** | `object-store`: one small class per provider (S3, GCS, Azure Blob), or the S3 API for MinIO/Ceph | **yes, the only one** |
| `SitemapGraphs` (sitemap crawls) | `memory` (`InMemorySitemapGraphs`) | **built** · `age` (`AgeSitemapGraphs`), `neo4j` (`Neo4jSitemapGraphs`): one named graph per sitemap, read through graphexecutor's `GraphStore`, and a catalog of the graphs loaded through `/v1/sitemap-graphs` | no |
| `Fetcher` | `HttpFetcher` behind `EgressFilteringFetcher` (SSRF check) | the same, plus an egress proxy or NAT with no internal routes | no |
| *planned* `SignedUrls` | — | S3 presign / GCS signed URL / Azure SAS / a local HMAC-signed API link | yes |
| *planned* `JobEvents` (job-done) | in-process listener | Redis pub/sub, or the outbox table | no |

`ContentStore` was kept deliberately small (`put`, `get`, `contains`) so a cloud adapter is about
40 lines. Every adapter uses the shared `ContentStore.key(hash)` layout (`ab/abcdef…`), so data can
be copied between providers with `rclone` or a storage transfer service, without re-keying.

---

## 5. Adapter modules

```
webcrawler                     core: engine, API, ports, memory + filesystem adapters   (this module)
webcrawler-adapter-kafka       built    Frontier over the Kafka API     → MSK, Event Hubs, GCP managed Kafka, Strimzi, Redpanda
webcrawler-adapter-redis-cql   built    JobStore + HostSchedule over Redis + CQL + JDBC (PageStore planned)
webcrawler-adapter-graph-age   built    SitemapGraphs (graphs + catalog) in Apache AGE (PostgreSQL)
webcrawler-adapter-graph-neo4j built    SitemapGraphs (graphs + catalog) in Neo4j
webcrawler-it                  built    two Spring Boot nodes on Kafka + Redis + Cassandra + Postgres/AGE (tests only)
webcrawler-adapter-s3          planned  ContentStore + SignedUrls, AWS SDK v2 (also MinIO/Ceph via endpoint override)
webcrawler-adapter-gcs         planned  ContentStore + SignedUrls, google-cloud-storage
webcrawler-adapter-azure-blob  planned  ContentStore + SignedUrls, azure-storage-blob
```

The built adapters are configured under their own prefixes:

| Property | Default | Notes |
|---|---|---|
| `crawler.kafka.bootstrap-servers` | `localhost:9092` | |
| `crawler.kafka.topic` / `partitions` / `replication-factor` | `crawl.frontier` / 64 / 3 | `partitions` is fixed for the topic's life: a host's partition is `murmur2(host) % partitions` |
| `crawler.kafka.create-topic` | `true` | off where topics are managed by IaC |
| `crawler.kafka.group-id` / `client-id` | `webcrawler` / `webcrawler-node` | `client-id` = the node's host name in production |
| `crawler.kafka.session-timeout` | `45s` | failover time for a crashed node |
| `crawler.kafka.commit-interval` | `1s` | |
| `crawler.kafka.fetch-lease` | `30s` | ≥ the fetch timeout: how long a new owner waits for an old owner's request in flight |
| `crawler.kafka.max-buffered` | `10000` | back queues full → pause the partitions |
| `crawler.kafka.extra.*` | | passed to the producer and consumer (SASL, TLS, IAM auth) |
| `crawler.redis-cql.redis-uri` | `redis://localhost:6379` | `rediss://` for TLS; keys are hash-tagged, so Redis Cluster works |
| `crawler.redis-cql.cassandra-contact-points` / `cassandra-local-datacenter` | `localhost:9042` / `datacenter1` | reads and writes at `LOCAL_QUORUM` |
| `crawler.redis-cql.keyspace` / `replication` | `webcrawler` / SimpleStrategy RF 1 | use `NetworkTopologyStrategy` in production |
| `crawler.redis-cql.jdbc-url` / `jdbc-user` / `jdbc-password` / `jdbc-pool-size` | local Postgres / 10 | the pool is private to the store, not a `DataSource` bean |
| `crawler.redis-cql.create-schema` | `true` | creates the keyspace, tables and `crawl_jobs` if missing |
| `crawler.redis-cql.hot-state-ttl` | `1d` | a finished job's Redis keys expire after this |
| `crawler.redis-cql.job-cache-ttl` | `1s` | staleness of a running job's status on a node, which bounds how long a cancel takes to reach every worker |
| `crawler.adapters.sitemap-graph` | `memory` | `age` or `neo4j` with the matching module; `crawler.sitemap.shards` (16) must match a bulk import's `shard`s; `crawler.sitemap.load-threads` (2) API loads at once per node |
| `crawler.sitemap.age.url` / `user` / `password` / `pool-size` / `batch-size` | local Postgres / `postgres` / — / 8 / 1000 | |
| `crawler.sitemap.neo4j.uri` / `user` / `password` / `database` / `batch-size` | `bolt://localhost:7687` / `neo4j` / — / `neo4j` / 500 | |

> **Boot's Cassandra auto-configuration.** The CQL driver on the classpath activates Boot's
> `CassandraAutoConfiguration`. When `job-store: redis-cql` is selected, this module's `CqlSession`
> is registered first and Boot's backs off. If the jar is on the classpath but **not selected**,
> Boot will try to connect to `localhost:9042` at startup. In that case, exclude it with
> `spring.autoconfigure.exclude: org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration`.

Each module ships a Spring Boot auto-configuration (listed in
`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`) that registers
its beans **only when its value is selected**. For the GCS adapter:

```java
@AutoConfiguration
@ConditionalOnProperty(name = "crawler.adapters.content-store", havingValue = "object-store")
@ConditionalOnClass(Storage.class)
class GcsContentStoreAutoConfiguration {
    @Bean ContentStore contentStore(@Value("${crawler.object-store.bucket}") String bucket) {
        return new GcsContentStore(StorageOptions.getDefaultInstance().getService(), bucket);
    }
}

final class GcsContentStore implements ContentStore {
    private final Storage storage; private final String bucket;
    GcsContentStore(Storage storage, String bucket) { this.storage = storage; this.bucket = bucket; }

    @Override public boolean put(String hash, byte[] bytes) {
        if (contains(hash)) return false;                       // content-addressed: existing = identical
        storage.create(BlobInfo.newBuilder(bucket, ContentStore.key(hash)).build(), bytes);
        return true;
    }
    @Override public Optional<byte[]> get(String hash) {
        Blob b = storage.get(bucket, ContentStore.key(hash));
        return b == null ? Optional.empty() : Optional.of(b.getContent());
    }
    @Override public boolean contains(String hash) { return storage.get(bucket, ContentStore.key(hash)) != null; }
}
```

The S3 version has the same shape: `headObject` for `contains`, `putObject`, `getObjectAsBytes`.
Credentials come from each SDK's default chain (workload identity in the cluster), so no keys or
ARNs appear in the code or the config schema.

---

## 6. Switching provider

The only things that change are the adapter dependency (a Maven profile or image variant) and a
Spring profile:

```yaml
# application-aws.yml
crawler:
  adapters: { frontier: kafka, job-store: redis-cql, content-store: object-store }
  object-store: { bucket: crawl-blobs-prod }
  kafka:
    bootstrap-servers: b-1.msk.internal:9098
    extra: { security.protocol: SASL_SSL, sasl.mechanism: AWS_MSK_IAM }
  redis-cql:
    redis-uri: rediss://crawl.cache.amazonaws.com:6379
    cassandra-contact-points: [cassandra.us-east-1.amazonaws.com:9142]   # Keyspaces
    jdbc-url: jdbc:postgresql://crawl.cluster.rds.amazonaws.com/webcrawler
```

```yaml
# application-gcp.yml
crawler:
  adapters: { frontier: kafka, job-store: redis-cql, content-store: object-store }
  object-store: { bucket: crawl-blobs-prod }
  kafka: { bootstrap-servers: bootstrap.crawl.us-central1.managedkafka.my-project.cloud.goog:9092 }
  redis-cql: { redis-uri: redis://10.0.0.3:6379, jdbc-url: jdbc:postgresql://10.0.0.5/webcrawler }   # Memorystore, Cloud SQL
```

```yaml
# application-local.yml: one process, no cloud at all
crawler:
  adapters: { content-store: filesystem, content-root: ./data/blobs }   # the rest default to memory
```

Unchanged by the switch: `CrawlEngine`, every API contract, the data model (the same tables, keys
and partitioning), the diagrams, and the tests (the same contract tests run against each adapter).

Infrastructure follows the same pattern. One Terraform module interface (`log`, `cache`,
`wide_column`, `relational`, `object_store`, `cluster`) has one implementation per provider. The
Helm chart is identical; only its values file differs.

---

## 7. Testing adapters

- **Contract tests**: `ContentStoreContract` defines the behaviour every `ContentStore` must have
  (round trip, idempotent put, defensive copies, concurrent writers, key layout).
  `InMemoryContentStoreTest` and `FileSystemContentStoreTest` extend it, and an S3 or GCS module
  adds one subclass. `JobStoreContract` (15 tests, including admit and close-task
  races under 16 and 8 threads) is extended by `InMemoryJobStoreTest` in the core and by
  `RedisCqlJobStoreTest` on real Redis, Cassandra and Postgres. `SitemapGraphsContract` (9 tests) is
  extended by `InMemorySitemapGraphsTest`, `AgeSitemapGraphsTest` and `Neo4jSitemapGraphsTest`. The
  core publishes these contracts in its `test-jar`.
- **The frontier** is tested by behaviour rather than by a contract class. `KafkaFrontierTest` runs
  the scenarios of `DistributedCrawlTest` on a real broker: three nodes in one group, a node
  killed mid-fetch, and a host's delay surviving its partition moving.
- **End to end**: `TwoNodeCrawlTest` (`webcrawler-it`) starts the unchanged application twice and
  selects the adapters by configuration only, including sitemap jobs over AGE.
- **Emulators** (Testcontainers) let adapters be tested without a cloud account. In use:
  `cp-kafka`, `redis`, `cassandra`, `postgres`, `apache/age`, `neo4j`. For the planned adapters: MinIO or LocalStack (S3),
  fake-gcs-server (GCS), Azurite (Blob). The tests are skipped when Docker isn't available.
- **Adapter selection**: `AdapterSelectionTest` proves a config value alone switches the
  implementation.

---

## 8. Rules that keep it portable

1. No vendor SDK type crosses a port. Adapters translate errors into the port's terms
   (`UncheckedIOException`, `Optional.empty()`).
2. Nothing vendor-only on the critical path: no S3 event notifications, DynamoDB Streams or Pub/Sub
   push triggering crawl steps. Work moves through the log, which every provider offers.
3. Clients see only our URLs (`/v1/crawls/...`) and our signed links, never bucket names, ARNs or
   provider hostnames.
4. Identity comes from the platform (workload identity) through the SDK's default chain. Static keys
   and account ids never appear in the code or config schema.
5. Prefer open wire protocols (Kafka, Redis, CQL, PostgreSQL) over proprietary APIs where the
   capability is the same, so the adapter count stays at one.
6. Every adapter passes the same contract tests, so "it works on AWS" and "it works on GCP" mean
   the same thing.
7. Telemetry goes through OpenTelemetry only; backends are a collector config change.
