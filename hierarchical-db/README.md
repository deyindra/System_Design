# Hierarchical Database System (Atlassian system-design interview)

This is the tree store behind Confluence page trees and Jira Epic → Story → Sub-task, built for heavy concurrent reads
and writes.

| Document | Use it for |
|---|---|
| [docs/hierarchical-db-one-pager.html](docs/hierarchical-db-one-pager.html) | The 60-minute interview walkthrough: agenda, diagrams, data model, deep dives, follow-up questions. Open it in a browser; mermaid is bundled, so it works offline |
| [docs/DESIGN.md](docs/DESIGN.md) | The detailed design: requirements, estimates, model comparison, full DDL, API, read and write paths with SQL and Java snippets, concurrency races, sharding, failure modes |

**Core idea:**
- `parent_id` (the truth) plus a materialized path of ancestor ids (the index): hot reads are one index operation.
- A per-space advisory lock, shared for creates and exclusive for moves, rules out the create-vs-move and
  move-vs-move races.
- Large moves commit instantly through a path-rewrite overlay and finish in background batches.

## Run it

Requires JDK 17+, Maven and Docker.

```bash
docker run -d --name hierarchy-pg -p 5432:5432 \
  -e POSTGRES_DB=hierarchy -e POSTGRES_USER=hierarchy -e POSTGRES_PASSWORD=hierarchy postgres:16-alpine
mvn spring-boot:run          # port 8080; Flyway migrates the shard on startup
```

The defaults (`application.yml`) give one shard, no cache and in-process events. `--spring.profiles.active=prod`
(`application-prod.yml`) adds two shards with replicas, Redis and Kafka.

```bash
H=(-H 'X-Tenant-Id: 6f1c1a2e-0000-4000-8000-000000000001' -H 'X-Actor-Id: alice' -H 'X-Actor-Groups: eng'
   -H 'Content-Type: application/json')
curl -s "${H[@]}" localhost:8080/v1/spaces -d '{"key":"ENG","title":"Engineering"}'
#   {"id":"…","key":"ENG","rootNodeId":"<root>","treeVersion":0}
curl -s "${H[@]}" localhost:8080/v1/spaces/<space>/nodes -d '{"parentId":"<root>","title":"Architecture","type":"page"}'
curl -s "${H[@]}" localhost:8080/v1/nodes/<node>/move -d '{"newParentId":"<other>"}'
curl -s "${H[@]}" localhost:8080/v1/nodes/<node>/ancestors
curl -s "${H[@]}" 'localhost:8080/v1/nodes/<root>/descendants?depth=3'
curl -s localhost:8080/actuator/health
```

Ids are strings on the wire. Writes return an `ETag` (`"v<version>"`; send it back as `If-Match` on PATCH, trash,
restore and purge) and an `X-Read-Token`, which makes a following read see the write even on a replica.

**Tests:**
- `mvn test` runs the unit tests (no Docker).
- `mvn verify` adds the integration tests, which run on Testcontainers (`postgres:16-alpine`, `redis:7-alpine`,
  `cp-kafka:7.6.0`) and are skipped when Docker is absent.
  - They include the DESIGN.md §4.3 example and large moves behind the overlay, including a row locked mid-rewrite.
  - Racing creates and moves are checked by `TreeVerifier`, along with opposing moves and 32 × 50 concurrent appends.
  - The full HTTP API, the Redis cache, the Kafka relay and replica routing on real LSNs each have their own tests.

## Code map (`src/main/java/com/salesforce/einstein/hierarchy`)

| Package | What's there |
|---|---|
| `domain` | Records (`Node`, `Space`, `MoveJob`, `TreeEvent`, …), `Paths`, `Overlay`, `LexoRank`, `Cursors`, `TypeRules`, `ReadToken`, the exceptions |
| `spi` | `IdGenerator`, `TreeCache`, `EventPublisher`, `TreeEventListener` |
| `tenant` | `SnowflakeIdGenerator`, `TenantDirectory` (tenant → shard), `ShardRouter` |
| `store` | `TreeStore` (every SQL statement), `SpaceLocks` (advisory locks), `ReplicaRouter` (LSN-aware reads), `TreeVerifier`, `Db`, `Shard`, `JdbcSchema` |
| `service` | `TreeService` (reads, writes, moves, purge), `MoveJobWorker` (large-move batches with a lease), `AccessControl` |
| `cache` | `RedisTreeCache` (raise-only `tv` keys, MGET breadcrumbs), `NoopTreeCache` |
| `events` | `OutboxRelay`, `KafkaEventPublisher`, `InProcessEventPublisher`, `CacheInvalidator` |
| `api` | `SpaceController`, `NodeController`, `MoveJobController`, DTOs, `ApiExceptionHandler` (RFC 7807) |
| `config` | `HierarchyProperties`, `HierarchyConfiguration` (pools per shard and replica, cache and event wiring), `Schedules` |
