# webcrawler

A distributed web crawler / URL scraping service: a working **low-level design** (single process,
Spring Boot API) whose interfaces are the seams of the **high-level design**. The design is
cloud-agnostic: components are capabilities behind ports (a durable log, a KV cache, a wide-column
store, a relational DB, an object store), and configuration picks the implementation.

| Document | Use it for |
|---|---|
| [INTERVIEW.md](INTERVIEW.md) | the 60-minute walkthrough: requirements, sizing, API, LLD, graph problems, DB model, use-case diagrams, the big picture, trade-offs |
| [docs/PORTABILITY.md](docs/PORTABILITY.md) | AWS / GCP / Azure / self-hosted mapping, semantic differences, adapter modules, switching provider |
| [docs/DESIGN-DETAILED.md](docs/DESIGN-DETAILED.md) | the full API reference, DDL, capacity math, failure modes, security, distributed adapters |

## Build, run, test

Requires JDK 17+.

```bash
cd webcrawler
mvn -pl core test              # 94 tests, no network beyond loopback: an in-memory FakeWeb plays the internet
mvn -pl core spring-boot:run   # http://localhost:8080, Swagger UI at /swagger-ui.html
```

`webcrawler/` is a multi-module build (`webcrawler-parent`). The distributed adapters are separate modules, so the core
never depends on them:

| Module | What | Tests (Testcontainers; skipped without Docker) |
|---|---|---|
| `core` (artifact `webcrawler`) | engine, API, ports, in-memory and filesystem adapters | 94 |
| `adapter-kafka` | `KafkaFrontier`: the frontier as a consumer group on one topic keyed by host | `KafkaFrontierTest` (3), on `cp-kafka` |
| `adapter-redis-cql` | `RedisCqlJobStore` (Redis Lua + Cassandra + Postgres) and `RedisHostSchedule` | `RedisCqlJobStoreTest` (17 = the 15-test `JobStoreContract` + 2), on `redis`, `cassandra`, `postgres` |
| `it` | two Spring Boot nodes of the unchanged app on all four | `TwoNodeCrawlTest` (1) |

```bash
# from webcrawler/: the reactor builds core first (the adapters use its test-jar), then the adapters and the e2e test
mvn install
# or from the repo root
mvn -pl webcrawler -amd install
```

To run a node distributed, add both adapter jars to the classpath and select them:

```bash
--crawler.adapters.frontier=kafka --crawler.adapters.job-store=redis-cql \
--crawler.adapters.content-store=filesystem --crawler.adapters.content-root=/shared/blobs \
--crawler.kafka.bootstrap-servers=… --crawler.kafka.client-id=$(hostname) \
--crawler.redis-cql.redis-uri=… --crawler.redis-cql.cassandra-contact-points=… --crawler.redis-cql.jdbc-url=…
```

Every property is listed in [PORTABILITY §5](docs/PORTABILITY.md#5-adapter-modules).

```bash
# async (default): 202 + Location
curl -si -X POST localhost:8080/v1/crawls -H 'X-Tenant-Id: t1' -H 'Content-Type: application/json' \
  -d '{"seeds":["https://example.com/"],"maxDepth":2,"maxPages":50}'

# sync: 200 with the whole graph, or 202 if it doesn't finish within syncTimeoutMs
curl -s -X POST localhost:8080/v1/crawls -H 'X-Tenant-Id: t1' -H 'Content-Type: application/json' \
  -d '{"seeds":["https://example.com/"],"maxDepth":1,"maxPages":10,"mode":"SYNC"}'

curl -s  localhost:8080/v1/crawls/{jobId}                     -H 'X-Tenant-Id: t1'
curl -s 'localhost:8080/v1/crawls/{jobId}/pages?type=PAGE'    -H 'X-Tenant-Id: t1'
curl -s 'localhost:8080/v1/crawls/{jobId}/pages/{hash}/content?links=snapshot' -H 'X-Tenant-Id: t1'
curl -so site.zip localhost:8080/v1/crawls/{jobId}/export      -H 'X-Tenant-Id: t1'
```

Settings are under `crawler.*` in [`application.yml`](core/src/main/resources/application.yml)
(workers, politeness delay, retries, redirect limit, page size cap, user agent, timeouts, max sync
wait, `adapters.*`, and `egress.*`, the SSRF guard). To keep blobs on disk instead of in memory:

```bash
mvn -pl core spring-boot:run -Dspring-boot.run.arguments=--crawler.adapters.content-store=filesystem
```

## Code map

Package `com.salesforce.einstein.webcrawler`:

| Package | Classes | Design role |
|---|---|---|
| `model` | `CrawlRequest`, `CrawlJob`, `CrawlTask`, `CanonicalUrl`, `JobPage`, `LinkEdge`, `PageSnapshot`, `JobStats`, enums | entities ([INTERVIEW §6.1](INTERVIEW.md#61-entities)). `CrawlRequest.validate()` holds the sync vs async boundary |
| `url` | `UrlNormalizer`, `ParamRules`, `TrapDetector`, `BloomFilter`, `Hashing` | canonical node ids, tracking and learned params, spider traps, the global negative cache |
| `parse` | `HtmlParser`, `CssParser` | links and assets from `a`, `img`/`srcset`, `video`/`audio`/`source`, `link`, `style`, CSS `url()`/`@import`; canonical and meta robots |
| `fetch` | `Fetcher`, `HttpFetcher`, `EgressPolicy`, `EgressFilteringFetcher`, `RobotsRules`, `RobotsCache`, `CachingDnsResolver` | no automatic redirects, size cap, one deadline over headers and body, SSRF guard, conditional GET, RFC 9309 robots |
| `frontier` | `Frontier`, `InMemoryFrontier`, `PartitionedFrontier` | two-level frontier: BFS queue per host, a ready heap by next-allowed time, one request in flight per host; `PartitionedFrontier` spreads hosts over several nodes (host → partition → node by rendezvous hashing) with at-least-once delivery and replay when a node leaves |
| `store` | `JobStore`, `PageStore`, `ContentStore`, in-memory implementations, `FileSystemContentStore` | `JobStore.admit()` is the atomic seen-test + budget + open task; `closeTask()` decrements `pending` once per task key; `claimContent()` is the content-seen test. All per-job state (scope, variant counts, terminal events) lives here, so several engines can share a job |
| `engine` | `CrawlEngine`, `EngineConfig` | the crawl loop: `process()` → `onContent()` → `follow()` / `addAsset()` → `enqueue()` |
| `render` | `CrawlResults`, `LinkRewriter`, `LinkMode` | read-time link rewriting (`RAW` / `ABSOLUTE` / `SNAPSHOT`), redirect and duplicate resolution, offline ZIP export |
| `api` | `CrawlController`, `PageController`, `ApiModels`, `ApiExceptionHandler`, `WebhookNotifier` | REST API, RFC 7807 problems, sandboxed content serving |
| `config` | `CrawlerProperties`, `CrawlerConfiguration` | wiring. `crawler.adapters.*` picks the implementation for each port; provider modules register beans for their own values ([PORTABILITY §5](docs/PORTABILITY.md#5-adapter-modules)) |

## Design → code

| Design question | Where it's answered |
|---|---|
| Sync vs async boundary | `CrawlRequest.validate()`, `CrawlController.create()` (bounded wait, then 202) |
| Large depth | BFS order in `InMemoryFrontier`; the depth check in `CrawlEngine.follow()`; shortest-path depth via `SHALLOWER` + `CrawlEngine.relax()`; budgets in `JobStore.admit()`; `TrapDetector` |
| Cycles | `UrlNormalizer` + `JobStore.admit()` → `SEEN`; edges are always recorded; redirect hop limit in `CrawlEngine.onRedirect()` |
| Same page, different URL params | `UrlNormalizer` (sort, tracking list), `ParamRules` (learned per host), `claimContent()` → `DUPLICATE`, `rel=canonical` |
| Images, video, audio | `HtmlParser` classification, `CrawlEngine.addAsset()` (leaf, `downloadAssets` policy, `REFERENCED`), `maxAssetBytes` |
| Already visited | per job: `admit()`. Across jobs: `BloomFilter` → `PageStore` snapshot (`REUSED`) → conditional GET (304). For the API: `GET /v1/pages?url=` → `JobStore.latestForTenant()`, the tenant's own crawls only |
| Serving and downloading with links to our copies | `CrawlResults.content()` and `export()`, `LinkRewriter` |

## Tests

| Class | Covers |
|---|---|
| `CrawlEngineTest` (27) | cycles, redirect loops, depth, page budget, scope, seed redirect to another host, canonical chains, spider traps, tracking params, duplicates under params, same bytes in different directories, param learning, assets and media references, embed later linked as a page, asset budget, oversized assets, cross-job reuse, 304 revalidation, retries, 404, robots (and robots redirects), nofollow, idempotency (and concurrent submits), sync bounds, cancel |
| `CrawlResultsTest` (6) | snapshot rewriting, absolute fallback, duplicate resolution, CSS rewriting, raw/absolute modes, ZIP export with relative links |
| `RewriteEdgeCasesTest` (4) | crawl-time edges win over later param rules, malformed `<base>`, `<base>` without a path, identical CSS at two URLs in the export |
| `EgressTest` (5) | SSRF: loopback, metadata, private, CGNAT, IPv6 ULA, NAT64, split DNS answers, ports, schemes; blocked requests never sent or retried |
| `HttpFetcherTest` (2) | size cap and a slow-drip body hitting the deadline, against a loopback server |
| `UrlNormalizerTest` (9) | normalization, relative resolution, empty base path, learned params (distinct values), trap detector |
| `DistributedCrawlTest` (6) | three nodes on one job: only ~1/N partitions move when a node joins, one node per host and no overlap, a node dying mid-fetch (no loss, no double count), depth lowered by a shorter path found mid-fetch or after expansion (re-expanded from stored bytes), and passed through a redirect |
| `InMemoryFrontierTest` (3) | one in flight per host, politeness delay, BFS within a host |
| `RobotsAndBloomTest` (2) | robots longest match and agent groups; Bloom false-positive rate |
| `InMemoryContentStoreTest` (5), `FileSystemContentStoreTest` (6) | the `ContentStoreContract` every blob adapter must pass |
| `InMemoryJobStoreTest` (15) | the `JobStoreContract` every job store must pass: idempotent create, status CAS and broadcast, admit (seen, shallower, upgrade, budgets, a 16-thread race), exactly-once task close under redelivery, content claims, exact stats, edges, BFS cursor, tenant-scoped latest |
| `AdapterSelectionTest` (1) | a config value alone switches the `ContentStore` implementation |
| `CrawlControllerTest` (3) | sync and async over HTTP, tenant isolation of jobs and of `GET /v1/pages`, validation problems (`@SpringBootTest` + MockMvc, `FakeWeb` as the fetcher) |

## Not in the single-process build

These are in the design but not implemented here:
- the object-store adapters and the CQL `PageStore` (`url_records`)
  ([PORTABILITY §5](docs/PORTABILITY.md#5-adapter-modules),
  [DESIGN-DETAILED §7](docs/DESIGN-DETAILED.md#7-distributed-adapters));
- the p0/p1/p2 priority lanes and the reconciler;
- connecting to the exact IP the egress check approved (the HTTP client resolves again, so DNS
  rebinding is left to the network layer);
- webhook retries and signatures, SimHash near-duplicates and JavaScript rendering.

With the default `memory` adapters, jobs and the frontier are lost on restart, and blobs survive
with `content-store: filesystem`. With `kafka` and `redis-cql` selected, jobs, the frontier and the
graph are durable.

Known simplifications: learned parameter rules are global and never expire; frontier entries of a
cancelled job are dropped lazily; `PartitionedFrontier` is an in-process stand-in for the log and
its consumer group (nodes are engines in one JVM sharing the in-memory stores).
