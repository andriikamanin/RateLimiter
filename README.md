# Rate Limiter Service

A production-grade, distributed, in-memory rate limiter and throttling service.
It runs as a gRPC sidecar/dependency in front of microservices, enforcing
per-client / per-resource quotas with sub-millisecond decision latency under
high concurrency, and degrading gracefully when its backing store is
unavailable.

**Stack:** Java 21 (virtual threads) · gRPC (Netty) · Spring Boot 3 ·
Redis (Lettuce, async) · Lua · Caffeine · Micrometer/Prometheus · Testcontainers.

---

## 1. Architecture

```
                     ┌─────────────────────────────────────────────┐
                     │              RateLimiterGrpcService          │
                     │  (virtual-thread-per-call gRPC executor)     │
                     └───────────────────────┬───────────────────────┘
                                              │
                                              ▼
                     ┌─────────────────────────────────────────────┐
                     │               RateLimitEngine                │
                     │                                               │
                     │  1. GuardCache (Caffeine, local, negative     │
                     │     cache) -- reject known-over-quota keys    │
                     │     with ZERO Redis round trips.              │
                     │                                               │
                     │  2. L2RedisRateLimiter -- atomic Lua eval      │
                     │     against Redis (source of truth).          │
                     │                                               │
                     │  3. On Redis failure/timeout: L1LocalRate-     │
                     │     Limiter -- same algorithm, per-instance,  │
                     │     lock-free (AtomicReference CAS + LongAdder)│
                     │                                               │
                     │  4. If even the fallback throws: FailStrategy  │
                     │     (FAIL_OPEN / FAIL_CLOSED) as last resort.  │
                     └───────────────────────┬───────────────────────┘
                                              │
                             ┌────────────────┴────────────────┐
                             ▼                                  ▼
                     ┌───────────────┐                 ┌────────────────┐
                     │     Redis     │                 │ Caffeine (JVM)  │
                     │ token_bucket. │                 │ per-key bucket  │
                     │ leaky_bucket. │                 │ state, lock-free│
                     │   lua (atomic)│                 └────────────────┘
                     └───────────────┘
```

### 2.1 Why two rate-limiting layers?

| Layer | Scope | Purpose |
|---|---|---|
| **GuardCache** (L1, negative cache) | per-instance | Once Redis rejects a key, absorb the *retry storm* that follows locally, at zero cost to Redis, for the duration of the backoff window. |
| **L2 (Redis + Lua)** | global, cluster-wide | Source of truth. All instances agree on the same bucket state via one atomic script execution per decision (single RTT, no client-side locking, no WATCH/MULTI races). |
| **L1 (Caffeine + AtomicReference)** | per-instance | Degradation path when Redis is unreachable. Runs the *same* token-bucket/leaky-bucket math locally so the service keeps behaving like a rate limiter (not a binary switch) during an outage. |

### 2.2 Fault tolerance: fail-open vs fail-closed

The `ratelimiter.fail-strategy` setting is a **last-resort safety valve**,
not the primary degradation path. The primary path is always "fall back to
local rate limiting" (requirement #3 in the spec). `FAIL_OPEN` / `FAIL_CLOSED`
only kicks in if the local fallback *itself* throws an unexpected exception
(e.g., a bug, an OOM on the local cache) — at that point there is no
algorithm left to run, and the operator's policy decides whether to let
traffic through (`FAIL_OPEN`) or block it (`FAIL_CLOSED`).

This design was chosen because a naive "Redis down ⇒ fail open/closed"
policy either turns off protection for the whole fleet during an outage
(fail-open) or takes down every dependent service at once (fail-closed).
Continuing to enforce a *local* limit is strictly safer in both directions.

### 2.3 Why Lua scripts instead of client-side logic

Reading bucket state, computing refill, and writing it back requires a
read-modify-write. Doing that with separate Redis commands from the client
is racy under concurrency (two clients can both read "3 tokens left" and
both deduct, leaving the bucket over-drawn). Lua scripts execute atomically
inside Redis's single-threaded event loop, so the entire decision is one
round trip and one indivisible operation — no locks, no transactions needed.

Scripts are cached with `SCRIPT LOAD` at startup and invoked with `EVALSHA`;
on a `NOSCRIPT` error (e.g., after a Redis restart) the runner transparently
falls back to `EVAL` with the full script body.

### 2.4 Virtual threads

`GrpcVirtualThreadConfig` swaps the gRPC server's executor for
`Executors.newVirtualThreadPerTaskExecutor()`, so each in-flight `CheckLimit`
call — including the (non-blocking, but still awaited) trip to Redis — costs
a virtual thread instead of pinning a platform thread. This lets the service
hold a very large number of concurrent in-flight requests with a small,
fixed number of OS threads.

### 2.5 Lock-free local state

`L1LocalRateLimiter` keeps one `AtomicReference<BucketState>` per
(key, resource, algorithm) in a Caffeine cache and updates it via a
compare-and-swap retry loop — no `synchronized`, no explicit locks.
`LongAdder` counters track local check/rejection totals without the
contention a single `AtomicLong` would suffer under many threads.

---

## 3. gRPC API

See [`src/main/proto/rate_limiter.proto`](src/main/proto/rate_limiter.proto).

```protobuf
service RateLimiterService {
  rpc CheckLimit (RateLimitRequest) returns (RateLimitResponse);
}

enum Algorithm {
  ALGORITHM_UNSPECIFIED = 0;
  TOKEN_BUCKET = 1;
  LEAKY_BUCKET = 2;
}

message RateLimitRequest {
  string key = 1;              // e.g. "user_123", "ip_192.168.1.1"
  string resource = 2;         // e.g. "/api/v1/checkout"
  int32 tokens_requested = 3;  // defaults to 1 if <= 0
  Algorithm algorithm = 4;     // defaults to TOKEN_BUCKET if unspecified
}

message RateLimitResponse {
  bool allowed = 1;
  int64 remaining_tokens = 2;
  int64 retry_after_ms = 3;
}
```

---

## 4. Configuration

All configuration lives under `ratelimiter.*` in
[`application.yml`](src/main/resources/application.yml):

```yaml
ratelimiter:
  fail-strategy: FAIL_OPEN          # FAIL_OPEN | FAIL_CLOSED (last-resort only, see 2.2)
  redis:
    uri: redis://localhost:6379
    command-timeout-ms: 150
    key-ttl-seconds: 120
  guard-cache:
    max-ttl-seconds: 30
    max-size: 500000
  local-fallback-cache:
    max-size: 500000
    expire-after-access-seconds: 300
  default-bucket:
    capacity: 100
    refill-rate-per-sec: 50
    leak-rate-per-sec: 50
  resources:
    /api/v1/checkout:
      capacity: 20
      refill-rate-per-sec: 5
      leak-rate-per-sec: 5
```

Every setting can be overridden via environment variables using Spring's
relaxed binding, e.g. `RATELIMITER_REDIS_URI`, `RATELIMITER_FAIL_STRATEGY`.

---

## 5. Running locally

### 5.1 Full stack via Docker Compose

```bash
docker compose up --build
```

This starts:

| Service | Port | Purpose |
|---|---|---|
| `rate-limiter` | `6565` (gRPC), `8081` (HTTP/actuator) | The service itself |
| `redis` | `6379` | Distributed bucket state |
| `prometheus` | `9090` | Scrapes `/actuator/prometheus` every 5s |
| `grafana` | `3000` | Pre-provisioned dashboard, anonymous viewer access enabled |

Open Grafana at http://localhost:3000 — the "Rate Limiter Service" dashboard
is auto-provisioned (admin/admin, or browse anonymously).

### 5.2 Running just the app against a local Redis

```bash
docker run -d --name redis -p 6379:6379 redis:7-alpine
mvn spring-boot:run
```

---

## 6. Testing it with `grpcurl`

```bash
# List services (reflection is enabled)
grpcurl -plaintext localhost:6565 list

# Allowed request
grpcurl -plaintext -d '{
  "key": "user_123",
  "resource": "/api/v1/checkout",
  "tokens_requested": 1,
  "algorithm": "TOKEN_BUCKET"
}' localhost:6565 ratelimiter.v1.RateLimiterService/CheckLimit

# Drive a key over quota (capacity for /api/v1/checkout is 20 by default)
for i in $(seq 1 25); do
  grpcurl -plaintext -d '{
    "key": "burst_user",
    "resource": "/api/v1/checkout",
    "tokens_requested": 1,
    "algorithm": "TOKEN_BUCKET"
  }' localhost:6565 ratelimiter.v1.RateLimiterService/CheckLimit
done

# Leaky bucket
grpcurl -plaintext -d '{
  "key": "user_456",
  "resource": "/api/v1/search",
  "tokens_requested": 1,
  "algorithm": "LEAKY_BUCKET"
}' localhost:6565 ratelimiter.v1.RateLimiterService/CheckLimit
```

Expected shape of a response:

```json
{
  "allowed": true,
  "remainingTokens": "19",
  "retryAfterMs": "0"
}
```

---

## 7. Benchmarking with `ghz`

[`ghz`](https://ghz.sh/) is a gRPC load-testing tool. With the stack running:

```bash
ghz \
  --insecure \
  --proto src/main/proto/rate_limiter.proto \
  --call ratelimiter.v1.RateLimiterService.CheckLimit \
  -d '{"key":"bench_user_{{.RequestNumber}}","resource":"/api/v1/search","tokens_requested":1,"algorithm":"TOKEN_BUCKET"}' \
  -c 200 -n 200000 \
  localhost:6565
```

- `-c 200`: 200 concurrent virtual clients.
- `-n 200000`: 200,000 total requests.
- `{{.RequestNumber}}` spreads load across distinct keys so you exercise the
  hot path (Redis + guard cache) rather than a single bucket's contention.

To specifically benchmark the sustained-hot-key path (repeated bursts against
one key, exercising the guard-cache short-circuit):

```bash
ghz \
  --insecure \
  --proto src/main/proto/rate_limiter.proto \
  --call ratelimiter.v1.RateLimiterService.CheckLimit \
  -d '{"key":"hot_key","resource":"/api/v1/checkout","tokens_requested":1,"algorithm":"TOKEN_BUCKET"}' \
  -c 500 -n 500000 \
  localhost:6565
```

Watch `rate_limiter_latency_seconds` (p50/p95/p99) and
`redis_execution_latency_seconds` on the Grafana dashboard while `ghz` runs —
the gap between the two shows how much of end-to-end latency is Redis vs.
in-process overhead, and the guard cache should keep p99 flat even as
rejected-request volume grows.

---

## 8. Metrics

Exposed at `/actuator/prometheus`:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `rate_limiter_requests_total` | Counter | `status` (`allowed`\|`rejected`), `algorithm`, `resource` | Every decision made |
| `rate_limiter_redis_failures_total` | Counter | `resource` | Times Redis was unreachable/timed out and the request fell back to L1 |
| `rate_limiter_latency_seconds` | Histogram | `algorithm`, `resource` | End-to-end decision latency (gRPC handler through engine) |
| `redis_execution_latency_seconds` | Histogram | — | Latency of the Lua script round trip to Redis alone |

Both histograms are configured with percentile-histogram buckets so
Prometheus `histogram_quantile` can compute p50/p95/p99 (see the pre-built
Grafana dashboard).

---

## 9. Testing

```bash
mvn test
```

- `L1LocalRateLimiterTest` — unit tests for the lock-free local fallback
  algorithm, including a concurrency test asserting exactly `capacity`
  requests are allowed out of many concurrent virtual-thread callers.
- `LuaScriptEngineTest` (Testcontainers) — exercises the raw Lua scripts
  against a real Redis: exhaustion, refill-over-time, and a concurrency test
  hammering one key from 20 threads to confirm the script's atomicity.
- `RateLimitEngineIntegrationTest` (Testcontainers) — full Spring context:
  capacity enforcement, retry-after reporting, guard-cache short-circuiting,
  concurrent virtual-thread callers, and the leaky-bucket algorithm.
- `RedisFailoverFallbackTest` (Testcontainers) — stops the Redis container
  mid-test and asserts the engine keeps enforcing a real (local) bucket
  limit instead of erroring or allowing unlimited traffic.
- `RateLimiterGrpcServiceTest` — in-process gRPC server/channel, validates
  request validation and response mapping without needing Redis at all.

Testcontainers tests require a running Docker daemon.

---

## 10. Performance tuning

The Dockerfile runs the JVM with:

```
-XX:+UseZGC -XX:+ZGenerational -Xms256m -Xmx512m
```

Generational ZGC targets sub-millisecond pause times regardless of heap
size, which matters here because pause time directly adds to p99 gRPC
latency under load. `spring.threads.virtual.enabled: true` plus the explicit
virtual-thread executor on the gRPC server (`GrpcVirtualThreadConfig`) means
call-handling threads are cheap enough that a stalled Redis call blocks only
that one virtual thread, not a shared platform-thread pool slot.

---

## 11. Project layout

```
src/main/proto/rate_limiter.proto        gRPC contract
src/main/resources/lua/*.lua             Atomic Redis state-evaluation scripts
src/main/resources/application.yml       All runtime configuration
src/main/java/com/ratelimiter/
  config/                                 Redis client, virtual-thread executor, typed config properties
  engine/                                 GuardCache, L1 (local) and L2 (Redis) limiters, orchestrating engine
  grpc/                                   gRPC service adapter
  metrics/                                Micrometer instrumentation
src/test/java/com/ratelimiter/           Unit + Testcontainers integration tests
docker/                                   Prometheus config, Grafana provisioning + dashboard
docker-compose.yml                        Full local stack
Dockerfile                                Multi-stage build, ZGC-tuned runtime
```
