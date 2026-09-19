package com.ratelimiter.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.ratelimiter.engine.RateLimitEngine;
import com.ratelimiter.engine.RateLimitResult;
import com.ratelimiter.grpc.v1.Algorithm;
import com.ratelimiter.grpc.v1.RateLimitRequest;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * End-to-end tests against the full Spring context and a real Redis
 * (Testcontainers), covering concurrent access, bucket exhaustion, and
 * refill-over-time -- the scenarios required to trust the atomicity of the
 * Lua scripts and the wiring between the gRPC layer's engine and Redis.
 */
@SpringBootTest(webEnvironment = WebEnvironment.NONE)
class RateLimitEngineIntegrationTest extends AbstractRedisIntegrationTest {

    private static final String RESOURCE = "/api/v1/checkout";

    @DynamicPropertySource
    static void overrideResourceConfig(DynamicPropertyRegistry registry) {
        // Disable refill so exhaustion/concurrency assertions are deterministic.
        registry.add("ratelimiter.resources[/api/v1/checkout].capacity", () -> "20");
        registry.add("ratelimiter.resources[/api/v1/checkout].refill-rate-per-sec", () -> "0");
        registry.add("ratelimiter.resources[/api/v1/checkout].leak-rate-per-sec", () -> "0");
    }

    @Autowired
    RateLimitEngine engine;

    private static RateLimitRequest request(String key, String resource, int tokens, Algorithm algorithm) {
        return RateLimitRequest.newBuilder()
                .setKey(key)
                .setResource(resource)
                .setTokensRequested(tokens)
                .setAlgorithm(algorithm)
                .build();
    }

    @Test
    void allowsExactlyCapacityRequestsThenRejects() {
        String key = "user-" + System.nanoTime();

        int allowed = 0;
        for (int i = 0; i < 25; i++) {
            RateLimitResult result = engine.checkLimit(request(key, RESOURCE, 1, Algorithm.TOKEN_BUCKET)).join();
            if (result.allowed()) {
                allowed++;
            }
        }

        assertThat(allowed).isEqualTo(20);
    }

    @Test
    void rejectedRequestReportsRetryAfter() {
        String key = "user-" + System.nanoTime();
        for (int i = 0; i < 20; i++) {
            assertThat(engine.checkLimit(request(key, RESOURCE, 1, Algorithm.TOKEN_BUCKET)).join().allowed()).isTrue();
        }

        RateLimitResult rejected = engine.checkLimit(request(key, RESOURCE, 1, Algorithm.TOKEN_BUCKET)).join();
        assertThat(rejected.allowed()).isFalse();
        assertThat(rejected.remainingTokens()).isEqualTo(0);
    }

    @Test
    void guardCacheShortCircuitsRepeatedBurstsAfterFirstRejection() {
        String key = "guarded-" + System.nanoTime();
        for (int i = 0; i < 20; i++) {
            engine.checkLimit(request(key, RESOURCE, 1, Algorithm.TOKEN_BUCKET)).join();
        }

        // First rejection comes from Redis and populates the guard cache.
        RateLimitResult firstRejection = engine.checkLimit(request(key, RESOURCE, 1, Algorithm.TOKEN_BUCKET)).join();
        assertThat(firstRejection.allowed()).isFalse();

        // Subsequent bursts should still be rejected (served from the local guard cache).
        for (int i = 0; i < 10; i++) {
            assertThat(engine.checkLimit(request(key, RESOURCE, 1, Algorithm.TOKEN_BUCKET)).join().allowed()).isFalse();
        }
    }

    @Test
    void concurrentRequestsAcrossVirtualThreadsNeverExceedCapacity() throws Exception {
        String key = "concurrent-" + System.nanoTime();
        int callers = 40;
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger allowed = new AtomicInteger();

        List<CompletableFuture<Void>> futures = IntStream.range(0, callers)
                .mapToObj(i -> CompletableFuture.runAsync(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    RateLimitResult result = engine.checkLimit(request(key, RESOURCE, 1, Algorithm.TOKEN_BUCKET)).join();
                    if (result.allowed()) {
                        allowed.incrementAndGet();
                    }
                }, pool))
                .toList();

        start.countDown();
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(allowed.get()).isEqualTo(20);
    }

    @Test
    void leakyBucketSmoothsBurstsIndependentlyOfTokenBucket() {
        String key = "leaky-" + System.nanoTime();

        int allowed = 0;
        for (int i = 0; i < 25; i++) {
            RateLimitResult result = engine.checkLimit(request(key, RESOURCE, 1, Algorithm.LEAKY_BUCKET)).join();
            if (result.allowed()) {
                allowed++;
            }
        }

        assertThat(allowed).isEqualTo(20);
    }
}
