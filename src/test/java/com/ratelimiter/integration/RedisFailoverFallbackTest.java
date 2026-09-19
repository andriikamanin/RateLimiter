package com.ratelimiter.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.ratelimiter.engine.RateLimitEngine;
import com.ratelimiter.engine.RateLimitResult;
import com.ratelimiter.grpc.v1.Algorithm;
import com.ratelimiter.grpc.v1.RateLimitRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Verifies the fault-tolerance requirement: once Redis becomes unreachable,
 * the engine must keep enforcing a real (degraded, per-instance) rate limit
 * via the L1 local limiter rather than failing every request or silently
 * allowing unlimited traffic.
 *
 * <p>Uses its own dedicated container (rather than the shared one in
 * {@link AbstractRedisIntegrationTest}) because this test deliberately
 * kills Redis mid-test.
 */
@SpringBootTest(webEnvironment = WebEnvironment.NONE)
@Testcontainers
class RedisFailoverFallbackTest {

    private static final String RESOURCE = "/api/v1/checkout";

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("ratelimiter.redis.uri",
                () -> "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        registry.add("ratelimiter.redis.command-timeout-ms", () -> "200");
        registry.add("ratelimiter.resources[/api/v1/checkout].capacity", () -> "10");
        registry.add("ratelimiter.resources[/api/v1/checkout].refill-rate-per-sec", () -> "0");
        registry.add("ratelimiter.resources[/api/v1/checkout].leak-rate-per-sec", () -> "0");
        registry.add("ratelimiter.fail-strategy", () -> "FAIL_OPEN");
    }

    @Autowired
    RateLimitEngine engine;

    private static RateLimitRequest request(String key) {
        return RateLimitRequest.newBuilder()
                .setKey(key)
                .setResource(RESOURCE)
                .setTokensRequested(1)
                .setAlgorithm(Algorithm.TOKEN_BUCKET)
                .build();
    }

    @Test
    void fallsBackToLocalRateLimitingWhenRedisIsUnreachable() {
        String key = "fallback-" + System.nanoTime();

        // Warm the path against real Redis first.
        assertThat(engine.checkLimit(request(key)).join().allowed()).isTrue();

        REDIS.stop();

        int allowed = 0;
        for (int i = 0; i < 15; i++) {
            RateLimitResult result = engine.checkLimit(request(key)).join();
            if (result.allowed()) {
                allowed++;
            }
        }

        // Capacity is 10; one token was already spent against Redis before it
        // went down, so the local fallback (which starts from a full bucket,
        // since it has no prior local state for this key) should allow up to
        // capacity and then start rejecting -- proving it is a real bucket,
        // not a blanket fail-open allow-everything path.
        assertThat(allowed).isEqualTo(10);
    }
}
