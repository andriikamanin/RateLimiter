package com.ratelimiter.engine;

import com.ratelimiter.config.RateLimiterProperties;
import com.ratelimiter.grpc.v1.Algorithm;
import com.ratelimiter.metrics.RateLimiterMetrics;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.springframework.stereotype.Component;

/**
 * Distributed (L2) rate limiting backed by Redis. State evaluation happens
 * entirely inside a single Lua script invocation so the read-modify-write
 * of the bucket is atomic across every instance of this service talking to
 * the same Redis, without needing WATCH/MULTI round trips or client locks.
 */
@Component
public class L2RedisRateLimiter {

    private final LuaScriptRunner scriptRunner;
    private final RateLimiterProperties properties;
    private final RateLimiterMetrics metrics;

    public L2RedisRateLimiter(LuaScriptRunner scriptRunner, RateLimiterProperties properties, RateLimiterMetrics metrics) {
        this.scriptRunner = scriptRunner;
        this.properties = properties;
        this.metrics = metrics;
    }

    public CompletableFuture<RateLimitResult> check(String key, String resource, int tokensRequested,
                                                      Algorithm algorithm, BucketConfig config) {
        LuaScript script = algorithm == Algorithm.LEAKY_BUCKET ? LuaScript.LEAKY_BUCKET : LuaScript.TOKEN_BUCKET;
        String redisKey = buildKey(resource, key, algorithm);
        long now = System.currentTimeMillis();
        long ttlMs = properties.getRedis().getKeyTtlSeconds() * 1000;
        String[] args = {
                Long.toString(config.capacity()),
                Double.toString(config.ratePerMs()),
                Long.toString(now),
                Integer.toString(tokensRequested),
                Long.toString(ttlMs)
        };

        var sample = metrics.startRedisTimer();
        return scriptRunner.eval(script, redisKey, args)
                .thenApply(L2RedisRateLimiter::toResult)
                .whenComplete((result, ex) -> metrics.stopRedisTimer(sample));
    }

    /**
     * Hash-tagged so that a Redis Cluster deployment routes all keys for a
     * given (resource, client) pair to the same slot; irrelevant for a
     * single-node Redis but free and forward-compatible.
     */
    private static String buildKey(String resource, String key, Algorithm algorithm) {
        return "rl:{" + resource + ":" + key + "}:" + algorithm.name();
    }

    private static RateLimitResult toResult(List<Long> reply) {
        boolean allowed = reply.get(0) == 1L;
        long remaining = reply.get(1);
        long retryAfterMs = reply.get(2);
        return new RateLimitResult(allowed, remaining, retryAfterMs);
    }
}
