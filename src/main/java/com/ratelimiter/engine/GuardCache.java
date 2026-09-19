package com.ratelimiter.engine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.ratelimiter.config.RateLimiterProperties;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;

/**
 * Local negative cache: once Redis rejects a (key, resource, algorithm)
 * tuple, subsequent requests during its backoff window are rejected purely
 * from JVM memory with no Redis round trip at all. This is what protects
 * the backing store during a sustained burst/attack from a single client
 * instead of forwarding every retry to Redis.
 */
@Component
public class GuardCache {

    private final Cache<String, Long> blockedUntilMs;
    private final long maxTtlMs;

    public GuardCache(RateLimiterProperties properties) {
        this.maxTtlMs = properties.getGuardCache().getMaxTtlSeconds() * 1000;
        this.blockedUntilMs = Caffeine.newBuilder()
                .maximumSize(properties.getGuardCache().getMaxSize())
                .expireAfterWrite(properties.getGuardCache().getMaxTtlSeconds(), TimeUnit.SECONDS)
                .build();
    }

    public static String keyFor(String key, String resource, String algorithm) {
        return key + '|' + resource + '|' + algorithm;
    }

    /** Returns remaining block time in ms, or -1 if not currently blocked. */
    public long remainingBlockMs(String guardKey, long nowMs) {
        Long blockedUntil = blockedUntilMs.getIfPresent(guardKey);
        if (blockedUntil == null || nowMs >= blockedUntil) {
            return -1;
        }
        return blockedUntil - nowMs;
    }

    public void block(String guardKey, long retryAfterMs, long nowMs) {
        long effectiveRetry = retryAfterMs <= 0 ? maxTtlMs : Math.min(retryAfterMs, maxTtlMs);
        blockedUntilMs.put(guardKey, nowMs + effectiveRetry);
    }
}
