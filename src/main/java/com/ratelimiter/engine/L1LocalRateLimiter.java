package com.ratelimiter.engine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.ratelimiter.config.RateLimiterProperties;
import com.ratelimiter.grpc.v1.Algorithm;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.springframework.stereotype.Component;

/**
 * In-process (L1) rate limiter that mirrors the token-bucket / leaky-bucket
 * semantics of the Redis Lua scripts. It is the degradation path used when
 * Redis is unreachable: each instance limits independently rather than
 * globally, which is strictly more permissive in aggregate across a fleet
 * but keeps individual instances protected and keeps behaving like "a rate
 * limiter" instead of collapsing to open/closed.
 *
 * <p>Bucket state is held in an {@link AtomicReference} per key and updated
 * via a lock-free compare-and-swap retry loop, so concurrent callers never
 * block on a lock; {@link LongAdder} counters track throughput without the
 * contention a single shared counter would cause under high concurrency.
 */
@Component
public class L1LocalRateLimiter {

    private record BucketState(double amount, long timestampMs) {
    }

    private final Cache<String, AtomicReference<BucketState>> buckets;
    private final LongAdder localChecks = new LongAdder();
    private final LongAdder localRejections = new LongAdder();

    public L1LocalRateLimiter(RateLimiterProperties properties) {
        this.buckets = Caffeine.newBuilder()
                .maximumSize(properties.getLocalFallbackCache().getMaxSize())
                .expireAfterAccess(properties.getLocalFallbackCache().getExpireAfterAccessSeconds(), TimeUnit.SECONDS)
                .build();
    }

    public RateLimitResult checkLocal(String key, String resource, int tokensRequested, Algorithm algorithm, BucketConfig config) {
        String cacheKey = key + '|' + resource + '|' + algorithm.name();
        long now = System.currentTimeMillis();
        AtomicReference<BucketState> ref = buckets.get(cacheKey,
                k -> new AtomicReference<>(new BucketState(algorithm == Algorithm.LEAKY_BUCKET ? 0 : config.capacity(), now)));

        boolean leaky = algorithm == Algorithm.LEAKY_BUCKET;
        localChecks.increment();

        while (true) {
            BucketState current = ref.get();
            long ts = System.currentTimeMillis();
            long elapsed = Math.max(0, ts - current.timestampMs());
            double drained = elapsed * config.ratePerMs();

            double amount = leaky
                    ? Math.max(0, current.amount() - drained)
                    : Math.min(config.capacity(), current.amount() + drained);

            boolean allowed;
            double nextAmount;
            long retryAfterMs = 0;
            long remaining;

            if (leaky) {
                if (amount + tokensRequested <= config.capacity()) {
                    allowed = true;
                    nextAmount = amount + tokensRequested;
                } else {
                    allowed = false;
                    nextAmount = amount;
                    double overflow = (amount + tokensRequested) - config.capacity();
                    retryAfterMs = config.ratePerMs() > 0 ? (long) Math.ceil(overflow / config.ratePerMs()) : -1;
                }
                remaining = Math.max(0, (long) (config.capacity() - nextAmount));
            } else {
                if (amount >= tokensRequested) {
                    allowed = true;
                    nextAmount = amount - tokensRequested;
                } else {
                    allowed = false;
                    nextAmount = amount;
                    double deficit = tokensRequested - amount;
                    retryAfterMs = config.ratePerMs() > 0 ? (long) Math.ceil(deficit / config.ratePerMs()) : -1;
                }
                remaining = (long) nextAmount;
            }

            BucketState next = new BucketState(nextAmount, ts);
            if (ref.compareAndSet(current, next)) {
                if (!allowed) {
                    localRejections.increment();
                }
                return new RateLimitResult(allowed, remaining, retryAfterMs);
            }
            // CAS lost the race to a concurrent caller; retry against fresh state.
        }
    }

    public long totalLocalChecks() {
        return localChecks.sum();
    }

    public long totalLocalRejections() {
        return localRejections.sum();
    }
}
