package com.ratelimiter.engine;

import com.ratelimiter.config.RateLimiterProperties;
import com.ratelimiter.config.RateLimiterProperties.BucketDefinition;
import com.ratelimiter.config.RateLimiterProperties.FailStrategy;
import com.ratelimiter.grpc.v1.Algorithm;
import com.ratelimiter.grpc.v1.RateLimitRequest;
import com.ratelimiter.metrics.RateLimiterMetrics;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Orchestrates the full decision pipeline for a single {@code CheckLimit}
 * call:
 *
 * <ol>
 *   <li>Fast local rejection via {@link GuardCache} for keys already known
 *       to be over quota, avoiding a Redis round trip entirely.</li>
 *   <li>Distributed evaluation against Redis ({@link L2RedisRateLimiter}),
 *       which is the source of truth across the whole fleet.</li>
 *   <li>On Redis failure or timeout, transparent degradation to
 *       {@link L1LocalRateLimiter}, which runs the same algorithm
 *       per-instance instead of globally.</li>
 *   <li>If even the local fallback cannot produce a decision (an
 *       unexpected runtime error), the configured {@link FailStrategy} is
 *       the last resort: fail-open allows traffic, fail-closed blocks it.</li>
 * </ol>
 */
@Component
public class RateLimitEngine {

    private static final Logger log = LoggerFactory.getLogger(RateLimitEngine.class);

    private final GuardCache guardCache;
    private final L2RedisRateLimiter l2;
    private final L1LocalRateLimiter l1;
    private final RateLimiterProperties properties;
    private final RateLimiterMetrics metrics;

    public RateLimitEngine(GuardCache guardCache, L2RedisRateLimiter l2, L1LocalRateLimiter l1,
                            RateLimiterProperties properties, RateLimiterMetrics metrics) {
        this.guardCache = guardCache;
        this.l2 = l2;
        this.l1 = l1;
        this.properties = properties;
        this.metrics = metrics;
    }

    public CompletableFuture<RateLimitResult> checkLimit(RateLimitRequest request) {
        String key = request.getKey();
        String resource = request.getResource();
        Algorithm algorithm = request.getAlgorithm() == Algorithm.ALGORITHM_UNSPECIFIED
                ? Algorithm.TOKEN_BUCKET
                : request.getAlgorithm();
        int tokensRequested = request.getTokensRequested() <= 0 ? 1 : request.getTokensRequested();
        String algorithmTag = algorithm.name();

        Timer.Sample overall = metrics.startTimer();
        String guardKey = GuardCache.keyFor(key, resource, algorithmTag);
        long now = System.currentTimeMillis();

        long remainingBlock = guardCache.remainingBlockMs(guardKey, now);
        if (remainingBlock >= 0) {
            metrics.recordRequest(false, algorithmTag, resource);
            metrics.stopTimer(overall, algorithmTag, resource);
            return CompletableFuture.completedFuture(RateLimitResult.reject(0, remainingBlock));
        }

        BucketDefinition definition = properties.resolve(resource);
        BucketConfig config = BucketConfig.forAlgorithm(algorithm, definition.getCapacity(),
                definition.getRefillRatePerSec(), definition.getLeakRatePerSec());

        return l2.check(key, resource, tokensRequested, algorithm, config)
                .orTimeout(properties.getRedis().getCommandTimeoutMs() * 2, TimeUnit.MILLISECONDS)
                .thenApply(result -> {
                    if (!result.allowed()) {
                        guardCache.block(guardKey, result.retryAfterMs(), System.currentTimeMillis());
                    }
                    return result;
                })
                .exceptionally(ex -> {
                    log.warn("Redis unavailable for resource '{}', degrading to local rate limiting: {}",
                            resource, ex.getMessage());
                    metrics.recordRedisFailure(resource);
                    try {
                        return l1.checkLocal(key, resource, tokensRequested, algorithm, config);
                    } catch (RuntimeException fallbackFailure) {
                        log.error("Local fallback rate limiter also failed, applying fail-strategy {}",
                                properties.getFailStrategy(), fallbackFailure);
                        return applyFailStrategy(definition.getCapacity());
                    }
                })
                .whenComplete((result, ex) -> {
                    if (result != null) {
                        metrics.recordRequest(result.allowed(), algorithmTag, resource);
                    }
                    metrics.stopTimer(overall, algorithmTag, resource);
                });
    }

    private RateLimitResult applyFailStrategy(long capacity) {
        if (properties.getFailStrategy() == FailStrategy.FAIL_OPEN) {
            return RateLimitResult.allow(capacity);
        }
        return RateLimitResult.reject(0, properties.getRedis().getKeyTtlSeconds() * 1000);
    }
}
