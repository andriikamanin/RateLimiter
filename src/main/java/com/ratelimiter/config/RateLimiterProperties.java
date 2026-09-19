package com.ratelimiter.config;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ratelimiter")
public class RateLimiterProperties {

    public enum FailStrategy {
        FAIL_OPEN,
        FAIL_CLOSED
    }

    private FailStrategy failStrategy = FailStrategy.FAIL_OPEN;
    private Redis redis = new Redis();
    private GuardCache guardCache = new GuardCache();
    private LocalFallbackCache localFallbackCache = new LocalFallbackCache();
    private BucketDefinition defaultBucket = new BucketDefinition(100, 50, 50);
    private Map<String, BucketDefinition> resources = new LinkedHashMap<>();

    public FailStrategy getFailStrategy() {
        return failStrategy;
    }

    public void setFailStrategy(FailStrategy failStrategy) {
        this.failStrategy = failStrategy;
    }

    public Redis getRedis() {
        return redis;
    }

    public void setRedis(Redis redis) {
        this.redis = redis;
    }

    public GuardCache getGuardCache() {
        return guardCache;
    }

    public void setGuardCache(GuardCache guardCache) {
        this.guardCache = guardCache;
    }

    public LocalFallbackCache getLocalFallbackCache() {
        return localFallbackCache;
    }

    public void setLocalFallbackCache(LocalFallbackCache localFallbackCache) {
        this.localFallbackCache = localFallbackCache;
    }

    public BucketDefinition getDefaultBucket() {
        return defaultBucket;
    }

    public void setDefaultBucket(BucketDefinition defaultBucket) {
        this.defaultBucket = defaultBucket;
    }

    public Map<String, BucketDefinition> getResources() {
        return resources;
    }

    public void setResources(Map<String, BucketDefinition> resources) {
        this.resources = resources;
    }

    /** Resolves the bucket configuration for a resource, falling back to the default. */
    public BucketDefinition resolve(String resource) {
        return resources.getOrDefault(resource, defaultBucket);
    }

    public static class Redis {
        private String uri = "redis://localhost:6379";
        private long commandTimeoutMs = 150;
        private long keyTtlSeconds = 120;

        public String getUri() {
            return uri;
        }

        public void setUri(String uri) {
            this.uri = uri;
        }

        public long getCommandTimeoutMs() {
            return commandTimeoutMs;
        }

        public void setCommandTimeoutMs(long commandTimeoutMs) {
            this.commandTimeoutMs = commandTimeoutMs;
        }

        public long getKeyTtlSeconds() {
            return keyTtlSeconds;
        }

        public void setKeyTtlSeconds(long keyTtlSeconds) {
            this.keyTtlSeconds = keyTtlSeconds;
        }
    }

    public static class GuardCache {
        private long maxTtlSeconds = 30;
        private long maxSize = 500_000;

        public long getMaxTtlSeconds() {
            return maxTtlSeconds;
        }

        public void setMaxTtlSeconds(long maxTtlSeconds) {
            this.maxTtlSeconds = maxTtlSeconds;
        }

        public long getMaxSize() {
            return maxSize;
        }

        public void setMaxSize(long maxSize) {
            this.maxSize = maxSize;
        }
    }

    public static class LocalFallbackCache {
        private long maxSize = 500_000;
        private long expireAfterAccessSeconds = 300;

        public long getMaxSize() {
            return maxSize;
        }

        public void setMaxSize(long maxSize) {
            this.maxSize = maxSize;
        }

        public long getExpireAfterAccessSeconds() {
            return expireAfterAccessSeconds;
        }

        public void setExpireAfterAccessSeconds(long expireAfterAccessSeconds) {
            this.expireAfterAccessSeconds = expireAfterAccessSeconds;
        }
    }

    /** capacity in tokens/units; refillRatePerSec used for TOKEN_BUCKET; leakRatePerSec used for LEAKY_BUCKET. */
    public static class BucketDefinition {
        private long capacity;
        private double refillRatePerSec;
        private double leakRatePerSec;

        public BucketDefinition() {
        }

        public BucketDefinition(long capacity, double refillRatePerSec, double leakRatePerSec) {
            this.capacity = capacity;
            this.refillRatePerSec = refillRatePerSec;
            this.leakRatePerSec = leakRatePerSec;
        }

        public long getCapacity() {
            return capacity;
        }

        public void setCapacity(long capacity) {
            this.capacity = capacity;
        }

        public double getRefillRatePerSec() {
            return refillRatePerSec;
        }

        public void setRefillRatePerSec(double refillRatePerSec) {
            this.refillRatePerSec = refillRatePerSec;
        }

        public double getLeakRatePerSec() {
            return leakRatePerSec;
        }

        public void setLeakRatePerSec(double leakRatePerSec) {
            this.leakRatePerSec = leakRatePerSec;
        }
    }
}
