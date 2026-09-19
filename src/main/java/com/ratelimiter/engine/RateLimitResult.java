package com.ratelimiter.engine;

public record RateLimitResult(boolean allowed, long remainingTokens, long retryAfterMs) {

    public static RateLimitResult allow(long remainingTokens) {
        return new RateLimitResult(true, remainingTokens, 0);
    }

    public static RateLimitResult reject(long remainingTokens, long retryAfterMs) {
        return new RateLimitResult(false, remainingTokens, retryAfterMs);
    }
}
