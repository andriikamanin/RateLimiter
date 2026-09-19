package com.ratelimiter.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

@Component
public class RateLimiterMetrics {

    private final MeterRegistry registry;

    public RateLimiterMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void recordRequest(boolean allowed, String algorithm, String resource) {
        Counter.builder("rate_limiter_requests_total")
                .tag("status", allowed ? "allowed" : "rejected")
                .tag("algorithm", algorithm)
                .tag("resource", resource)
                .register(registry)
                .increment();
    }

    public void recordRedisFailure(String resource) {
        Counter.builder("rate_limiter_redis_failures_total")
                .tag("resource", resource)
                .register(registry)
                .increment();
    }

    public Timer.Sample startTimer() {
        return Timer.start(registry);
    }

    public void stopTimer(Timer.Sample sample, String algorithm, String resource) {
        sample.stop(Timer.builder("rate_limiter_latency_seconds")
                .tag("algorithm", algorithm)
                .tag("resource", resource)
                .register(registry));
    }

    public Timer.Sample startRedisTimer() {
        return Timer.start(registry);
    }

    public void stopRedisTimer(Timer.Sample sample) {
        sample.stop(Timer.builder("redis_execution_latency_seconds").register(registry));
    }
}
