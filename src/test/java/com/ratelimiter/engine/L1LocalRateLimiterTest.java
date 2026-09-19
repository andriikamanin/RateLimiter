package com.ratelimiter.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.ratelimiter.config.RateLimiterProperties;
import com.ratelimiter.grpc.v1.Algorithm;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class L1LocalRateLimiterTest {

    private final RateLimiterProperties properties = new RateLimiterProperties();
    private final L1LocalRateLimiter limiter = new L1LocalRateLimiter(properties);

    @Test
    void tokenBucketAllowsUpToCapacityThenRejects() {
        BucketConfig config = new BucketConfig(5, 0); // no refill during the test window

        int allowed = 0;
        for (int i = 0; i < 10; i++) {
            RateLimitResult result = limiter.checkLocal("user-1", "/res", 1, Algorithm.TOKEN_BUCKET, config);
            if (result.allowed()) {
                allowed++;
            }
        }

        assertThat(allowed).isEqualTo(5);
    }

    @Test
    void leakyBucketRejectsOnceCapacityExceeded() {
        BucketConfig config = new BucketConfig(3, 0);

        assertThat(limiter.checkLocal("user-2", "/res", 1, Algorithm.LEAKY_BUCKET, config).allowed()).isTrue();
        assertThat(limiter.checkLocal("user-2", "/res", 1, Algorithm.LEAKY_BUCKET, config).allowed()).isTrue();
        assertThat(limiter.checkLocal("user-2", "/res", 1, Algorithm.LEAKY_BUCKET, config).allowed()).isTrue();
        RateLimitResult fourth = limiter.checkLocal("user-2", "/res", 1, Algorithm.LEAKY_BUCKET, config);
        assertThat(fourth.allowed()).isFalse();
        // ratePerMs is 0 in this test (no leaking), so retry-after is reported as -1 (never).
        assertThat(fourth.retryAfterMs()).isEqualTo(-1L);
    }

    @Test
    void concurrentCheckLocalNeverAllowsMoreThanCapacity() throws InterruptedException {
        BucketConfig config = new BucketConfig(100, 0);
        int threads = 32;
        int attemptsPerThread = 50; // 1600 total attempts against a 100-token bucket
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger allowedCount = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int i = 0; i < attemptsPerThread; i++) {
                    RateLimitResult result = limiter.checkLocal("shared-key", "/res", 1, Algorithm.TOKEN_BUCKET, config);
                    if (result.allowed()) {
                        allowedCount.incrementAndGet();
                    }
                }
            });
        }

        ready.await();
        start.countDown();
        pool.shutdown();
        pool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS);

        assertThat(allowedCount.get()).isEqualTo(100);
    }
}
