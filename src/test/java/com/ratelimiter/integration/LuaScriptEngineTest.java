package com.ratelimiter.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ratelimiter.engine.LuaScript;
import com.ratelimiter.engine.LuaScriptRunner;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Exercises the raw Lua scripts directly against a real Redis instance,
 * independent of the Spring context, to verify the atomic state-evaluation
 * math: refill/leak behavior, exhaustion, and correctness under concurrency.
 */
class LuaScriptEngineTest extends AbstractRedisIntegrationTest {

    static RedisClient client;
    static StatefulRedisConnection<String, String> connection;
    static LuaScriptRunner runner;

    @BeforeAll
    static void setUp() {
        client = RedisClient.create(redisUri());
        connection = client.connect(StringCodec.UTF8);
        runner = new LuaScriptRunner(connection);
        runner.loadScripts();
    }

    @AfterAll
    static void tearDown() {
        connection.close();
        client.shutdown();
    }

    private static String[] tokenBucketArgs(long capacity, double ratePerMs, long now, int requested) {
        return new String[] {
                Long.toString(capacity), Double.toString(ratePerMs), Long.toString(now),
                Integer.toString(requested), "60000"
        };
    }

    @Test
    void tokenBucketAllowsUntilExhaustedThenRejects() {
        String key = "test:tb:" + UUID.randomUUID();
        long now = System.currentTimeMillis();

        for (int i = 0; i < 5; i++) {
            List<Long> reply = runner.eval(LuaScript.TOKEN_BUCKET, key, tokenBucketArgs(5, 0, now, 1)).join();
            assertThat(reply.get(0)).isEqualTo(1L);
        }

        List<Long> rejected = runner.eval(LuaScript.TOKEN_BUCKET, key, tokenBucketArgs(5, 0, now, 1)).join();
        assertThat(rejected.get(0)).isEqualTo(0L);
        assertThat(rejected.get(1)).isEqualTo(0L);
        assertThat(rejected.get(2)).isEqualTo(-1L); // rate 0 => never refills
    }

    @Test
    void tokenBucketRefillsOverTime() throws InterruptedException {
        String key = "test:tb-refill:" + UUID.randomUUID();
        long now = System.currentTimeMillis();
        double ratePerMs = 0.01; // 10 tokens/sec

        List<Long> first = runner.eval(LuaScript.TOKEN_BUCKET, key, tokenBucketArgs(1, ratePerMs, now, 1)).join();
        assertThat(first.get(0)).isEqualTo(1L);

        List<Long> immediateRetry = runner.eval(LuaScript.TOKEN_BUCKET, key, tokenBucketArgs(1, ratePerMs, now, 1)).join();
        assertThat(immediateRetry.get(0)).isEqualTo(0L);
        long retryAfterMs = immediateRetry.get(2);
        assertThat(retryAfterMs).isGreaterThan(0);

        await().atMost(Duration.ofMillis(retryAfterMs + 500)).untilAsserted(() -> {
            List<Long> retried = runner.eval(LuaScript.TOKEN_BUCKET, key,
                    tokenBucketArgs(1, ratePerMs, System.currentTimeMillis(), 1)).join();
            assertThat(retried.get(0)).isEqualTo(1L);
        });
    }

    @Test
    void leakyBucketRejectsOnceQueueIsFull() {
        String key = "test:lb:" + UUID.randomUUID();
        long now = System.currentTimeMillis();

        for (int i = 0; i < 3; i++) {
            List<Long> reply = runner.eval(LuaScript.LEAKY_BUCKET, key, tokenBucketArgs(3, 0, now, 1)).join();
            assertThat(reply.get(0)).isEqualTo(1L);
        }

        List<Long> rejected = runner.eval(LuaScript.LEAKY_BUCKET, key, tokenBucketArgs(3, 0, now, 1)).join();
        assertThat(rejected.get(0)).isEqualTo(0L);
    }

    @Test
    void concurrentRequestsNeverExceedCapacityAcrossManyClients() throws InterruptedException {
        String key = "test:concurrent:" + UUID.randomUUID();
        long capacity = 50;
        int threads = 20;
        int attemptsPerThread = 10; // 200 attempts against a 50-token bucket, rate 0 (no refill)
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger allowed = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < attemptsPerThread; i++) {
                        List<Long> reply = runner.eval(LuaScript.TOKEN_BUCKET, key,
                                tokenBucketArgs(capacity, 0, System.currentTimeMillis(), 1)).join();
                        if (reply.get(0) == 1L) {
                            allowed.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        done.await();
        pool.shutdown();

        assertThat(allowed.get()).isEqualTo((int) capacity);
    }
}
