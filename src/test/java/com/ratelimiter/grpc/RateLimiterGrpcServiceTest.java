package com.ratelimiter.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.ratelimiter.config.RateLimiterProperties;
import com.ratelimiter.engine.BucketConfig;
import com.ratelimiter.engine.GuardCache;
import com.ratelimiter.engine.L1LocalRateLimiter;
import com.ratelimiter.engine.L2RedisRateLimiter;
import com.ratelimiter.engine.RateLimitEngine;
import com.ratelimiter.engine.RateLimitResult;
import com.ratelimiter.grpc.v1.Algorithm;
import com.ratelimiter.grpc.v1.RateLimitRequest;
import com.ratelimiter.grpc.v1.RateLimitResponse;
import com.ratelimiter.grpc.v1.RateLimiterServiceGrpc;
import com.ratelimiter.metrics.RateLimiterMetrics;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Verifies the gRPC surface end-to-end over an in-process channel: request
 * validation and correct translation of engine decisions into
 * {@link RateLimitResponse}, without needing a real Redis.
 */
class RateLimiterGrpcServiceTest {

    @Mock
    private L2RedisRateLimiter l2;

    private Server server;
    private ManagedChannel channel;
    private RateLimiterServiceGrpc.RateLimiterServiceBlockingStub stub;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        RateLimiterProperties properties = new RateLimiterProperties();
        GuardCache guardCache = new GuardCache(properties);
        L1LocalRateLimiter l1 = new L1LocalRateLimiter(properties);
        RateLimiterMetrics metrics = new RateLimiterMetrics(new SimpleMeterRegistry());
        RateLimitEngine engine = new RateLimitEngine(guardCache, l2, l1, properties, metrics);

        String serverName = "in-process-" + System.nanoTime();
        server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new RateLimiterGrpcService(engine))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        stub = RateLimiterServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() throws Exception {
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void returnsAllowedDecisionFromEngine() {
        when(l2.check(anyString(), anyString(), anyInt(), any(), any(BucketConfig.class)))
                .thenReturn(CompletableFuture.completedFuture(RateLimitResult.allow(99)));

        RateLimitResponse response = stub.checkLimit(RateLimitRequest.newBuilder()
                .setKey("user_123")
                .setResource("/api/v1/checkout")
                .setTokensRequested(1)
                .setAlgorithm(Algorithm.TOKEN_BUCKET)
                .build());

        assertThat(response.getAllowed()).isTrue();
        assertThat(response.getRemainingTokens()).isEqualTo(99);
        assertThat(response.getRetryAfterMs()).isZero();
    }

    @Test
    void returnsRejectedDecisionWithRetryAfter() {
        when(l2.check(anyString(), anyString(), anyInt(), any(), any(BucketConfig.class)))
                .thenReturn(CompletableFuture.completedFuture(RateLimitResult.reject(0, 250)));

        RateLimitResponse response = stub.checkLimit(RateLimitRequest.newBuilder()
                .setKey("user_123")
                .setResource("/api/v1/checkout")
                .setTokensRequested(1)
                .setAlgorithm(Algorithm.TOKEN_BUCKET)
                .build());

        assertThat(response.getAllowed()).isFalse();
        assertThat(response.getRetryAfterMs()).isEqualTo(250);
    }

    @Test
    void rejectsRequestMissingKeyWithInvalidArgument() {
        assertThatThrownBy(() -> stub.checkLimit(RateLimitRequest.newBuilder()
                .setResource("/api/v1/checkout")
                .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("INVALID_ARGUMENT");
    }
}
