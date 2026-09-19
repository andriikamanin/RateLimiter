package com.ratelimiter.grpc;

import com.ratelimiter.engine.RateLimitEngine;
import com.ratelimiter.engine.RateLimitResult;
import com.ratelimiter.grpc.v1.RateLimitRequest;
import com.ratelimiter.grpc.v1.RateLimitResponse;
import com.ratelimiter.grpc.v1.RateLimiterServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@GrpcService
public class RateLimiterGrpcService extends RateLimiterServiceGrpc.RateLimiterServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(RateLimiterGrpcService.class);

    private final RateLimitEngine engine;

    public RateLimiterGrpcService(RateLimitEngine engine) {
        this.engine = engine;
    }

    @Override
    public void checkLimit(RateLimitRequest request, StreamObserver<RateLimitResponse> responseObserver) {
        if (request.getKey().isBlank() || request.getResource().isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("key and resource are required")
                    .asRuntimeException());
            return;
        }

        engine.checkLimit(request)
                .thenAccept(result -> {
                    responseObserver.onNext(toResponse(result));
                    responseObserver.onCompleted();
                })
                .exceptionally(ex -> {
                    log.error("Unhandled error evaluating rate limit for key={} resource={}",
                            request.getKey(), request.getResource(), ex);
                    responseObserver.onError(Status.INTERNAL
                            .withDescription("rate limiter internal error")
                            .withCause(ex)
                            .asRuntimeException());
                    return null;
                });
    }

    private static RateLimitResponse toResponse(RateLimitResult result) {
        return RateLimitResponse.newBuilder()
                .setAllowed(result.allowed())
                .setRemainingTokens(result.remainingTokens())
                .setRetryAfterMs(result.retryAfterMs())
                .build();
    }
}
