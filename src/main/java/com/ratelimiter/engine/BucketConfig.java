package com.ratelimiter.engine;

import com.ratelimiter.grpc.v1.Algorithm;

/** Resolved, per-request bucket parameters expressed in millisecond-denominated rates. */
public record BucketConfig(long capacity, double ratePerMs) {

    public static BucketConfig forAlgorithm(Algorithm algorithm, long capacity, double refillRatePerSec, double leakRatePerSec) {
        double ratePerSec = algorithm == Algorithm.LEAKY_BUCKET ? leakRatePerSec : refillRatePerSec;
        return new BucketConfig(capacity, ratePerSec / 1000.0);
    }
}
