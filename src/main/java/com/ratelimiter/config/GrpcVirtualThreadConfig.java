package com.ratelimiter.config;

import java.util.concurrent.Executors;
import net.devh.boot.grpc.server.serverfactory.GrpcServerConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Runs every gRPC call on a virtual thread (Project Loom) instead of the
 * default fixed platform-thread pool, so a slow downstream call (Redis)
 * never pins a scarce OS thread and the server scales to very high
 * concurrent connection counts with minimal footprint per call.
 */
@Configuration
public class GrpcVirtualThreadConfig {

    @Bean
    public GrpcServerConfigurer virtualThreadServerConfigurer() {
        return serverBuilder -> serverBuilder.executor(Executors.newVirtualThreadPerTaskExecutor());
    }
}
