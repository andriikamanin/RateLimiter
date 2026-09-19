package com.ratelimiter.config;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Single multiplexed Lettuce connection shared across all virtual threads.
 * Lettuce is thread-safe and pipelines commands over one connection, so a
 * pool is unnecessary here and would only add overhead.
 */
@Configuration
public class RedisConfig {

    @Bean(destroyMethod = "shutdown")
    public RedisClient redisClient(@Autowired RateLimiterProperties properties) {
        RedisURI uri = RedisURI.create(properties.getRedis().getUri());
        RedisClient client = RedisClient.create(uri);
        client.setOptions(ClientOptions.builder()
                .autoReconnect(true)
                .socketOptions(SocketOptions.builder()
                        .connectTimeout(Duration.ofMillis(properties.getRedis().getCommandTimeoutMs() * 4))
                        .keepAlive(true)
                        .build())
                .timeoutOptions(TimeoutOptions.enabled(
                        Duration.ofMillis(properties.getRedis().getCommandTimeoutMs())))
                .build());
        return client;
    }

    @Bean(destroyMethod = "close")
    public StatefulRedisConnection<String, String> redisConnection(RedisClient redisClient) {
        return redisClient.connect(StringCodec.UTF8);
    }
}
