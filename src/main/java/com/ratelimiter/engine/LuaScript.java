package com.ratelimiter.engine;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

public enum LuaScript {
    TOKEN_BUCKET("lua/token_bucket.lua"),
    LEAKY_BUCKET("lua/leaky_bucket.lua");

    private final String body;

    LuaScript(String classpathResource) {
        this.body = load(classpathResource);
    }

    public String body() {
        return body;
    }

    private static String load(String classpathResource) {
        try (InputStream in = LuaScript.class.getClassLoader().getResourceAsStream(classpathResource)) {
            if (in == null) {
                throw new IllegalStateException("Missing lua script on classpath: " + classpathResource);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load lua script: " + classpathResource, e);
        }
    }
}
