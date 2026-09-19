package com.ratelimiter.engine;

import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.springframework.stereotype.Component;

/**
 * Executes the rate-limiting Lua scripts atomically against Redis using
 * EVALSHA with a transparent fallback to EVAL on a cache miss (NOSCRIPT),
 * which is the standard pattern for avoiding re-transmitting script source
 * on every call while remaining correct after a Redis restart/failover
 * that clears the script cache.
 */
@Component
public class LuaScriptRunner {

    private final StatefulRedisConnection<String, String> connection;
    private final Map<LuaScript, String> shaByScript = new EnumMap<>(LuaScript.class);

    public LuaScriptRunner(StatefulRedisConnection<String, String> connection) {
        this.connection = connection;
    }

    @PostConstruct
    public void loadScripts() {
        RedisCommands<String, String> sync = connection.sync();
        for (LuaScript script : LuaScript.values()) {
            shaByScript.put(script, sync.scriptLoad(script.body()));
        }
    }

    public CompletableFuture<List<Long>> eval(LuaScript script, String key, String[] args) {
        RedisAsyncCommands<String, String> async = connection.async();
        String sha = shaByScript.get(script);
        String[] keys = {key};
        CompletableFuture<Object> future = async.<Object>evalsha(sha, ScriptOutputType.MULTI, keys, args)
                .toCompletableFuture();
        return future
                .exceptionallyCompose(ex -> {
                    if (isNoScript(ex)) {
                        return async.<Object>eval(script.body(), ScriptOutputType.MULTI, keys, args)
                                .toCompletableFuture();
                    }
                    return CompletableFuture.failedFuture(ex);
                })
                .thenApply(LuaScriptRunner::toLongList);
    }

    private static boolean isNoScript(Throwable ex) {
        Throwable cause = ex;
        while (cause != null) {
            if (cause instanceof RedisNoScriptException) {
                return true;
            }
            if (cause.getMessage() != null && cause.getMessage().startsWith("NOSCRIPT")) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static List<Long> toLongList(Object raw) {
        List<Object> list = (List<Object>) raw;
        List<Long> result = new ArrayList<>(list.size());
        for (Object o : list) {
            result.add(((Number) o).longValue());
        }
        return result;
    }
}
