package org.cache;

import java.util.Optional;
import java.util.function.Function;

/**
 * Extends Cache with stampede-safe loading.
 *
 * getOrLoad() guarantees that on a cache miss only ONE thread executes the
 * loader function; every other concurrent caller for the same key waits on
 * the same CompletableFuture instead of fanning out to the origin.
 */
public interface LoadingCache extends Cache {

    /**
     * @param key        cache key
     * @param loader     called at most once per concurrent miss; must be thread-safe
     *                   and may return null (treated as empty)
     * @param ttlSeconds how long to cache a successfully loaded value
     * @param timeoutMs  max millis to wait for an in-flight load by another thread
     * @return the cached or freshly loaded value
     */
    Optional<Object> getOrLoad(String key,
                               Function<String, Object> loader,
                               long ttlSeconds,
                               long timeoutMs);
}
