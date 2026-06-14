package org.cache;

import com.github.benmanes.caffeine.cache.Caffeine;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

public class ShardedCache implements LoadingCache {
    public static final int HOTKEY_ADD_FREQ = 1000;
    private final Shard[] shards;
    private final int N = 64;

    /**
     * Tracks in-flight loads for stampede protection.
     * Key: cache key  →  Value: future that will carry the loaded result.
     * Only the "winning" thread populates the cache; all others wait here.
     */
    private final ConcurrentHashMap<String, CompletableFuture<Optional<Object>>> inflight =
            new ConcurrentHashMap<>();

    private ExpiryJanitor janitor;

    private CacheMetrics cacheMetrics;

    com.github.benmanes.caffeine.cache.Cache<String, Object> l1Cache =
            Caffeine.newBuilder()
                    .expireAfterWrite(500, TimeUnit.MILLISECONDS)
                    .maximumSize(1000)
                    .build();

    public CacheMetrics getMetrics() { return cacheMetrics; }

    public ShardedCache(Supplier<EvictionPolicy> supplier) {
        this.shards = new Shard[N];
        for(int i = 0; i < N; i++){
            shards[i] = new Shard(5000, new HashMap<>(), new ReentrantReadWriteLock(), supplier.get());
        }
        janitor = new ExpiryJanitor(this.shards);
        janitor.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            janitor.stop();
        }));
        this.cacheMetrics = new CacheMetrics();
    }

    private Shard shardFor(String key) {
        int shardIdx = fnv1a(key) % shards.length;
        return shards[shardIdx];
    }

    private int fnv1a(String key) {
        int hash = 0x811c9dc5;
        for (byte b : key.getBytes(StandardCharsets.UTF_8)) {
            hash ^= (b & 0xFF);
            hash *= 0x01000193;
        }
        return hash & 0x7FFFFFFF;
    }

    @Override
    public void put(String key, Object value, long ttlSeconds) {
        Shard shard = shardFor(key);
        shard.getLock().writeLock().lock();
        try {
            if (shard.getCache().size() >= shard.getCapacity()) {
                String victim =  shard.getEvictionPolicy().evict();
                shard.getCache().remove(victim);
                cacheMetrics.recordEviction();
            }
            shard.getCache().put(key, new CacheEntry(value, ttlSeconds));
            shard.getEvictionPolicy().onInsert(key);
        }
        finally {
            shard.getLock().writeLock().unlock();
        }
    }

    @Override
    public Optional<Object> get(String key) {
        Object l1Value = l1Cache.getIfPresent(key);
        if (l1Value != null){
            Shard shard = shardFor(key);
            shard.getLock().readLock().lock();
            try {
                shard.getEvictionPolicy().onAccess(key);
            }
            finally {
                shard.getLock().readLock().unlock();
            }
            cacheMetrics.recordHit();
            return Optional.of(l1Value);
        }
        return getFromL2(key);
    }

    private Optional<Object> getFromL2(String key) {
        Shard shard = shardFor(key);
        shard.getLock().readLock().lock();
        try{
            CacheEntry entry = shard.getCache().get(key);
            if (entry != null && System.currentTimeMillis() < entry.getExpireEpoch()) {
                shard.getEvictionPolicy().onAccess(key);
                long freq = entry.getFrequency().incrementAndGet();
                if(freq > HOTKEY_ADD_FREQ)
                    l1Cache.put(key, entry.getValue());
                cacheMetrics.recordHit();
                return Optional.of(entry.getValue());
            }
        }
        finally {
            shard.getLock().readLock().unlock();
        }
        cacheMetrics.recordMiss();
        return Optional.empty();
    }

    @Override
    public void delete(String key) {
        Shard shard = shardFor(key);
        shard.getLock().writeLock().lock();
        try{
            CacheEntry cacheEntry = shard.getCache().get(key);
            if(cacheEntry != null){
                shard.getCache().remove(key);
                shard.getEvictionPolicy().onDelete(key);
            }
        }
        finally {
            shard.getLock().writeLock().unlock();
        }
    }

    /**
     * Stampede-safe get-or-load.
     *
     * Flow:
     *   1. Cache hit  → return immediately (L1 or L2).
     *   2. Cache miss → race to register an in-flight future via putIfAbsent.
     *        Winner  : executes loader, writes result to cache, completes future.
     *        Losers  : block on the existing future (no origin call).
     *   3. Loader exception → future completes exceptionally; losers propagate
     *      the exception as a RuntimeException.
     *   4. Timeout waiting for in-flight load → fall back to a direct cache
     *      read (the winner may have finished just after we timed out).
     */
    @Override
    public Optional<Object> getOrLoad(String key,
                                      Function<String, Object> loader,
                                      long ttlSeconds,
                                      long timeoutMs) {
        // --- Fast path: already cached ---
        Optional<Object> cached = get(key);
        if (cached.isPresent()) return cached;

        // --- Slow path: register intent to load ---
        CompletableFuture<Optional<Object>> myFuture  = new CompletableFuture<>();
        CompletableFuture<Optional<Object>> racing     = inflight.putIfAbsent(key, myFuture);

        if (racing != null) {
            // Another thread is already loading — wait for it
            try {
                return racing.get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (java.util.concurrent.TimeoutException e) {
                // Winner is slow; try the cache one more time before giving up
                return get(key);
            } catch (java.util.concurrent.ExecutionException e) {
                throw new RuntimeException("Loader failed for key: " + key, e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return get(key);
            }
        }

        // --- We won the race: execute the loader ---
        try {
            Object value = loader.apply(key);
            Optional<Object> result = (value != null) ? Optional.of(value) : Optional.empty();
            if (value != null) {
                put(key, value, ttlSeconds);
            }
            myFuture.complete(result);
            return result;
        } catch (Exception e) {
            myFuture.completeExceptionally(e);
            throw new RuntimeException("Loader failed for key: " + key, e);
        } finally {
            inflight.remove(key, myFuture);
        }
    }
}
