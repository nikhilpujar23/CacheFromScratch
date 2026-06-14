package org.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("=== In-Memory Cache Demo ===\n");

        demo_basicOps();
        demo_ttlExpiry();
        demo_lruEviction();
        demo_asyncQueue();
        demo_loadTest();
        demo_lfuVsLru();
        demo_stampedeProtection();
    }

    // ------------------------------------------------------------------ //
    //  1. Basic put / get / delete
    // ------------------------------------------------------------------ //
    static void demo_basicOps() {
        System.out.println("--- Basic Operations ---");
        Cache cache = new ShardedCache(LRUEvictionPolicy::new);

        cache.put("user:1", "Alice", 60);
        cache.put("user:2", "Bob",   60);

        System.out.println("GET user:1 -> " + cache.get("user:1").orElse("MISS"));
        System.out.println("GET user:2 -> " + cache.get("user:2").orElse("MISS"));

        cache.delete("user:1");
        System.out.println("GET user:1 (after delete) -> " + cache.get("user:1").orElse("MISS"));
        System.out.println();
    }

    // ------------------------------------------------------------------ //
    //  2. TTL expiry
    // ------------------------------------------------------------------ //
    static void demo_ttlExpiry() throws InterruptedException {
        System.out.println("--- TTL Expiry ---");
        Cache cache = new ShardedCache(LRUEvictionPolicy::new);

        cache.put("session:abc", "token-xyz", 1);   // 1-second TTL
        System.out.println("GET session:abc (before expiry) -> "
                + cache.get("session:abc").orElse("MISS"));

        Thread.sleep(1500);
        System.out.println("GET session:abc (after 1.5s)    -> "
                + cache.get("session:abc").orElse("MISS"));
        System.out.println();
    }

    // ------------------------------------------------------------------ //
    //  3. LRU eviction — fill a shard past capacity so eviction fires
    // ------------------------------------------------------------------ //
    static void demo_lruEviction() {
        System.out.println("--- LRU Eviction ---");
        // Each shard holds 5 000 entries. With 64 shards that is 320 000 slots.
        // Insert 320 100 entries to guarantee at least one eviction.
        Cache cache = new ShardedCache(LRUEvictionPolicy::new);

        int total = 320_100;
        for (int i = 0; i < total; i++) {
            cache.put("key:" + i, "value:" + i, 3600);
        }

        // The very first key should have been evicted from its shard
        Optional<Object> evicted = cache.get("key:0");
        System.out.println("key:0 after " + total + " inserts: "
                + (evicted.isPresent() ? evicted.get() : "EVICTED (expected)"));

        // Most recent key should still be present
        Optional<Object> recent = cache.get("key:" + (total - 1));
        System.out.println("key:" + (total - 1) + " (most recent): "
                + recent.orElse("MISS"));
        System.out.println();
    }

    // ------------------------------------------------------------------ //
    //  4. Async command queue
    // ------------------------------------------------------------------ //
    static void demo_asyncQueue() throws Exception {
        System.out.println("--- Async Command Queue ---");
        Cache cache = new ShardedCache(LRUEvictionPolicy::new);
        CacheCommandQueue queue = new CacheCommandQueue(cache);
        queue.start();

        // Fire PUT asynchronously
        CompletableFuture<Optional<Object>> putFuture = new CompletableFuture<>();
        queue.submit(new CacheCommand(CacheCommand.Command.PUT, "async:key", "async-value", 60, putFuture));
        putFuture.get(2, TimeUnit.SECONDS);

        // Fire GET asynchronously
        CompletableFuture<Optional<Object>> getFuture = new CompletableFuture<>();
        queue.submit(new CacheCommand(CacheCommand.Command.GET, "async:key", null, 0, getFuture));
        Optional<Object> result = getFuture.get(2, TimeUnit.SECONDS);
        System.out.println("Async GET async:key -> " + result.orElse("MISS"));

        // Fire DELETE asynchronously
        CompletableFuture<Optional<Object>> delFuture = new CompletableFuture<>();
        queue.submit(new CacheCommand(CacheCommand.Command.DELETE, "async:key", null, 0, delFuture));
        delFuture.get(2, TimeUnit.SECONDS);

        CompletableFuture<Optional<Object>> getFuture2 = new CompletableFuture<>();
        queue.submit(new CacheCommand(CacheCommand.Command.GET, "async:key", null, 0, getFuture2));
        System.out.println("Async GET async:key (after delete) -> " + getFuture2.get(2, TimeUnit.SECONDS).orElse("MISS"));
        System.out.println();
    }

    // ------------------------------------------------------------------ //
    //  5. Load test — concurrent readers + writers, then print metrics
    // ------------------------------------------------------------------ //
    static void demo_loadTest() throws InterruptedException {
        System.out.println("--- Load Test (concurrent r/w) ---");
        Cache cache = new ShardedCache(LRUEvictionPolicy::new);
        CacheCommandQueue queue = new CacheCommandQueue(cache);
        queue.start();

        int writerThreads = 8;
        int readerThreads = 16;
        int opsPerThread  = 2_000;

        CountDownLatch latch = new CountDownLatch(writerThreads + readerThreads);
        ExecutorService pool = Executors.newFixedThreadPool(writerThreads + readerThreads);
        Random rng = new Random();

        long start = System.currentTimeMillis();

        // Writers
        for (int t = 0; t < writerThreads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    for (int i = 0; i < opsPerThread; i++) {
                        String key = "k:" + (tid * opsPerThread + i);
                        CompletableFuture<Optional<Object>> f = new CompletableFuture<>();
                        queue.submit(new CacheCommand(CacheCommand.Command.PUT, key, "v" + i, 60, f));
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        // Readers (random keys, so expect some misses)
        for (int t = 0; t < readerThreads; t++) {
            pool.submit(() -> {
                try {
                    for (int i = 0; i < opsPerThread; i++) {
                        String key = "k:" + rng.nextInt(writerThreads * opsPerThread);
                        CompletableFuture<Optional<Object>> f = new CompletableFuture<>();
                        queue.submit(new CacheCommand(CacheCommand.Command.GET, key, null, 0, f));
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        long elapsed = System.currentTimeMillis() - start;
        int totalOps = (writerThreads + readerThreads) * opsPerThread;
        System.out.printf("Completed %,d ops in %d ms  (~%,d ops/sec)%n",
                totalOps, elapsed, totalOps * 1000L / Math.max(1, elapsed));

        // Print metrics from the ShardedCache
        if (cache instanceof ShardedCache sc) {
            System.out.print("Metrics -> ");
            sc.getMetrics().snapshot();
        }
        System.out.println();
    }

    // ------------------------------------------------------------------ //
    //  6. LFU vs LRU — skewed workload
    //
    //  Pattern: warm N "hot" keys with many accesses, then flood with N
    //  "cold" keys to force evictions. Under LRU every cold write evicts a
    //  hot key; under LFU the hot keys survive because their freq is higher.
    // ------------------------------------------------------------------ //
    static void demo_lfuVsLru() {
        System.out.println("--- LFU vs LRU (skewed workload) ---");

        int HOT  = 1_000;
        int COLD = 320_000; // more than total capacity → forces evictions
        int HOT_ACCESS_ROUNDS = 20; // access each hot key 20× before flood

        Cache lru = new ShardedCache(LRUEvictionPolicy::new);
        Cache lfu = new ShardedCache(LFUEvictionPolicy::new);

        // 1. Insert + warm hot keys in both caches
        for (int i = 0; i < HOT; i++) {
            lru.put("hot:" + i, "hotval:" + i, 3600);
            lfu.put("hot:" + i, "hotval:" + i, 3600);
        }
        for (int r = 0; r < HOT_ACCESS_ROUNDS; r++) {
            for (int i = 0; i < HOT; i++) {
                lru.get("hot:" + i);
                lfu.get("hot:" + i);
            }
        }

        // 2. Flood with cold keys to trigger evictions
        for (int i = 0; i < COLD; i++) {
            lru.put("cold:" + i, "coldval:" + i, 3600);
            lfu.put("cold:" + i, "coldval:" + i, 3600);
        }

        // 3. Count how many hot keys survived
        int lruSurvived = 0, lfuSurvived = 0;
        for (int i = 0; i < HOT; i++) {
            if (lru.get("hot:" + i).isPresent()) lruSurvived++;
            if (lfu.get("hot:" + i).isPresent()) lfuSurvived++;
        }

        System.out.printf("Hot keys surviving after %,d cold inserts:%n", COLD);
        System.out.printf("  LRU: %d / %d  (%.1f%%)%n", lruSurvived, HOT, lruSurvived * 100.0 / HOT);
        System.out.printf("  LFU: %d / %d  (%.1f%%)%n", lfuSurvived, HOT, lfuSurvived * 100.0 / HOT);
        System.out.println("LFU should retain significantly more hot keys.");
        System.out.println();
    }

    // ------------------------------------------------------------------ //
    //  7. Cache stampede protection
    //
    //  50 threads all miss on the same key simultaneously.
    //  Without protection: 50 origin calls.
    //  With getOrLoad:     exactly 1 origin call — others wait on the future.
    // ------------------------------------------------------------------ //
    static void demo_stampedeProtection() throws InterruptedException {
        System.out.println("--- Cache Stampede Protection ---");

        ShardedCache cache = new ShardedCache(LRUEvictionPolicy::new);

        int THREADS = 50;
        java.util.concurrent.atomic.AtomicInteger loaderCallCount = new java.util.concurrent.atomic.AtomicInteger(0);

        // Simulated origin: slow (50 ms) so threads pile up on the miss
        Function<String, Object> slowOrigin = key -> {
            loaderCallCount.incrementAndGet();
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return "loaded-value-for-" + key;
        };

        CountDownLatch ready  = new CountDownLatch(THREADS);
        CountDownLatch start  = new CountDownLatch(1);
        CountDownLatch done   = new CountDownLatch(THREADS);
        List<Optional<Object>> results = new java.util.concurrent.CopyOnWriteArrayList<>();

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        for (int i = 0; i < THREADS; i++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();  // all threads start at exactly the same time
                    Optional<Object> val = cache.getOrLoad("stampede-key", slowOrigin, 60, 5000);
                    results.add(val);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        ready.await();   // wait for all threads to be ready
        start.countDown(); // fire
        done.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        long distinct = results.stream().flatMap(Optional::stream).distinct().count();
        System.out.printf("Threads: %d  |  Origin calls: %d  |  Distinct values returned: %d%n",
                THREADS, loaderCallCount.get(), distinct);
        System.out.println("Origin calls should be 1, not " + THREADS + ".");
        System.out.println();
    }
}
