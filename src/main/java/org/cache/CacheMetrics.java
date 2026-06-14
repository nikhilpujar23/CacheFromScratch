package org.cache;

import lombok.Data;

import java.util.concurrent.atomic.AtomicLong;

@Data
public class CacheMetrics {
    private final AtomicLong hits      = new AtomicLong(0);
    private final AtomicLong misses    = new AtomicLong(0);
    private final AtomicLong evictions = new AtomicLong(0);

    public void recordHit()      { hits.incrementAndGet(); }
    public void recordMiss()     { misses.incrementAndGet(); }
    public void recordEviction() { evictions.incrementAndGet(); }

    public void snapshot() {
        System.out.printf("Hits: %d | Misses: %d | Evictions: %d | Hit Rate: %.2f%%%n",
                hits.get(),
                misses.get(),
                evictions.get(),
                hits.get() * 100.0 / Math.max(1, hits.get() + misses.get())
        );
    }
}