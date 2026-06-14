package org.cache;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.concurrent.atomic.AtomicLong;

@Data
@AllArgsConstructor
public class CacheEntry {
    private final long expireEpoch;
    private final Object value;
    private AtomicLong frequency = new AtomicLong(0);

    public CacheEntry(Object value, Long ttlSeconds){
        this.value = value;
        this.expireEpoch = System.currentTimeMillis() + (ttlSeconds * 1000);
    }

    public boolean isExpired() {
        return System.currentTimeMillis() >= expireEpoch;
    }
}
