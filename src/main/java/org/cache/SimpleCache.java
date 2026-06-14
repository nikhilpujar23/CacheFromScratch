package org.cache;

import java.util.HashMap;
import java.util.Optional;

public class SimpleCache implements Cache{

    HashMap<String, CacheEntry> cache = new HashMap<>();
    @Override
    public void put(String key, Object value, long ttlSeconds) {
        cache.put(key, new CacheEntry(value, ttlSeconds));
    }

    @Override
    public Optional<Object> get(String key) {
        CacheEntry entry = cache.get(key);
        if (entry != null && System.currentTimeMillis() < entry.getExpireEpoch()) {
            entry.getFrequency().incrementAndGet();
            return Optional.of(entry.getValue());
        }
        return Optional.empty();
    }

    @Override
    public void delete(String key) {
        cache.remove(key);
    }
}
