package org.cache;

import java.util.Optional;

public interface Cache {
    void put(String key, Object value, long ttlSeconds);
    Optional<Object> get(String key);
    void delete(String key);
}
