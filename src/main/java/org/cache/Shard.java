package org.cache;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Data
@AllArgsConstructor
public class Shard {
    private int capacity;
    private HashMap<String, CacheEntry> cache;
    private ReentrantReadWriteLock lock;
    private EvictionPolicy evictionPolicy;
}
