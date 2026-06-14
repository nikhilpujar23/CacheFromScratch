package org.cache;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * O(1) LFU eviction policy (Shah et al., 2010).
 *
 * Data structures:
 *   keyToFreq   : key  → current access frequency
 *   freqToKeys  : freq → insertion-ordered set of keys at that frequency
 *                 (LinkedHashSet gives O(1) add/remove and LRU tiebreaking)
 *   minFreq     : lowest frequency bucket that still has keys
 *
 * All operations — onInsert, onAccess, onDelete, evict — are O(1).
 *
 * NOTE: this class is NOT thread-safe on its own. The caller (Shard) holds
 * a ReentrantReadWriteLock around every call, so no extra locking is needed here.
 */
public class LFUEvictionPolicy implements EvictionPolicy {

    private final Map<String, Integer>           keyToFreq  = new HashMap<>();
    private final Map<Integer, LinkedHashSet<String>> freqToKeys = new HashMap<>();
    private int minFreq = 0;

    // ------------------------------------------------------------------ //

    @Override
    public void onInsert(String key) {
        keyToFreq.put(key, 1);
        freqToKeys.computeIfAbsent(1, k -> new LinkedHashSet<>()).add(key);
        minFreq = 1;           // new key always has the lowest possible freq
    }

    @Override
    public void onAccess(String key) {
        Integer freq = keyToFreq.get(key);
        if (freq == null) return;   // key not tracked (shouldn't happen, but be safe)

        // Move key from freq bucket → freq+1 bucket
        promote(key, freq);
    }

    @Override
    public void onDelete(String key) {
        Integer freq = keyToFreq.remove(key);
        if (freq == null) return;

        LinkedHashSet<String> bucket = freqToKeys.get(freq);
        if (bucket != null) {
            bucket.remove(key);
            if (bucket.isEmpty()) freqToKeys.remove(freq);
            // minFreq may now be stale, but evict() will never be called
            // for a deleted key, so we don't need to repair it here.
        }
    }

    @Override
    public String evict() {
        LinkedHashSet<String> bucket = freqToKeys.get(minFreq);
        if (bucket == null || bucket.isEmpty())
            throw new IllegalStateException("LFU evict called on empty policy");

        // LinkedHashSet iteration order = insertion order → oldest at front
        String victim = bucket.iterator().next();
        bucket.remove(victim);
        if (bucket.isEmpty()) freqToKeys.remove(minFreq);

        keyToFreq.remove(victim);
        // minFreq will be reset to 1 on the next onInsert() call;
        // no need to scan for a new minimum here.
        return victim;
    }

    // ------------------------------------------------------------------ //

    private void promote(String key, int freq) {
        // Remove from current bucket
        LinkedHashSet<String> oldBucket = freqToKeys.get(freq);
        oldBucket.remove(key);
        if (oldBucket.isEmpty()) {
            freqToKeys.remove(freq);
            if (minFreq == freq) minFreq = freq + 1;
        }

        // Add to next bucket
        int newFreq = freq + 1;
        keyToFreq.put(key, newFreq);
        freqToKeys.computeIfAbsent(newFreq, k -> new LinkedHashSet<>()).add(key);
    }
}
