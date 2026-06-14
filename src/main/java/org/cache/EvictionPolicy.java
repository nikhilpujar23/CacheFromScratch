package org.cache;

public interface EvictionPolicy {
    String evict();
    void onInsert(String key);
    void onDelete(String key);

    void onAccess(String key);
}
