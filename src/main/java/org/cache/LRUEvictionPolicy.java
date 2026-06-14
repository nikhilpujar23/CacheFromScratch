package org.cache;

import java.util.LinkedHashMap;

public class LRUEvictionPolicy implements EvictionPolicy{

    private final LinkedHashMap<String, Boolean> linkedHashMap = new LinkedHashMap<>(16, 0.75f, true);
    @Override
    public String evict() {
        String key =  linkedHashMap.keySet().iterator().next();
        linkedHashMap.remove(key);
        return key;
    }

    @Override
    public void onInsert(String key) {
        linkedHashMap.put(key, true);
    }

    @Override
    public void onDelete(String key) {
        linkedHashMap.remove(key);
    }

    @Override
    public void onAccess(String key) {
        linkedHashMap.get(key);
    }
}
