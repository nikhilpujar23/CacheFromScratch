package org.cache;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class ExpiryJanitor{
    Shard[] shards;
    ScheduledExecutorService scheduledExecutorService;

    public ExpiryJanitor(Shard[] shards){
        this.shards = shards;
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        scheduledExecutorService.scheduleAtFixedRate(this::scanAllShards, 0, 10, TimeUnit.SECONDS);
    }

    private void scanAllShards() {
        for (Shard shard : shards) {
            scanShard(shard);
        }
    }

    private void scanShard(Shard shard) {
        shard.getLock().writeLock().lock();
        try {
            boolean repeat = true;
            while (repeat) {
                List<String> expiredKeys = shard.getCache().entrySet().stream()
                        .limit(20)
                        .filter(e -> e.getValue().isExpired())
                        .map(Map.Entry::getKey)
                        .toList();

                expiredKeys.forEach(key -> {
                    shard.getCache().remove(key);
                    shard.getEvictionPolicy().onDelete(key);
                });

                repeat = expiredKeys.size() > 20 * 0.2;
            }
        } finally {
            shard.getLock().writeLock().unlock();
        }
    }

    public void stop() {
        scheduledExecutorService.shutdown();
    }

}
