package org.cache;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Data
@AllArgsConstructor
public class CacheCommand {
    public enum Command{GET, PUT, DELETE};

    private Command command;
    private String key;
    private Object value;
    private long ttlSeconds;
    private CompletableFuture<Optional<Object>> future;

}
