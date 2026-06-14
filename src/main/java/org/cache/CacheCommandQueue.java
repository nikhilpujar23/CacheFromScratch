package org.cache;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.nonNull;

public class CacheCommandQueue {
    private final LinkedBlockingQueue<CacheCommand> commandQueue;
    private final ExecutorService executorService;

    private final Cache cache;
    private static final int WORKER_THREADS = Runtime.getRuntime().availableProcessors();



    public CacheCommandQueue(Cache cache) {
        this.commandQueue = new LinkedBlockingQueue<>(1000);
        this.executorService = Executors.newFixedThreadPool(WORKER_THREADS);
        this.cache = cache;
    }

    public void submit(CacheCommand command){
       if (!commandQueue.offer(command))
           throw new RuntimeException("Command queue is full");
    }

    public void start(){
        for (int i = 0; i < WORKER_THREADS; i++) {
            executorService.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        CacheCommand command = commandQueue.poll(1000, TimeUnit.MILLISECONDS);
                        if (nonNull(command)) process(command);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });
        }
    }

    private void process(CacheCommand command) {
        switch (command.getCommand()){
            case GET -> {
                Optional<Object> value = cache.get(command.getKey());
                command.getFuture().complete(value);
            }
            case PUT -> {
                cache.put(command.getKey(), command.getValue(), command.getTtlSeconds());
                command.getFuture().complete(Optional.empty());
            }
            case DELETE -> {
                cache.delete(command.getKey());
                command.getFuture().complete(Optional.empty());
            }
        }
    }
}
